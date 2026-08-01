import asyncio
import logging
import time
import json
from typing import List
from sqlalchemy.exc import OperationalError, DBAPIError
import redis.asyncio as aioredis
from app.db.session import async_session_factory
from app.repositories.event_repo import EventRepository
from datetime import datetime, timezone
from app.core.config import settings
from collections import Counter
from app.metrics import (ACTIVE_WORKERS_COUNT, EVENTS_PROCESSED_TOTAL, WORKER_PROCESSING_LATENCY, WORKER_RETRIES_TOTAL,)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Воркер %(name)s] - %(levelname)s - %(message)s")
logger = logging.getLogger("PipelineWorker")

BATCH_SIZE = 100        
BATCH_TIMEOUT = 5.0     
MAX_RETRIES = 5         
BACKOFF_FACTOR = 2       
NUM_WORKERS = settings.WORKERS_COUNT       
QUEUE_NAME = settings.QUEUE_NAME
DLQ_QUEUE_NAME = settings.DLQ_QUEUE_NAME
def count_events_by_source(events: List[dict]) -> Counter:
    return Counter(str(event.get("source") or "unknown") for event in events)

def record_events_status(events: List[dict], status: str,) -> None:
    try:
        for source, count in count_events_by_source(events).items():
            EVENTS_PROCESSED_TOTAL.labels(source=source,status=status,).inc(count)
    except Exception as metrics_err:
        logger.warning(f"Не удалось обновить метрику обработанных событий: " f"{metrics_err}")

def record_retry_metrics(events: List[dict]) -> None:
    try:
        for source, count in count_events_by_source(events).items():
            WORKER_RETRIES_TOTAL.labels(source=source,).inc(count)
    except Exception as metrics_err:
        logger.warning(f"Не удалось обновить метрику повторных попыток: " f"{metrics_err}")

def record_batch_latency(events: List[dict], duration: float,) -> None:
    try:
        sources = list(count_events_by_source(events))
        if not sources:
            metric_source = "unknown"
        elif len(sources) == 1:
            metric_source = sources[0]
        else:
            metric_source = "mixed"
        WORKER_PROCESSING_LATENCY.labels(source=metric_source,).observe(duration)
    except Exception as metrics_err:
        logger.warning(f"Не удалось обновить метрику времени обработки: " f"{metrics_err}")

def record_worker_started(worker_id: int) -> None:
    try:
        ACTIVE_WORKERS_COUNT.inc()
    except Exception as metrics_err:
        logger.warning(f"Worker {worker_id}: не удалось увеличить " f"ACTIVE_WORKERS_COUNT: {metrics_err}")

def record_worker_stopped(worker_id: int) -> None:
    try:
        ACTIVE_WORKERS_COUNT.dec()
    except Exception as metrics_err:
        logger.warning(
            f"Worker {worker_id}: не удалось уменьшить "
            f"ACTIVE_WORKERS_COUNT: {metrics_err}")
async def send_to_dlq(worker_id: int, events: List[dict], redis_client: aioredis.Redis,) -> bool:
    batch_size = len(events)
    serialized_events = [json.dumps(event, default=str) for event in events]
    try:
        await redis_client.rpush(DLQ_QUEUE_NAME, *serialized_events,)
    except Exception as dlq_err:
        logger.critical(
            f"Worker {worker_id}: не удалось отправить "
            f"{batch_size} событий в DLQ: {dlq_err}")
        return False
    record_events_status(events, status="failed",)
    await update_failed_stats(worker_id, batch_size,redis_client,)
    logger.warning(
        f"Worker {worker_id}: {batch_size} событий "
        f"отправлены в DLQ ({DLQ_QUEUE_NAME}).")
    return True
async def update_processed_stats(worker_id: int,batch_size: int, redis_client: aioredis.Redis,) -> None:
    try:
        await redis_client.incrby("stats:processed", batch_size)
        await redis_client.hincrby("stats:workers", f"worker_{worker_id}", batch_size,)
    except Exception as stats_err:
        logger.warning(f"Worker {worker_id}: батч сохранён в PostgreSQL, "f"но статистика Redis не обновлена: {stats_err}")
async def update_failed_stats(worker_id: int,batch_size: int, redis_client: aioredis.Redis,) -> None:
    try:
        await redis_client.incrby("stats:failed", batch_size)
    except Exception as stats_err:
        logger.warning(f"Worker {worker_id}: события отправлены в DLQ, " f"но счётчик stats:failed не обновлён: {stats_err}")
async def update_retry_stats(worker_id: int, redis_client: aioredis.Redis,) -> None:
    try:
        await redis_client.incr("stats:retries")
    except Exception as stats_err:
        logger.warning(
            f"Worker {worker_id}: повторная попытка продолжится, "
            f"но счётчик stats:retries не обновлён: {stats_err}")

async def send_invalid_message_to_dlq(worker_id: int, raw_data: str, error: Exception, redis_client: aioredis.Redis,) -> bool:
    dead_letter_event = {"reason": "invalid_event_payload", "raw_data": raw_data, "error": str(error), "failed_at": datetime.now(timezone.utc).isoformat(),}
    return await send_to_dlq(worker_id, [dead_letter_event], redis_client,)
async def requeue_raw_message(worker_id: int, raw_data: str, redis_client: aioredis.Redis,) -> bool:
    try:
        await redis_client.rpush(QUEUE_NAME, raw_data,)
    except Exception as requeue_err:
        logger.critical(f"Worker {worker_id}: не удалось ни отправить сообщение " f"в DLQ, ни вернуть его в основную очередь: {requeue_err}")
        return False
    logger.warning(f"Worker {worker_id}: DLQ недоступна, сообщение возвращено " f"в очередь {QUEUE_NAME}.")
    return True

async def requeue_events(worker_id: int, events: List[dict], redis_client: aioredis.Redis,) -> bool:
    if not events:
        return True
    serialized_events = [json.dumps(event, default=str) for event in events]
    try:
        await redis_client.rpush(QUEUE_NAME, *serialized_events,)
    except Exception as requeue_err:
        logger.critical(f"Worker {worker_id}: не удалось вернуть " f"{len(events)} событий в очередь {QUEUE_NAME}: " f"{requeue_err}")
        return False
    logger.warning(f"Worker {worker_id}: DLQ недоступна, " f"{len(events)} событий возвращены в очередь {QUEUE_NAME}.")
    return True

async def handle_failed_batch(worker_id: int, events: List[dict], redis_client: aioredis.Redis,) -> bool:
    sent_to_dlq = await send_to_dlq(worker_id, events, redis_client,)
    if sent_to_dlq:
        return True
    return await requeue_events(worker_id, events, redis_client,)

async def save_batch_with_retry(worker_id: int, events: List[dict], redis_client: aioredis.Redis) -> bool:
    batch_size = len(events)
    retries = 0
    delay = 1.0
    while retries < MAX_RETRIES:
        attempt_started = time.perf_counter()
        try:
            async with async_session_factory() as session:
                async with session.begin():
                    repo = EventRepository(session)
                    await repo.create_many(events)
                    processing_duration = time.perf_counter() - attempt_started
                    record_events_status(events, status="success",)
                    record_batch_latency(events, processing_duration,) 
            logger.info(f"Worker {worker_id}: Успешно сохранил батч из {batch_size} записей в PostgreSQL.")
            await update_processed_stats(worker_id, batch_size, redis_client,)
            return True
        except (OperationalError, DBAPIError) as db_err:
            retries += 1
            logger.error(f"Worker {worker_id}: ошибка базы данных " f"(попытка {retries}/{MAX_RETRIES}): {db_err}")
            if retries >= MAX_RETRIES:
                logger.critical(f"Worker {worker_id}: попытки исчерпаны.""Обрабатываем неудачный батч.")
                return await handle_failed_batch(worker_id,events,redis_client,)
            await update_retry_stats(worker_id,redis_client,)
            record_retry_metrics(events)
            await asyncio.sleep(delay)
            delay *= BACKOFF_FACTOR
        except Exception as e:
            logger.error(f"Worker {worker_id}: Непредвиденная ошибка при записи в базу: {e}. Отправка в DLQ...")
            return await handle_failed_batch(worker_id, events, redis_client,)
            
async def worker_loop(worker_id: int, redis_client: aioredis.Redis,) -> None:
    buffer: List[dict] = []
    last_flush_time = time.time()
    record_worker_started(worker_id)
    logger.info(f"Worker {worker_id} запущен и слушает очередь {QUEUE_NAME}.")
    try:
        while True:
            try:
                data = await redis_client.blpop(QUEUE_NAME, timeout=1,)
                if data:
                    raw_data = data[1]
                    try:
                        event = json.loads(raw_data)
                        if not isinstance(event, dict):
                            raise ValueError("Событие должно быть JSON-объектом")
                        created_at = event.get("created_at")
                        if isinstance(created_at, str):
                            event["created_at"] = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    except (json.JSONDecodeError, TypeError, ValueError,) as parse_error:
                        logger.error(f"Worker {worker_id}: получено " f"некорректное событие: {parse_error}")
                        sent_to_dlq = (await send_invalid_message_to_dlq(worker_id, raw_data, parse_error, redis_client,))
                        if not sent_to_dlq:
                            requeued = await requeue_raw_message(worker_id, raw_data, redis_client,)
                            if not requeued:
                                logger.critical(f"Worker {worker_id}: сообщение " "не удалось отправить в DLQ или " "вернуть в основную очередь.")
                            await asyncio.sleep(1)
                        continue
                    buffer.append(event)
                current_time = time.time()
                time_since_flush = current_time - last_flush_time
                batch_is_full = len(buffer) >= BATCH_SIZE
                batch_timed_out = (bool(buffer) and time_since_flush >= BATCH_TIMEOUT)
                if batch_is_full or batch_timed_out:
                    logger.info(
                        f"Worker {worker_id}: сброс батча. "
                        f"Размер: {len(buffer)}, "
                        f"ожидание: {time_since_flush:.2f} сек.")
                    batch_handled = await save_batch_with_retry(worker_id,buffer, redis_client,)
                    if batch_handled:
                        buffer.clear()
                        last_flush_time = time.time()
                    else:
                        logger.critical(
                            f"Worker {worker_id}: батч не удалось "
                            "сохранить в PostgreSQL, отправить в DLQ "
                            "или вернуть в основную очередь. "
                            "События остаются в памяти воркера.")
                        await asyncio.sleep(1)
            except asyncio.CancelledError:
                if buffer:
                    logger.warning(
                        f"Worker {worker_id}: получен сигнал остановки. "
                        f"Обрабатываем оставшиеся {len(buffer)} событий.")
                    batch_handled = await asyncio.shield(
                        save_batch_with_retry(worker_id, buffer, redis_client,))
                    if batch_handled:
                        buffer.clear()
                        logger.info(
                            f"Worker {worker_id}: оставшийся батч "
                            "безопасно обработан.")
                    else:
                        logger.critical(
                            f"Worker {worker_id}: при остановке "
                            "не удалось сохранить батч, отправить его "
                            "в DLQ или вернуть в основную очередь.")
                raise
            except Exception as worker_error:
                logger.exception(
                    f"Worker {worker_id}: ошибка в основном цикле: "
                    f"{worker_error}")
                await asyncio.sleep(1)
    finally:
        record_worker_stopped(worker_id)
        logger.info(f"Worker {worker_id} полностью остановлен.")

async def main():
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    logger.info(f"Запуск конвейера. Создаем {NUM_WORKERS} параллельных воркеров...")
    workers = [asyncio.create_task(worker_loop(worker_id=i, redis_client=redis_client)) for i in range(NUM_WORKERS)]
    try:
        await asyncio.gather(*workers)
    finally:
        await redis_client.aclose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Пайплайн остановлен пользователем с клавиатуры.")