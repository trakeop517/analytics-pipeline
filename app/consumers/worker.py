import asyncio
import logging
import time
from typing import List
from sqlalchemy.exc import OperationalError, DBAPIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Worker %(name)s] - %(levelname)s - %(message)s")
logger = logging.getLogger("PipelineWorker")

BATCH_SIZE = 100     
BATCH_TIMEOUT = 5.0     
MAX_RETRIES = 5          
BACKOFF_FACTOR = 2       
NUM_WORKERS = 8          

async def save_batch_with_retry(worker_id: int, events: List[dict]):
    retries = 0
    delay = 1.0
    while retries < MAX_RETRIES:
        try:
            async with async_session_maker() as session:
                async with session.begin():
                    repo = EventRepository(session)
                    await repo.create_many(events) 
            logger.info(f"Worker {worker_id}: Успешно сохранил батч из {len(events)} записей.")
            return True
        except (OperationalError, DBAPIError) as db_err:
            retries += 1
            logger.error(
                f"Worker {worker_id}: Ошибка базы данных (Попытка {retries}/{MAX_RETRIES}). "
                f"Ошибка: {db_err}. Повтор через {delay} сек..."
            )
            if retries >= MAX_RETRIES:
                logger.critical(f"Worker {worker_id}: Не удалось сохранить данные после {MAX_RETRIES} попыток!")
                raise db_err 
            await asyncio.sleep(delay)
            delay *= BACKOFF_FACTOR
        except Exception as e:
            logger.error(f"Worker {worker_id}: Непредвиденная ошибка: {e}")
            raise e
async def worker_loop(worker_id: int, redis_client):
    buffer: List[dict] = []
    last_flush_time = time.time()
    logger.info(f"Воркер {worker_id} запущен и готов к работе.")
    while True:
        try:
            data = await redis_client.brpop("events_queue", timeout=1)
            if data:
                import json
                event = json.loads(data[1])
                buffer.append(event)
            current_time = time.time()
            time_since_flush = current_time - last_flush_time
            if len(buffer) >= BATCH_SIZE or (time_since_flush >= BATCH_TIMEOUT and buffer):
                logger.info(f"Worker {worker_id}: Триггер сброса. Размер батча: {len(buffer)}, прошло секунд: {time_since_flush:.2f}")
                await save_batch_with_retry(worker_id, buffer)
                buffer.clear()
                last_flush_time = time.time()
        except asyncio.CancelledError:
            if buffer:
                logger.warning(f"Worker {worker_id}: Завершение работы. Аварийный сброс оставшихся {len(buffer)} записей...")
                try:
                    await save_batch_with_retry(worker_id, buffer)
                except Exception as e:
                    logger.critical(f"Worker {worker_id}: Не удалось спасти данные при выключении: {e}")
            break
        except Exception as e:
            logger.error(f"Worker {worker_id}: Ошибка в основном цикле воркера: {e}")
            await asyncio.sleep(1) # Защита от бесконечного быстрого спама ошибками

async def main():
    logger.info(f"Запуск конвейера. Создаем {NUM_WORKERS} воркеров...")
    workers = [
        asyncio.create_task(worker_loop(worker_id=i, redis_client=redis_client))
        for i in range(NUM_WORKERS)
    ]
    await asyncio.gather(*workers)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Пайплайн остановлен пользователем.")