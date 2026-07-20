import asyncio
import time
import json
from datetime import datetime
import redis.asyncio as redis
from app.core.config import settings
TOTAL_EVENTS = 100_000
BATCH_SIZE = 1000
QUEUE_NAME = "events_queue"

async def run_benchmark():
    client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    print(f" Preparation: Очищаем старую статистику и DLQ...")
    await client.set("stats:processed", 0)
    await client.set("stats:failed", 0)
    await client.delete("stats:workers")
    await client.delete(QUEUE_NAME)
    await client.delete("events:dlq")
    print(f" Generating: Генерируем {TOTAL_EVENTS} уникальных событий по схеме базы...")
    start_push = time.time()
    pipeline = client.pipeline()
    for i in range(TOTAL_EVENTS):
        fake_event = {
            "source": "benchmark_test",
            "external_id": f"evt_{i}_{int(time.time())}",  
            "title": f"Stress Test Event #{i}",
            "payload": {"metric": "performance", "step": i},
            "created_at": datetime.utcnow().isoformat() }
        await pipeline.rpush(QUEUE_NAME, json.dumps(fake_event))
        if (i + 1) % BATCH_SIZE == 0:
            await pipeline.execute()      
    end_push = time.time()
    print(f"✓ Все {TOTAL_EVENTS} событий успешно залиты в Redis за {end_push - start_push:.2f} сек.")
    print("🚀 Воркеры уже вовсю фигачат. Начинаем замер времени...")
    start_process = time.time()
    while True:
        queue_len = await client.llen(QUEUE_NAME)
        processed = int(await client.get("stats:processed") or 0)
        failed = int(await client.get("stats:failed") or 0)
        print(f"   [Мониторинг] В очереди: {queue_len} | Успешно в БД: {processed} | Ошибок в DLQ: {failed}", end="\r")
        if queue_len == 0 and (processed + failed) >= TOTAL_EVENTS:
            break
        await asyncio.sleep(0.2)
    end_process = time.time()
    total_time = end_process - start_process
    events_per_sec = TOTAL_EVENTS / total_time
    print("\n" + "="*40)
    print("📊 РЕЗУЛЬТАТЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")
    print("="*40)
    print(f"• Всего сгенерировано событий: {TOTAL_EVENTS}")
    print(f"• Успешно записано в Postgres: {processed}")
    print(f"• Сброшено в DLQ (ошибки): {failed}")
    print(f"• Чистое время обработки: {total_time:.2f} сек")
    print(f"• Итоговая скорость пайплайна: {events_per_sec:.2f} events/sec")
    print("="*40)
    await client.close()

if __name__ == "__main__":
    asyncio.run(run_benchmark())