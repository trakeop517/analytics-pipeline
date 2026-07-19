import asyncio
import json
from datetime import datetime
import redis.asyncio as aioredis

async def main():
    redis_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)
    print("Затапливаем очередь 500 тестовыми событиями...")
    for i in range(1, 501):
        event = {
            "source": "test_generator",
            "external_id": f"post_uni_id_{i}",
            "title": f"Тестовая новость #{i}",
            "payload": {"clicks": i * 5, "status": "active"},
            "created_at": datetime.utcnow().isoformat()}
        await redis_client.lpush("events_queue", json.dumps(event))
    print(" Все 500 событий в Redis. Смотри логи в окне воркера!")
    await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())