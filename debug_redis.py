import asyncio
import redis.asyncio as redis

async def test_connection():
    try:
        # Используем 127.0.0.1 вместо localhost
        client = redis.from_url("redis://127.0.0.1:6380", decode_responses=True)
        print("Ping result:", await client.ping())
        await client.close()
        print("✅ Успешно!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())