import redis.asyncio as redis
from app.core.config import settings

async def get_redis_client():
    client = redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        socket_timeout=None,       
        socket_connect_timeout=5,   
        socket_keepalive=True      
    )
    return client