from fastapi import APIRouter, HTTPException, status
import redis.asyncio as aioredis
from datetime import datetime

router = APIRouter(prefix="/monitoring", tags=["Мониторинг Пайплайна"])
async def get_redis_client():
    return aioredis.from_url("redis://localhost:6379", decode_responses=True)

@router.get("/stats")
async def get_general_stats():
    """1. Общая статистика обработанных и упавших событий"""
    redis = await get_redis_client()
    try:
        processed = await redis.get("stats:processed") or 0
        failed = await redis.get("stats:failed") or 0
        retries = await redis.get("stats:retries") or 0
        return {
            "events_processed": int(processed),
            "events_failed_dlq": int(failed),
            "total_retries": int(retries),
            "timestamp": datetime.utcnow().isoformat()}
    finally:
        await redis.aclose()

@router.get("/workers")
async def get_workers_stats():
    redis = await get_redis_client()
    try:
        workers_data = await redis.hgetall("stats:workers") or {}
        return {worker: int(count) for worker, count in workers_data.items()}
    finally:
        await redis.aclose()

@router.get("/queue")
async def get_queue_status():
    redis = await get_redis_client()
    try:
        main_queue_len = await redis.llen("events_queue")
        dlq_len = await redis.llen("events:dlq")
        system_status = "healthy"
        if main_queue_len > 1000:
            system_status = "backed_up"  
        elif main_queue_len > 5000:
            system_status = "critical"   
        return {
            "main_queue_length": main_queue_len,
            "dead_letter_queue_length": dlq_len,
            "status": system_status}
    finally:
        await redis.aclose()

@router.get("/health")
async def health_check():
    redis = await get_redis_client()
    try:
        await redis.ping()
        return {"status": "OK", "redis": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Redis connection failed: {str(e)}")
    finally:
        await redis.aclose()