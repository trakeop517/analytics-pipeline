from fastapi import APIRouter, HTTPException, status
import redis.asyncio as aioredis
from datetime import datetime
from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from app.db.redis import get_redis_client

router = APIRouter(prefix="/monitoring", tags=["Мониторинг Пайплайна"])
async def get_redis_client():
    return aioredis.from_url("redis://localhost:6379", decode_responses=True)

@router.get("/stats")
async def get_general_stats():
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

@router.get("/metrics", response_class=PlainTextResponse)
async def get_prometheus_metrics(redis: aioredis.Redis = Depends(get_redis_client)):
    processed = await redis.get("stats:processed") or 0
    failed = await redis.get("stats:failed") or 0
    retries = await redis.get("stats:retries") or 0
    queue_len = await redis.llen("events_queue") or 0
    dlq_len = await redis.llen("events:dlq") or 0
    workers_data = await redis.hgetall("stats:workers") or {}
    lines = [
        "# HELP pipeline_events_processed_total Total number of successfully processed events.",
        "# TYPE pipeline_events_processed_total counter",
        f"pipeline_events_processed_total {processed}",
        
        "# HELP pipeline_events_failed_total Total number of events sent to DLQ.",
        "# TYPE pipeline_events_failed_total counter",
        f"pipeline_events_failed_total {failed}",

        "# HELP pipeline_events_retries_total Total number of database insert retries.",
        "# TYPE pipeline_events_retries_total counter",
        f"pipeline_events_retries_total {retries}",
        
        "# HELP pipeline_queue_length Current number of events in the main Redis queue.",
        "# TYPE pipeline_queue_length gauge",
        f"pipeline_queue_length {queue_len}",
        
        "# HELP pipeline_dlq_length Current number of events in the Dead Letter Queue.",
        "# TYPE pipeline_dlq_length gauge",
        f"pipeline_dlq_length {dlq_len}",]
    if workers_data:
        lines.extend([
            "# HELP pipeline_worker_processed_total Total events processed by a specific worker.",
            "# TYPE pipeline_worker_processed_total counter"])
        for worker_id, count in workers_data.items():
            w_id = worker_id.decode() if isinstance(worker_id, bytes) else worker_id
            c_val = count.decode() if isinstance(count, bytes) else count
            lines.append(f'pipeline_worker_processed_total{{worker="{w_id}"}} {c_val}')
    return "\n".join(lines) + "\n"