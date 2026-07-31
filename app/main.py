import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.api.fetchers import router as fetchers_router
from app.api.monitoring import router as monitoring
from app.core.config import settings
from app.db.session import async_engine, async_session_factory
from app.metrics import (
    ACTIVE_WORKERS_COUNT,
    EVENTS_PROCESSED_TOTAL,
    REDIS_DLQ_SIZE,
    REDIS_QUEUE_SIZE,
    WORKER_PROCESSING_LATENCY,
    WORKER_RETRIES_TOTAL,
)
from app.models.event import Base, EventModel
from app.services.github_fetcher import github_fetcher
from app.services.hn_fetcher import hn_fetcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("main")

QUEUE_NAME = "events_queue"
DLQ_QUEUE_NAME = "dlq_events_queue"
MAX_RETRIES = 3

REDIS_URL = getattr(
    settings, 
    "REDIS_URL", 
    f"redis://{getattr(settings, 'REDIS_HOST', '127.0.0.1')}:{getattr(settings, 'REDIS_PORT', 6379)}")
app_state = {"workers": [], "redis": None, "metrics_task": None}

async def redis_worker(worker_id: int):
    logger.info(f"Worker-{worker_id} (Redis) запущен.")
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        while True:
            try:
                res = await redis_client.blpop(QUEUE_NAME, timeout=2)
                if not res:
                    continue
                _, raw_data = res
                start_time = time.perf_counter()
                ACTIVE_WORKERS_COUNT.inc()
                task_data = None
                try:
                    task_data = json.loads(raw_data)
                    source = task_data.get("source", "unknown")
                    logger.info(f"Worker-{worker_id} обрабатывает [{source}]: {task_data.get('title')}")
                    async with async_session_factory() as session:
                        stmt = pg_insert(EventModel).values(
                            source=source,
                            external_id=str(task_data["external_id"]),
                            title=task_data.get("title"),
                            payload=task_data.get("payload", {}),
                        ).on_conflict_do_nothing(
                            index_elements=["source", "external_id"])
                        await session.execute(stmt)
                        await session.commit()
                    EVENTS_PROCESSED_TOTAL.labels(source=source, status="success").inc()
                    WORKER_PROCESSING_LATENCY.labels(source=source).observe(time.perf_counter() - start_time)
                except Exception as e:
                    logger.error(f"Ошибка воркера {worker_id} при записи: {e}")
                    source = task_data.get("source", "unknown") if task_data else "unknown"
                    retries = (task_data.get("retries", 0) + 1) if task_data else MAX_RETRIES + 1
                    WORKER_RETRIES_TOTAL.labels(source=source).inc()
                    if retries <= MAX_RETRIES and task_data:
                        task_data["retries"] = retries
                        await redis_client.rpush(QUEUE_NAME, json.dumps(task_data))
                    else:
                        EVENTS_PROCESSED_TOTAL.labels(source=source, status="failed").inc()
                        await redis_client.rpush(DLQ_QUEUE_NAME, raw_data)
                    await asyncio.sleep(1)
                finally:
                    ACTIVE_WORKERS_COUNT.dec()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Системная ошибка воркера {worker_id}: {e}")
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info(f"Worker-{worker_id} остановлен.")
    finally:
        await redis_client.aclose()

async def update_metrics_loop():
    while True:
        if app_state.get("redis"):
            try:
                q_size = await app_state["redis"].llen(QUEUE_NAME)
                dlq_size = await app_state["redis"].llen(DLQ_QUEUE_NAME)
                REDIS_QUEUE_SIZE.set(q_size)
                REDIS_DLQ_SIZE.set(dlq_size)
            except Exception:
                pass
        await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запуск приложения и инициализация сервисов...")
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    app_state["redis"] = redis.from_url(REDIS_URL, decode_responses=True)
    app_state["metrics_task"] = asyncio.create_task(update_metrics_loop())
    github_fetcher.start_background_polling(interval_seconds=60)
    hn_fetcher.start_background_polling(interval_seconds=30)
    logger.info("✅ Фоновые фетчеры GitHub и Hacker News успешно запущены!")
    for i in range(settings.WORKERS_COUNT):
        task = asyncio.create_task(redis_worker(worker_id=i + 1))
        app_state["workers"].append(task)
    yield
    logger.info("Остановка приложения...")
    await github_fetcher.stop_background_polling()
    await hn_fetcher.stop_background_polling()
    for task in app_state["workers"]:
        task.cancel()
    if app_state["metrics_task"]:
        app_state["metrics_task"].cancel()
    all_tasks = app_state["workers"] + [app_state["metrics_task"]]
    await asyncio.gather(*all_tasks, return_exceptions=True)
    if app_state["redis"]:
        await app_state["redis"].aclose()

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)
app.include_router(monitoring)
app.include_router(fetchers_router)
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/health")
async def health():
    queue_size = 0
    if app_state["redis"]:
        try:
            queue_size = await app_state["redis"].llen(QUEUE_NAME)
        except Exception as e:
            logger.error(f"Ошибка чтения размера очереди Redis: {e}")
    return {"status": "ok", "redis_queue_size": queue_size}

@app.get("/analytics/summary")
async def get_analytics_summary():
    async with async_session_factory() as session:
        counts_query = select(EventModel.source, func.count(EventModel.id)).group_by(EventModel.source)
        res = await session.execute(counts_query)
        stats_by_source = dict(res.all())
        total_count = sum(stats_by_source.values())
        recent_query = select(EventModel).order_by(EventModel.created_at.desc()).limit(10)
        recent_res = await session.execute(recent_query)
        recent_events = recent_res.scalars().all()
        return {
            "total_events_in_db": total_count,
            "by_source": stats_by_source,
            "latest_events": [
                {
                    "source": ev.source,
                    "external_id": ev.external_id,
                    "title": ev.title,
                    "created_at": ev.created_at.isoformat() if ev.created_at else None
                }
                for ev in recent_events]}