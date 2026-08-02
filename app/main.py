import asyncio
import logging
from contextlib import asynccontextmanager
import redis.asyncio as redis
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from sqlalchemy import func, select
from app.consumers.worker import worker_loop
from app.api.fetchers import router as fetchers_router
from app.api.monitoring import router as monitoring
from app.core.config import settings
from app.db.session import async_engine, async_session_factory
from app.metrics import (REDIS_DLQ_SIZE,REDIS_QUEUE_SIZE)
from app.models.event import Base, EventModel
from app.services.github_fetcher import github_fetcher
from app.services.hn_fetcher import hn_fetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("main")

QUEUE_NAME = settings.QUEUE_NAME
DLQ_QUEUE_NAME = settings.DLQ_QUEUE_NAME
REDIS_URL = getattr(settings, "REDIS_URL", f"redis://{getattr(settings, 'REDIS_HOST', '127.0.0.1')}:{getattr(settings, 'REDIS_PORT', 6379)}")
app_state = {"workers": [], "redis": None, "metrics_task": None}

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
    logger.info("Фоновые фетчеры GitHub и Hacker News успешно запущены!")
    for i in range(settings.WORKERS_COUNT):
        task = asyncio.create_task(worker_loop(worker_id=i + 1, redis_client=app_state["redis"],))
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
        return {"total_events_in_db": total_count, "by_source": stats_by_source, "latest_events": [
                {"source": ev.source, "external_id": ev.external_id, "title": ev.title, "created_at": ev.created_at.isoformat() if ev.created_at else None}
                for ev in recent_events]}