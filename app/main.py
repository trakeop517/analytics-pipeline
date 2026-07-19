import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.config import settings
from app.db.session import async_engine, async_session_factory
from app.models.event import Base, EventModel
from app.api.monitoring import router as monitoring
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("main")

app_state = {"queue": None, "workers": []}

async def test_worker(worker_id: int, queue: asyncio.Queue):
    logger.info(f"Worker-{worker_id} успешно запущен.")
    try:
        while True:
            task_data = await queue.get()
            try:
                logger.info(f"Worker-{worker_id} взял в работу: {task_data['title']}")
                async with async_session_factory() as session:
                    new_event = EventModel(
                        source=task_data["source"],
                        external_id=task_data["external_id"],
                        title=task_data["title"],
                        payload=task_data["payload"]
                    )
                    session.add(new_event)
                    await session.commit()
                    
                logger.info(f"Worker-{worker_id} успешно сохранил событие в PostgreSQL!")
            except Exception as e:
                logger.error(f"Ошибка воркера {worker_id} при записи в БД: {e}")
            finally:
                queue.task_done()
    except asyncio.CancelledError:
        logger.info(f"Worker-{worker_id} остановлен.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Старт приложения. Проверка инфраструктуры...")
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Таблицы в PostgreSQL проверены/созданы.")
    queue = asyncio.Queue(maxsize=settings.MAX_QUEUE_SIZE)
    app_state["queue"] = queue
    for i in range(settings.WORKERS_COUNT):
        task = asyncio.create_task(test_worker(worker_id=i+1, queue=queue))
        app_state["workers"].append(task)
    yield
    logger.info("Остановка... Разбираем очередь.")
    if queue.qsize() > 0:
        try:
            await asyncio.wait_for(queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Не успели разобрать очередь при выключении.")
    for task in app_state["workers"]:
        task.cancel()
    await asyncio.gather(*app_state["workers"], return_exceptions=True)
    logger.info("Все воркеры остановлены.")

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)
app.include_router(monitoring)
@app.get("/health")
async def health():
    return {"queue_size": app_state["queue"].qsize() if app_state["queue"] else 0}

@app.post("/test-produce")
async def test_produce():
    if not app_state["queue"]:
        return {"error": "Очередь не инициализирована"}
    
    fake_event = {
        "source": "github",
        "external_id": f"gh-{asyncio.get_event_loop().time()}", 
        "title": "Test pipeline event",
        "payload": {"status": "success", "meta": "initial_commit_test"}
    }
    
    await app_state["queue"].put(fake_event)
    return {"status": "отправлено в очередь", "event_title": fake_event["title"]}