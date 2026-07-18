import asyncio
import json
import logging
from app.db.redis import get_redis_client # Твой клиент Redis
from app.db.session import async_session_factory
from app.api.schemas import EventCreate
from app.repositories.event_repo import EventRepository

logger = logging.getLogger(__name__)

async def start_worker():
    redis_client = await get_redis_client()
    logger.info("Worker started and listening to 'analytics_queue'...")
    while True:
        try:
            result = await redis_client.blpop("analytics_queue", 0)
            if result:
                queue_name, raw_data = result
                logger.info(f"Received raw event from {queue_name.decode()}")
                event_dict = json.loads(raw_data.decode("utf-8"))
                event_data = EventCreate(**event_dict)
                async with async_session_factory() as session:
                    repo = EventRepository(session)
                    db_event = await repo.create_event(event_data)
                    logger.info(f"Successfully saved event to DB: ID={db_event.id} [{db_event.source}]")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON from queue, skipping item.")
        except Exception as e:
            logger.error(f"Error while processing event: {str(e)}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(start_worker())