import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import httpx
import redis.asyncio as redis
from app.core.config import settings

logger = logging.getLogger(__name__)
HN_NEW_STORIES_URL = "https://hacker-news.firebaseio.com/v0/newstories.json"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{id}.json"
QUEUE_NAME = settings.QUEUE_NAME

class HackerNewsFetcherService:
    def __init__(self):
        self._bg_task: Optional[asyncio.Task] = None
        self._is_running: bool = False
    async def _fetch_item(self, client: httpx.AsyncClient, item_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = await client.get(HN_ITEM_URL.format(id=item_id))
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"[HN Fetcher] Не удалось загрузить таску #{item_id}: {e}")
        return None

    async def fetch_and_push(self, limit: int = 15) -> int:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(HN_NEW_STORIES_URL)
            response.raise_for_status()
            story_ids: List[int] = response.json()[:limit]
            if not story_ids:
                return 0
            tasks = [self._fetch_item(client, s_id) for s_id in story_ids]
            raw_items = await asyncio.gather(*tasks)
        valid_items = [item for item in raw_items if item and "id" in item]
        if not valid_items:
            return 0
        redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        pushed_count = 0
        try:
            pipeline = redis_client.pipeline()
            for item in valid_items:
                created_ts = item.get("time")
                created_at_iso = (
                    datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat()
                    if created_ts
                    else datetime.now(timezone.utc).isoformat())
                event_data = {
                    "source": "hacker_news",
                    "external_id": str(item["id"]),
                    "title": f"[{item.get('type', 'story')}] {item.get('title', 'Без заголовка')}",
                    "payload": item,
                    "created_at": created_at_iso,}
                await pipeline.rpush(QUEUE_NAME, json.dumps(event_data))
                pushed_count += 1
            await pipeline.execute()
        finally:
            await redis_client.aclose()
        return pushed_count

    async def _loop(self, interval_seconds: int = 15):
        while self._is_running:
            try:
                count = await self.fetch_and_push()
                logger.info(f"[HN Fetcher] Загружено и отправлено в Redis новостей: {count}")
            except Exception as e:
                logger.error(f"[HN Fetcher] Ошибка при откачке Hacker News: {e}")
            await asyncio.sleep(interval_seconds)

    def start_background_polling(self, interval_seconds: int = 15) -> bool:
        if self._is_running:
            return False
        self._is_running = True
        self._bg_task = asyncio.create_task(self._loop(interval_seconds))
        return True

    async def stop_background_polling(self) -> bool:
        if not self._is_running:
            return False
        self._is_running = False
        if self._bg_task:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            self._bg_task = None
        return True
    @property
    def is_running(self) -> bool:
        return self._is_running

hn_fetcher = HackerNewsFetcherService()