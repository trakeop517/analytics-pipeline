import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import httpx
import redis.asyncio as redis
from app.core.config import settings
logger = logging.getLogger(__name__)

GITHUB_EVENTS_URL = "https://api.github.com/events"
QUEUE_NAME = settings.QUEUE_NAME
class GitHubFetcherService:
    def __init__(self):
        self._bg_task: Optional[asyncio.Task] = None
        self._is_running: bool = False
    async def fetch_and_push(self) -> int:
        headers = {
            "User-Agent": "AnalyticsPipelineApp/1.0",
            "Accept": "application/vnd.github.v3+json",}
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(GITHUB_EVENTS_URL, headers=headers)
            response.raise_for_status()
            raw_events: List[Dict[str, Any]] = response.json()
        if not raw_events:
            return 0
        redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        pushed_count = 0
        try:
            pipeline = redis_client.pipeline()
            for item in raw_events:
                event_data = {
                    "source": "github",
                    "external_id": str(item["id"]),  
                    "title": f"{item.get('type', 'UnknownEvent')} в {item.get('repo', {}).get('name', 'unknown')}",
                    "payload": item,
                    "created_at": item.get("created_at") or datetime.utcnow().isoformat()}
                await pipeline.rpush(QUEUE_NAME, json.dumps(event_data))
                pushed_count += 1
            await pipeline.execute()
        finally:
            await redis_client.aclose()
        return pushed_count
    async def _loop(self, interval_seconds: int = 10):
        while self._is_running:
            try:
                count = await self.fetch_and_push()
                logger.info(f"[GitHub Fetcher] Загружено и отправлено в Redis событий: {count}")
            except Exception as e:
                logger.error(f"[GitHub Fetcher] Ошибка при откачке событий: {e}")
            await asyncio.sleep(interval_seconds)

    def start_background_polling(self, interval_seconds: int = 10) -> bool:
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

github_fetcher = GitHubFetcherService()