import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from app.services.github_fetcher import github_fetcher
from app.services.hn_fetcher import hn_fetcher

@pytest.mark.asyncio
async def test_github_fetcher_success():
    """Тест: GitHubFetcher забирает события и кладет их в очереди Redis."""
    mock_github_response = [
        {"id": "123456", "type": "PushEvent", "repo": {"name": "octocat/Hello-World"}, "created_at": "2026-07-31T12:00:00Z"}]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_github_response
    mock_response.raise_for_status = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.rpush = AsyncMock()
    mock_pipeline.execute = AsyncMock()
    mock_redis = MagicMock()
    mock_redis.pipeline.return_value = mock_pipeline
    mock_redis.aclose = AsyncMock()
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get, \
         patch("redis.asyncio.from_url", return_value=mock_redis):
        mock_get.return_value = mock_response
        pushed_count = await github_fetcher.fetch_and_push()
        assert pushed_count == 1
        mock_pipeline.rpush.assert_called_once()
        queue_name, payload_str = mock_pipeline.rpush.call_args[0]
        assert queue_name == "events_queue"
        payload = json.loads(payload_str)
        assert payload["source"] == "github"
        assert payload["external_id"] == "123456"

@pytest.mark.asyncio
async def test_hn_fetcher_success():
    """Тест: HackerNewsFetcher забирает ID новостей, скачивает детали и кладет в Redis."""
    mock_story_ids = [9901]
    mock_story_item = {"id": 9901, "type": "story","title": "Show HN: Async Pipeline in Python", "time": 1700000000}
    mock_response_ids = MagicMock()
    mock_response_ids.status_code = 200
    mock_response_ids.json.return_value = mock_story_ids
    mock_response_ids.raise_for_status = MagicMock()
    mock_response_item = MagicMock()
    mock_response_item.status_code = 200
    mock_response_item.json.return_value = mock_story_item
    mock_pipeline = MagicMock()
    mock_pipeline.rpush = AsyncMock()
    mock_pipeline.execute = AsyncMock()
    mock_redis = MagicMock()
    mock_redis.pipeline.return_value = mock_pipeline
    mock_redis.aclose = AsyncMock()
    async def mock_get_side_effect(url, *args, **kwargs):
        if "newstories.json" in url:
            return mock_response_ids
        return mock_response_item
    with patch("httpx.AsyncClient.get", side_effect=mock_get_side_effect), \
         patch("redis.asyncio.from_url", return_value=mock_redis):
        pushed_count = await hn_fetcher.fetch_and_push(limit=1)
        assert pushed_count == 1
        mock_pipeline.rpush.assert_called_once()
        queue_name, payload_str = mock_pipeline.rpush.call_args[0]
        assert queue_name == "events_queue"
        payload = json.loads(payload_str)
        assert payload["source"] == "hacker_news"
        assert payload["external_id"] == "9901"

@pytest.mark.asyncio
async def test_hn_fetcher_empty_ids():
    """Тест: Если Hacker News возвращает пустой список ID, отправка не происходит."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    mock_response.raise_for_status = MagicMock()
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        pushed_count = await hn_fetcher.fetch_and_push()
        assert pushed_count == 0