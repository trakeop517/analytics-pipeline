import json
import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from app.main import redis_worker, QUEUE_NAME, DLQ_QUEUE_NAME, MAX_RETRIES


@pytest.mark.asyncio
async def test_redis_worker_successful_flow():
    """Интеграционный тест: Успешное вычитывание из Redis и сохранение в БД."""
    raw_event = json.dumps({"source": "github", "external_id": "555", "title": "New Commit", "payload": {"repo": "analytics"}})
    mock_redis = AsyncMock()
    mock_redis.blpop.side_effect = [(QUEUE_NAME, raw_event), asyncio.CancelledError()]
    mock_session = AsyncMock()
    with patch("redis.asyncio.from_url", return_value=mock_redis), \
         patch("app.main.async_session_factory") as mock_factory:
        mock_factory.return_value.__aenter__.return_value = mock_session
        try:
            await redis_worker(worker_id=1)
        except asyncio.CancelledError:
            pass
        assert mock_session.execute.called
        assert mock_session.commit.called

@pytest.mark.asyncio
async def test_redis_worker_error_and_retry_flow():
    """Интеграционный тест: Ошибка записи в БД приводит к инкременту retries и повторной отправке в очередь."""
    raw_event = json.dumps({"source": "hn", "external_id": "777", "title": "Broken story","retries": 1})
    mock_redis = AsyncMock()
    mock_redis.blpop.side_effect = [(QUEUE_NAME, raw_event), asyncio.CancelledError()]
    mock_redis.rpush = AsyncMock()
    mock_session = AsyncMock()
    mock_session.execute.side_effect = Exception("PostgreSQL disk space full")
    with patch("redis.asyncio.from_url", return_value=mock_redis), \
         patch("app.main.async_session_factory") as mock_factory, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        mock_factory.return_value.__aenter__.return_value = mock_session
        try:
            await redis_worker(worker_id=1)
        except asyncio.CancelledError:
            pass
        assert mock_redis.rpush.called
        pushed_queue, pushed_payload = mock_redis.rpush.call_args[0]
        assert pushed_queue == QUEUE_NAME
        pushed_data = json.loads(pushed_payload)
        assert pushed_data["retries"] == 2

@pytest.mark.asyncio
async def test_redis_worker_exceed_retries_moves_to_dlq():
    """Интеграционный тест: Превышение лимита ретраев отправляет событие в DLQ."""
    raw_event = json.dumps({"source": "hn","external_id": "888", "title": "Permanently failed story", "retries": MAX_RETRIES})
    mock_redis = AsyncMock()
    mock_redis.blpop.side_effect = [(QUEUE_NAME, raw_event), asyncio.CancelledError()]
    mock_redis.rpush = AsyncMock()
    mock_session = AsyncMock()
    mock_session.execute.side_effect = Exception("Fatal DB Error")
    with patch("redis.asyncio.from_url", return_value=mock_redis), \
         patch("app.main.async_session_factory") as mock_factory, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        mock_factory.return_value.__aenter__.return_value = mock_session
        try:
            await redis_worker(worker_id=1)
        except asyncio.CancelledError:
            pass
        assert mock_redis.rpush.called
        pushed_queue, pushed_payload = mock_redis.rpush.call_args[0]
        assert pushed_queue == DLQ_QUEUE_NAME
        assert pushed_payload == raw_event