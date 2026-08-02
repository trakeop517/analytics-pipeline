import json
import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.exc import OperationalError
from app.consumers.worker import save_batch_with_retry, worker_loop, MAX_RETRIES, DLQ_QUEUE_NAME

def create_mock_session():
    mock_session = AsyncMock()
    mock_begin_cm = MagicMock()
    mock_begin_cm.__aenter__ = AsyncMock()
    mock_begin_cm.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin_cm)
    return mock_session
@pytest.mark.asyncio
async def test_save_batch_success():
    """Тест: Батч событий успешно записывается в БД, а статистика обновляется в Redis."""
    events = [{"source": "github", "external_id": "1", "title": "Commit 1"}, {"source": "github", "external_id": "2", "title": "Commit 2"}]
    mock_session = create_mock_session()
    mock_redis = AsyncMock()
    mock_redis.incrby = AsyncMock()
    mock_redis.hincrby = AsyncMock()
    with patch("app.consumers.worker.async_session_factory") as mock_factory, \
         patch("app.consumers.worker.EventRepository") as mock_repo_class:
        mock_factory.return_value.__aenter__.return_value = mock_session
        mock_repo_instance = MagicMock()
        mock_repo_instance.create_many = AsyncMock()
        mock_repo_class.return_value = mock_repo_instance
        result = await save_batch_with_retry(worker_id=1, events=events, redis_client=mock_redis)
        assert result is True
        mock_repo_instance.create_many.assert_called_once_with(events)
        mock_redis.incrby.assert_called_once_with("stats:processed", 2)
        mock_redis.hincrby.assert_called_once_with("stats:workers", "worker_1", 2)

@pytest.mark.asyncio
async def test_save_batch_db_error_triggers_dlq():
    """Тест: При постоянной ошибке БД воркер делает ретраи и отправляет батч в DLQ."""
    events = [{"source": "hn", "external_id": "100", "title": "Fail story"}]
    mock_session = create_mock_session()
    mock_redis = AsyncMock()
    mock_redis.incr = AsyncMock()
    mock_redis.incrby = AsyncMock()
    mock_redis.rpush = AsyncMock()
    with patch("app.consumers.worker.async_session_factory") as mock_factory, \
         patch("app.consumers.worker.EventRepository") as mock_repo_class, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        mock_factory.return_value.__aenter__.return_value = mock_session
        mock_repo_instance = MagicMock()
        mock_repo_instance.create_many = AsyncMock(side_effect=OperationalError("DB dead", params=None, orig=None))
        mock_repo_class.return_value = mock_repo_instance
        result = await save_batch_with_retry(worker_id=1, events=events, redis_client=mock_redis,)
        assert result is True
        assert mock_repo_instance.create_many.await_count == MAX_RETRIES
        assert mock_redis.incr.await_count == MAX_RETRIES - 1
        mock_redis.rpush.assert_awaited_once()
        pushed_queue, pushed_payload = mock_redis.rpush.await_args.args
        assert pushed_queue == DLQ_QUEUE_NAME
        assert json.loads(pushed_payload) == events[0]
        mock_redis.incrby.assert_awaited_once_with("stats:failed", 1)
@pytest.mark.asyncio
async def test_worker_loop_parses_iso_date_and_flushes_batch():
    """Тест: Цикл вычитывает события из Redis, преобразует ISO-даты в datetime и сбрасывает батч."""
    raw_event = json.dumps({"source": "github", "external_id": "999", "created_at": "2026-07-31T12:00:00"})
    mock_redis = MagicMock()
    mock_redis.blpop = AsyncMock(side_effect=[("events_queue", raw_event), asyncio.CancelledError()])
    captured_batches = []
    async def mock_save_side_effect(worker_id, events, redis_client):
        captured_batches.append([dict(e) for e in events])
        return True
    with patch("app.consumers.worker.save_batch_with_retry", side_effect=mock_save_side_effect), \
         patch("app.consumers.worker.BATCH_SIZE", 1):
        with pytest.raises(asyncio.CancelledError):
            await worker_loop(worker_id=1, redis_client=mock_redis)
        assert len(captured_batches) == 1
        saved_batch = captured_batches[0]
        assert len(saved_batch) == 1
        assert isinstance(saved_batch[0]["created_at"], datetime)

@pytest.mark.asyncio
async def test_worker_loop_flushes_remaining_buffer_on_cancel():
    """Тест: При остановке воркера (CancelledError) нераспределенный буфер сохраняется в базу."""
    raw_event = json.dumps({"source": "github", "external_id": "777"})
    mock_redis = MagicMock()
    mock_redis.blpop = AsyncMock(side_effect=[("events_queue", raw_event), asyncio.CancelledError()])
    captured_batches = []
    async def mock_save_side_effect(worker_id, events, redis_client):
        captured_batches.append([dict(e) for e in events])
        return True
    with patch("app.consumers.worker.save_batch_with_retry", side_effect=mock_save_side_effect), \
         patch("app.consumers.worker.BATCH_SIZE", 100):
        with pytest.raises(asyncio.CancelledError):
            await worker_loop(worker_id=1, redis_client=mock_redis)
        assert len(captured_batches) == 1
        saved_batch = captured_batches[0]
        assert len(saved_batch) == 1
        assert saved_batch[0]["external_id"] == "777"