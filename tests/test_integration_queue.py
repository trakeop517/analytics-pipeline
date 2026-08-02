import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from sqlalchemy.exc import OperationalError
from app.consumers.worker import (MAX_RETRIES, QUEUE_NAME, save_batch_with_retry,worker_loop,)

def make_session_factory_mock() -> MagicMock:
    """Создаёт заглушку  с транзакцией"""
    session = MagicMock()
    transaction_context = MagicMock()
    transaction_context.__aenter__ = AsyncMock(return_value=None)
    transaction_context.__aexit__ = AsyncMock(return_value=False)
    session.begin.return_value = transaction_context
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=session_context)

@pytest.mark.asyncio
async def test_worker_loop_flushes_received_event_on_shutdown():
    """Воркер получает событие из Redis и при остановке передаёт накопленный батч на обработку"""
    event = {"source": "github", "external_id": "555", "title": "New Commit", "payload": {"repo": "analytics"},}
    raw_event = json.dumps(event)
    redis_client = AsyncMock()
    redis_client.blpop.side_effect = [(QUEUE_NAME, raw_event), asyncio.CancelledError(),]
    captured_events = []
    async def capture_batch(worker_id: int, events: list[dict], redis,) -> bool:
        captured_events.extend(event.copy() for event in events)
        return True
    with (patch("app.consumers.worker.save_batch_with_retry", side_effect=capture_batch) as save_batch_mock, patch("app.consumers.worker.record_worker_started"), patch("app.consumers.worker.record_worker_stopped")):
        with pytest.raises(asyncio.CancelledError):
            await worker_loop(worker_id=1,redis_client=redis_client)
    save_batch_mock.assert_awaited_once()
    assert captured_events == [event]


@pytest.mark.asyncio
async def test_save_batch_retries_after_database_error():
    """ После временной ошибки PostgreSQL выполняется повторная попытка, а затем батч успешно сохраняется"""
    events = [{"source": "hn","external_id": "777", "title": "Temporary database failure"}]
    database_error = OperationalError("INSERT INTO events", {},Exception("PostgreSQL temporarily unavailable"))
    redis_client = AsyncMock()
    repository = MagicMock()
    repository.create_many = AsyncMock(side_effect=[database_error, None])
    session_factory = make_session_factory_mock()
    with (patch("app.consumers.worker.async_session_factory", new=session_factory), patch("app.consumers.worker.EventRepository", return_value=repository),
        patch("app.consumers.worker.update_retry_stats", new_callable=AsyncMock,) as retry_stats_mock,
        patch("app.consumers.worker.update_processed_stats", new_callable=AsyncMock),
        patch("app.consumers.worker.record_retry_metrics"),
        patch("app.consumers.worker.record_events_status"),
        patch("app.consumers.worker.record_batch_latency"),
        patch("app.consumers.worker.asyncio.sleep", new_callable=AsyncMock,) as sleep_mock):
        result = await save_batch_with_retry(worker_id=1, events=events, redis_client=redis_client,)
    assert result is True
    assert repository.create_many.await_count == 2
    retry_stats_mock.assert_awaited_once_with(1, redis_client,)
    sleep_mock.assert_awaited_once()

@pytest.mark.asyncio
async def test_exhausted_retries_delegate_failed_batch():
    """После исчерпания попыток батч передаётся обработчику, который отвечает за DLQ или возврат в основную очередь."""
    events = [{"source": "hn", "external_id": "888", "title": "Permanent database failure",}]
    database_error = OperationalError("INSERT INTO events", {}, Exception("PostgreSQL unavailable"),)
    redis_client = AsyncMock()
    repository = MagicMock()
    repository.create_many = AsyncMock(side_effect=database_error,)
    session_factory = make_session_factory_mock()
    with (
        patch("app.consumers.worker.async_session_factory", new=session_factory),
        patch("app.consumers.worker.EventRepository", return_value=repository),
        patch("app.consumers.worker.update_retry_stats", new_callable=AsyncMock),
        patch("app.consumers.worker.record_retry_metrics"),
        patch("app.consumers.worker.handle_failed_batch", new_callable=AsyncMock, return_value=True) as failed_batch_mock,
        patch("app.consumers.worker.asyncio.sleep", new_callable=AsyncMock) as sleep_mock):
        result = await save_batch_with_retry(worker_id=1, events=events, redis_client=redis_client,)
    assert result is True
    assert repository.create_many.await_count == MAX_RETRIES
    failed_batch_mock.assert_awaited_once_with(1, events, redis_client)
    assert sleep_mock.await_count == MAX_RETRIES - 1