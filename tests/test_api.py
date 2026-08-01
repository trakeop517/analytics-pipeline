import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.api.monitoring import get_redis_client


@pytest.mark.asyncio
async def test_root_health_endpoint():
    """Тест эндпоинта GET /health."""
    mock_redis = AsyncMock()
    mock_redis.llen.return_value = 5
    with patch.dict("app.main.app_state", {"redis": mock_redis}):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["redis_queue_size"] == 5

@pytest.mark.asyncio
async def test_monitoring_stats_endpoint():
    """Тест эндпоинта GET /monitoring/stats."""
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ["100", "5", "2"]
    with patch("app.api.monitoring.get_redis_client", return_value=mock_redis):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/monitoring/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["events_processed"] == 100
        assert data["events_failed_dlq"] == 5
        assert data["total_retries"] == 2
        assert "timestamp" in data

@pytest.mark.asyncio
async def test_monitoring_queue_status():
    """Тест эндпоинта GET /monitoring/queue и статуса системы."""
    mock_redis = AsyncMock()
    mock_redis.llen.side_effect = [1500, 10] 
    with patch("app.api.monitoring.get_redis_client", return_value=mock_redis):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/monitoring/queue")
        assert response.status_code == 200
        data = response.json()
        assert data["main_queue_length"] == 1500
        assert data["dead_letter_queue_length"] == 10
        assert data["status"] == "backed_up"

@pytest.mark.asyncio
async def test_monitoring_health_success():
    """Тест успешной проверки здоровья Redis GET /monitoring/health."""
    mock_redis = AsyncMock()
    mock_redis.ping.return_value = True
    with patch("app.api.monitoring.get_redis_client", return_value=mock_redis):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/monitoring/health")
        assert response.status_code == 200
        assert response.json() == {"status": "OK", "redis": "connected"}

@pytest.mark.asyncio
async def test_monitoring_health_redis_failure():
    """Тест падения Redis и возврата статуса 503 на GET /monitoring/health."""
    mock_redis = AsyncMock()
    mock_redis.ping.side_effect = Exception("Connection refused")
    with patch("app.api.monitoring.get_redis_client", return_value=mock_redis):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/monitoring/health")
        assert response.status_code == 503
        assert "Redis connection failed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_monitoring_prometheus_metrics():
    """Тест отдачи метрик Prometheus в формате PlainText GET /monitoring/metrics."""
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ["50", "2", "1"]
    mock_redis.llen.side_effect = [10, 0]  
    mock_redis.hgetall.return_value = {"worker_1": "30", "worker_2": "20"}
    async def override_get_redis():
        return mock_redis
    app.dependency_overrides[get_redis_client] = override_get_redis
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/monitoring/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]
        body = response.text
        assert "pipeline_events_processed_total 50" in body
        assert "pipeline_queue_length 10" in body
        assert 'pipeline_worker_processed_total{worker="worker_1"} 30' in body
    finally:
        app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_analytics_summary():
    """Тест эндпоинта аналитики GET /analytics/summary."""
    mock_session = AsyncMock()
    mock_res_counts = MagicMock()
    mock_res_counts.all.return_value = [("github", 15), ("hn", 10)]
    mock_event_1 = MagicMock(source="github", external_id="101", title="PR #1", created_at=None)
    mock_event_2 = MagicMock(source="hn", external_id="202", title="Post #2", created_at=None)
    mock_res_recent = MagicMock()
    mock_res_recent.scalars.return_value.all.return_value = [mock_event_1, mock_event_2]
    mock_session.execute.side_effect = [mock_res_counts, mock_res_recent]
    with patch("app.main.async_session_factory") as mock_factory:
        mock_factory.return_value.__aenter__.return_value = mock_session
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get("/analytics/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_events_in_db"] == 25
        assert data["by_source"] == {"github": 15, "hn": 10}
        assert len(data["latest_events"]) == 2
        assert data["latest_events"][0]["external_id"] == "101"