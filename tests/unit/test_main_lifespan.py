"""Unit tests for application lifespan behavior."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.main import app, lifespan


@pytest.mark.asyncio
async def test_lifespan_initializes_s3_session_before_buckets(monkeypatch):
    """Test S3 session is ensured before bucket initialization."""
    call_order: list[str] = []

    def ensure_session() -> None:
        call_order.append("session")

    async def ensure_buckets() -> None:
        call_order.append("buckets")

    monkeypatch.setattr("app.main.kafka_service.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.stop", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.stop", AsyncMock())
    monkeypatch.setattr("app.main.close_http_client", AsyncMock())
    monkeypatch.setattr("app.main.db.ensure_engine", MagicMock())
    monkeypatch.setattr("app.main.s3_service._ensure_session", ensure_session)
    monkeypatch.setattr("app.main.s3_service._ensure_buckets", ensure_buckets)
    monkeypatch.setattr("app.main.db.engine", None)

    async with lifespan(app):
        pass

    assert call_order == ["session", "buckets"]


@pytest.mark.asyncio
async def test_lifespan_disposes_runtime_db_engine(monkeypatch):
    """Test lifespan disposes runtime database engine on shutdown."""
    mock_engine = MagicMock()
    mock_engine.dispose = AsyncMock()

    monkeypatch.setattr("app.main.kafka_service.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.stop", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.stop", AsyncMock())
    monkeypatch.setattr("app.main.close_http_client", AsyncMock())
    monkeypatch.setattr("app.main.db.ensure_engine", MagicMock())
    monkeypatch.setattr("app.main.s3_service._ensure_session", MagicMock())
    monkeypatch.setattr("app.main.s3_service._ensure_buckets", AsyncMock())
    monkeypatch.setattr("app.main.db.engine", mock_engine)

    async with lifespan(app):
        pass

    mock_engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_lifespan_calls_db_ensure_engine(monkeypatch):
    """Test lifespan initializes DB engine during startup."""
    ensure_engine = MagicMock()

    monkeypatch.setattr("app.main.db.ensure_engine", ensure_engine)
    monkeypatch.setattr("app.main.kafka_service.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.stop", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.stop", AsyncMock())
    monkeypatch.setattr("app.main.close_http_client", AsyncMock())
    monkeypatch.setattr("app.main.s3_service._ensure_session", MagicMock())
    monkeypatch.setattr("app.main.s3_service._ensure_buckets", AsyncMock())
    monkeypatch.setattr("app.main.db.engine", None)

    async with lifespan(app):
        pass

    ensure_engine.assert_called_once()


@pytest.mark.asyncio
async def test_lifespan_fails_fast_when_db_init_fails(monkeypatch):
    """Test lifespan startup fails if DB initialization fails."""
    monkeypatch.setattr(
        "app.main.db.ensure_engine",
        MagicMock(side_effect=RuntimeError("db init failed")),
    )
    monkeypatch.setattr("app.main.kafka_service.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.stop", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_request_consumer.stop", AsyncMock())
    monkeypatch.setattr("app.main.close_http_client", AsyncMock())
    monkeypatch.setattr("app.main.s3_service._ensure_session", MagicMock())
    monkeypatch.setattr("app.main.s3_service._ensure_buckets", AsyncMock())
    monkeypatch.setattr("app.main.db.engine", None)

    with pytest.raises(RuntimeError, match="db init failed"):
        async with lifespan(app):
            pass
