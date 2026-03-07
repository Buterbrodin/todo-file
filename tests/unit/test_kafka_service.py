from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.kafka_service import KafkaService


@pytest.mark.asyncio
async def test_kafka_service_start():
    """Test Kafka producer startup."""
    service = KafkaService()

    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer_cls = MagicMock(return_value=mock_producer)

    with patch.object(service, "_load_producer_cls"):
        service._producer_cls = mock_producer_cls
        with patch("app.services.kafka_service.settings") as mock_settings:
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

            await service.start()

            assert service._producer is not None
            assert service._started is True
            mock_producer.start.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_service_start_no_bootstrap_servers():
    """Test Kafka producer does not start without bootstrap servers."""
    service = KafkaService()

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = None

        await service.start()

        assert service._producer is None
        assert service._started is False


@pytest.mark.asyncio
async def test_kafka_service_stop():
    """Test Kafka producer graceful shutdown."""
    service = KafkaService()
    mock_producer = AsyncMock()
    service._producer = mock_producer
    service._started = True

    await service.stop()

    mock_producer.stop.assert_called_once()
    assert service._producer is None
    assert service._started is False


@pytest.mark.asyncio
async def test_send_file_uploaded():
    """Test sending file.uploaded event."""
    service = KafkaService()
    mock_producer = AsyncMock()
    service._producer = mock_producer
    service._started = True

    payload = {
        "event": "file.uploaded",
        "file_id": 1,
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
        "url": "http://test.com/file.jpg",
        "timestamp": "2025-01-01T00:00:00",
        "user_id": 1,
    }

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        mock_settings.KAFKA_TOPIC_FILE_UPLOADED = "file.uploaded"

        result = await service.send_file_uploaded(payload)

        assert result is True
        mock_producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_send_file_deleted():
    """Test sending file.deleted event."""
    service = KafkaService()
    mock_producer = AsyncMock()
    service._producer = mock_producer
    service._started = True

    payload = {
        "event": "file.deleted",
        "file_id": 1,
        "timestamp": "2025-01-01T00:00:00",
        "user_id": 1,
    }

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        mock_settings.KAFKA_TOPIC_FILE_DELETED = "file.deleted"

        result = await service.send_file_deleted(payload)

        assert result is True
        mock_producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_send_file_updated():
    """Test sending file.updated event."""
    service = KafkaService()
    mock_producer = AsyncMock()
    service._producer = mock_producer
    service._started = True

    payload = {
        "event": "file.updated",
        "file_id": 1,
        "url": "http://test.com/file.jpg",
        "timestamp": "2025-01-01T00:00:00",
        "user_id": 1,
    }

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        mock_settings.KAFKA_TOPIC_FILE_UPDATED = "file.updated"

        result = await service.send_file_updated(payload)

        assert result is True
        mock_producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_send_no_bootstrap_servers():
    """Test message is not sent when bootstrap servers not configured."""
    service = KafkaService()

    payload = {"event": "file.uploaded", "file_id": 1}

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = None

        result = await service.send_file_uploaded(payload)

        assert result is False
        assert service._producer is None


@pytest.mark.asyncio
async def test_send_with_retry():
    """Test retry logic on send failure."""
    service = KafkaService()
    mock_producer = AsyncMock()
    mock_producer.send_and_wait.side_effect = [
        Exception("Connection failed"),
        Exception("Connection failed"),
        None,
    ]
    service._producer = mock_producer
    service._started = True
    service._retry_delay = 0.01

    payload = {"event": "file.uploaded", "file_id": 1}

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        mock_settings.KAFKA_TOPIC_FILE_UPLOADED = "file.uploaded"

        result = await service.send_file_uploaded(payload)

        assert result is True
        assert mock_producer.send_and_wait.call_count == 3


@pytest.mark.asyncio
async def test_send_all_retries_fail():
    """Test message drop when all retries fail."""
    service = KafkaService()
    mock_producer = AsyncMock()
    mock_producer.send_and_wait.side_effect = Exception("Connection failed")
    service._producer = mock_producer
    service._started = True
    service._max_retries = 3
    service._retry_delay = 0.01

    payload = {"event": "file.uploaded", "file_id": 1}

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
        mock_settings.KAFKA_TOPIC_FILE_UPLOADED = "file.uploaded"

        result = await service.send_file_uploaded(payload)

        assert result is False
        assert mock_producer.send_and_wait.call_count == 3


@pytest.mark.asyncio
async def test_stop_when_not_started():
    """Test stop is safe when producer not started."""
    service = KafkaService()
    service._producer = None
    service._started = False

    await service.stop()

    assert service._producer is None
    assert service._started is False


@pytest.mark.asyncio
async def test_start_already_started():
    """Test start is idempotent."""
    service = KafkaService()
    service._started = True
    mock_producer = AsyncMock()
    service._producer = mock_producer

    with patch("app.services.kafka_service.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

        await service.start()

        mock_producer.start.assert_not_called()


@pytest.mark.asyncio
async def test_send_starts_producer_if_needed():
    """Test send auto-starts producer if not started."""
    service = KafkaService()
    mock_producer = AsyncMock()
    mock_producer_cls = MagicMock(return_value=mock_producer)

    with patch.object(service, "_load_producer_cls"):
        service._producer_cls = mock_producer_cls
        with patch("app.services.kafka_service.settings") as mock_settings:
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
            mock_settings.KAFKA_TOPIC_FILE_UPLOADED = "file.uploaded"

            payload = {"event": "file.uploaded", "file_id": 1}
            result = await service.send_file_uploaded(payload)

            assert result is True
            mock_producer.start.assert_called_once()
