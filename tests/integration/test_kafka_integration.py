"""Integration tests for Kafka message publishing."""

import json
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import status


@pytest.mark.asyncio
async def test_kafka_message_on_file_upload(
    authenticated_client, test_db, test_image_file
):
    """Test that Kafka message is sent when file is uploaded."""
    test_image_file.seek(0)
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
    }

    # Mock Kafka producer
    mock_producer = AsyncMock()
    mock_send_and_wait = AsyncMock()
    mock_producer.send_and_wait = mock_send_and_wait

    with patch("app.services.kafka_service.kafka_service._producer", mock_producer):
        with patch("app.services.kafka_service.kafka_service._started", True):
            response = await authenticated_client.post(
                "/api/files/upload", files=files, data=data
            )

    assert response.status_code == status.HTTP_201_CREATED

    # Verify Kafka message was sent
    assert mock_send_and_wait.called
    call_args = mock_send_and_wait.call_args
    topic = call_args[0][0]
    message_bytes = call_args[0][1]

    # Parse message
    message = json.loads(message_bytes.decode("utf-8"))

    assert topic == "file.uploaded"
    assert message["event"] == "file.uploaded"
    assert message["file_type"] == "avatar"
    assert message["entity_type"] == "user"
    assert message["entity_id"] == 1
    assert "url" in message
    assert "timestamp" in message
    assert "user_id" in message


@pytest.mark.asyncio
async def test_kafka_message_on_file_delete(authenticated_client, test_file_metadata):
    """Test that Kafka message is sent when file is deleted."""
    mock_producer = AsyncMock()
    mock_send_and_wait = AsyncMock()
    mock_producer.send_and_wait = mock_send_and_wait

    with patch("app.services.kafka_service.kafka_service._producer", mock_producer):
        with patch("app.services.kafka_service.kafka_service._started", True):
            response = await authenticated_client.delete(
                f"/api/files/{test_file_metadata.id}"
            )

    assert response.status_code == status.HTTP_200_OK

    # Verify Kafka message was sent
    assert mock_send_and_wait.called
    call_args = mock_send_and_wait.call_args
    topic = call_args[0][0]
    message_bytes = call_args[0][1]

    message = json.loads(message_bytes.decode("utf-8"))

    assert topic == "file.deleted"
    assert message["event"] == "file.deleted"
    assert message["file_id"] == test_file_metadata.id
    assert message["file_type"] == test_file_metadata.file_type
    assert message["entity_type"] == test_file_metadata.entity_type
    assert message["entity_id"] == test_file_metadata.entity_id


@pytest.mark.asyncio
async def test_kafka_message_on_file_update(
    authenticated_client, test_file_metadata, test_image_file
):
    """Test that Kafka message is sent when file is updated."""
    test_image_file.seek(0)
    files = {"file": ("updated.png", test_image_file, "image/png")}
    data = {}

    mock_producer = AsyncMock()
    mock_send_and_wait = AsyncMock()
    mock_producer.send_and_wait = mock_send_and_wait

    with patch("app.services.kafka_service.kafka_service._producer", mock_producer):
        with patch("app.services.kafka_service.kafka_service._started", True):
            response = await authenticated_client.put(
                f"/api/files/{test_file_metadata.id}",
                files=files,
                data=data,
            )

    assert response.status_code == status.HTTP_200_OK

    # Verify Kafka message was sent
    assert mock_send_and_wait.called
    call_args = mock_send_and_wait.call_args
    topic = call_args[0][0]
    message_bytes = call_args[0][1]

    message = json.loads(message_bytes.decode("utf-8"))

    assert topic == "file.updated"
    assert message["event"] == "file.updated"
    assert message["file_id"] == test_file_metadata.id
    assert "url" in message
    assert "timestamp" in message
