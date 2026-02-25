"""Unit tests for Kafka request consumer."""

import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.core.security import UserPrincipal
from app.models.file import FileMetadata
from app.services.kafka_request_consumer import KafkaRequestConsumer
from app.settings import settings


@pytest.fixture
def test_user() -> UserPrincipal:
    """Create test user principal."""
    return UserPrincipal(user_id=1, roles=["member"], email="test@example.com")


@pytest.fixture
def test_admin() -> UserPrincipal:
    """Create test admin principal."""
    return UserPrincipal(user_id=2, roles=["admin"], email="admin@example.com")


@pytest.fixture
def valid_png_data() -> bytes:
    """Create valid PNG file data."""
    return b"\x89PNG\r\n\x1a\n" + b"0" * 100


@pytest.fixture
def kafka_consumer() -> KafkaRequestConsumer:
    """Create Kafka request consumer instance."""
    return KafkaRequestConsumer()


@pytest.mark.asyncio
async def test_consumer_start(kafka_consumer: KafkaRequestConsumer):
    """Test Kafka consumer startup."""
    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer_cls = MagicMock(return_value=mock_consumer)

    with patch.object(kafka_consumer, "_load_consumer_cls"):
        kafka_consumer._consumer_cls = mock_consumer_cls
        with patch("app.services.kafka_request_consumer.settings") as mock_settings:
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
            mock_settings.KAFKA_TOPIC_FILE_UPLOAD_REQUEST = "file.upload.request"
            mock_settings.KAFKA_TOPIC_FILE_DELETE_REQUEST = "file.delete.request"
            mock_settings.KAFKA_TOPIC_FILE_LIST_REQUEST = "file.list.request"

            await kafka_consumer.start()

            assert kafka_consumer._consumer is not None
            assert kafka_consumer._running is True
            mock_consumer.start.assert_called_once()


@pytest.mark.asyncio
async def test_consumer_start_no_bootstrap_servers(
    kafka_consumer: KafkaRequestConsumer,
):
    """Test consumer does not start without bootstrap servers."""
    with patch("app.services.kafka_request_consumer.settings") as mock_settings:
        mock_settings.KAFKA_BOOTSTRAP_SERVERS = None

        await kafka_consumer.start()

        assert kafka_consumer._consumer is None
        assert kafka_consumer._running is False


@pytest.mark.asyncio
async def test_consumer_stop(kafka_consumer: KafkaRequestConsumer):
    """Test Kafka consumer graceful shutdown."""
    mock_consumer = AsyncMock()
    mock_consumer.stop = AsyncMock()
    kafka_consumer._consumer = mock_consumer
    kafka_consumer._running = True
    kafka_consumer._consume_task = None

    await kafka_consumer.stop()

    mock_consumer.stop.assert_called_once()
    assert kafka_consumer._consumer is None
    assert kafka_consumer._running is False


@pytest.mark.asyncio
async def test_handle_upload_request_success(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    test_db,
    valid_png_data: bytes,
):
    """Test successful file upload request handling."""
    request_id = "test-request-123"
    file_data_b64 = base64.b64encode(valid_png_data).decode("utf-8")

    payload = {
        "request_id": request_id,
        "token": "valid-token",
        "file_data": file_data_b64,
        "file_name": "test.png",
        "content_type": "image/png",
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
        "user_id": 1,
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.validate_entity_exists",
            new_callable=AsyncMock,
        ) as mock_validate:
            with patch(
                "app.services.kafka_request_consumer.s3_service.upload_file",
                new_callable=AsyncMock,
                return_value="http://test-s3/avatars/test.png",
            ) as mock_upload:
                with patch(
                    "app.services.kafka_request_consumer.kafka_service._send",
                    new_callable=AsyncMock,
                ) as mock_kafka_send:
                    await kafka_consumer._handle_upload_request(payload)

                    mock_validate.assert_called_once()
                    mock_upload.assert_called_once()
                    assert mock_kafka_send.called
                    # Verify file.uploaded event was sent
                    call_args = mock_kafka_send.call_args_list
                    uploaded_call = next(
                        (c for c in call_args if "file.uploaded" in str(c)), None
                    )
                    response_call = next(
                        (c for c in call_args if "file.upload.response" in str(c)), None
                    )
                    assert uploaded_call is not None
                    assert response_call is not None
                    assert response_call[0][1]["status"] == "ok"
                    assert response_call[0][1]["request_id"] == request_id


@pytest.mark.asyncio
async def test_handle_upload_request_missing_token(
    kafka_consumer: KafkaRequestConsumer,
):
    """Test upload request with missing token."""
    payload = {
        "request_id": "test-request-123",
        "file_data": base64.b64encode(b"test").decode("utf-8"),
    }

    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._handle_upload_request(payload)

        # Verify error response was sent
        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert "file.upload.response" in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "error"
        assert "Missing authentication token" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_upload_request_invalid_token(
    kafka_consumer: KafkaRequestConsumer,
):
    """Test upload request with invalid token."""
    payload = {
        "request_id": "test-request-123",
        "token": "invalid-token",
        "file_data": base64.b64encode(b"test").decode("utf-8"),
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token",
        side_effect=Exception("Invalid token"),
    ):
        with patch(
            "app.services.kafka_request_consumer.kafka_service._send",
            new_callable=AsyncMock,
        ) as mock_kafka_send:
            await kafka_consumer._handle_upload_request(payload)

            # Verify error response was sent
            assert mock_kafka_send.called
            call_args = mock_kafka_send.call_args
            response_payload = call_args[0][1]
            assert response_payload["status"] == "error"
            assert "Invalid authentication token" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_upload_request_missing_fields(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test upload request with missing required fields."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "file_data": base64.b64encode(b"test").decode("utf-8"),
        # Missing file_type, entity_type, entity_id
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.kafka_service._send",
            new_callable=AsyncMock,
        ) as mock_kafka_send:
            await kafka_consumer._handle_upload_request(payload)

            # Verify error response was sent
            assert mock_kafka_send.called
            call_args = mock_kafka_send.call_args
            response_payload = call_args[0][1]
            assert response_payload["status"] == "error"
            assert "Missing required fields" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_upload_request_validation_error_detail(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test upload request returns HTTPException detail from validators."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "file_data": base64.b64encode(b"test").decode("utf-8"),
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
        "content_type": "image/invalid",
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.validate_entity_exists",
            new_callable=AsyncMock,
        ):
            with patch(
                "app.services.kafka_request_consumer.validate_content_type",
                side_effect=HTTPException(
                    status_code=400, detail="Invalid content type"
                ),
            ):
                with patch(
                    "app.services.kafka_request_consumer.kafka_service._send",
                    new_callable=AsyncMock,
                ) as mock_kafka_send:
                    await kafka_consumer._handle_upload_request(payload)

                    assert mock_kafka_send.called
                    response_payload = mock_kafka_send.call_args[0][1]
                    assert response_payload["status"] == "error"
                    assert response_payload["detail"] == "Invalid content type"


@pytest.mark.asyncio
async def test_handle_upload_request_coerces_entity_and_user_ids(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    valid_png_data: bytes,
):
    """Test upload request coercion for entity_id/user_id from Kafka payload."""
    request_id = "test-request-123"
    payload = {
        "request_id": request_id,
        "token": "valid-token",
        "file_data": base64.b64encode(valid_png_data).decode("utf-8"),
        "file_name": "test.png",
        "content_type": "image/png",
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1.0,
        "user_id": "1",
    }

    meta = MagicMock()
    meta.id = 1
    meta.url = "http://test-s3/avatars/test.png"
    meta.created_at = datetime.now(timezone.utc)

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.validate_entity_exists",
            new_callable=AsyncMock,
        ):
            with patch.object(
                kafka_consumer,
                "_process_upload_file",
                new_callable=AsyncMock,
                return_value=meta,
            ) as mock_process:
                with patch(
                    "app.services.kafka_request_consumer.kafka_service._send",
                    new_callable=AsyncMock,
                ):
                    await kafka_consumer._handle_upload_request(payload)

                    process_args = mock_process.call_args[0]
                    assert process_args[6] == 1  # entity_id
                    assert process_args[7] == 1  # user_id


@pytest.mark.asyncio
async def test_handle_upload_request_invalid_entity_id_type(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test upload request rejects non-coercible entity_id."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "file_data": base64.b64encode(b"test").decode("utf-8"),
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": "not-an-int",
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.kafka_service._send",
            new_callable=AsyncMock,
        ) as mock_kafka_send:
            await kafka_consumer._handle_upload_request(payload)

            response_payload = mock_kafka_send.call_args[0][1]
            assert response_payload["status"] == "error"
            assert response_payload["detail"] == "entity_id must be an integer"


@pytest.mark.asyncio
async def test_handle_delete_request_success(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    test_db,
):
    """Test successful file delete request handling."""
    # Create test file metadata
    file_meta = FileMetadata(
        file_key="avatar/user/1/test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=1,
        uploader_id=1,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test-s3/avatars/test.jpg",
        created_at=datetime.now(timezone.utc),
    )
    test_db.add(file_meta)
    await test_db.commit()
    await test_db.refresh(file_meta)

    request_id = "test-request-123"
    payload = {
        "request_id": request_id,
        "token": "valid-token",
        "file_id": file_meta.id,
    }

    @asynccontextmanager
    async def mock_get_db_session():
        """Mock get_db_session to return test_db."""
        yield test_db

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):

        def get_mock_session():
            return mock_get_db_session()

        with patch(
            "app.services.kafka_request_consumer.get_db_session",
            side_effect=get_mock_session,
        ):
            with patch(
                "app.services.kafka_request_consumer.check_file_permission",
                new_callable=AsyncMock,
            ):
                with patch(
                    "app.services.kafka_request_consumer.s3_service.delete_file",
                    new_callable=AsyncMock,
                ) as mock_delete:
                    with patch(
                        "app.services.kafka_request_consumer.kafka_service._send",
                        new_callable=AsyncMock,
                    ) as mock_kafka_send:
                        await kafka_consumer._handle_delete_request(payload)

                        mock_delete.assert_called_once()
                        assert mock_kafka_send.called
                        # Verify file.deleted event was sent
                        call_args = mock_kafka_send.call_args_list
                        deleted_call = next(
                            (c for c in call_args if "file.deleted" in str(c)), None
                        )
                        response_call = next(
                            (c for c in call_args if "file.delete.response" in str(c)),
                            None,
                        )
                        assert deleted_call is not None
                        assert response_call is not None
                        assert response_call[0][1]["status"] == "ok"
                        assert response_call[0][1]["request_id"] == request_id


@pytest.mark.asyncio
async def test_handle_delete_request_file_not_found(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    test_db,
):
    """Test delete request for non-existent file."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "file_id": 99999,  # Non-existent ID
    }

    @asynccontextmanager
    async def mock_get_db_session():
        """Mock get_db_session to return test_db."""
        yield test_db

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):

        def get_mock_session():
            return mock_get_db_session()

        with patch(
            "app.services.kafka_request_consumer.get_db_session",
            side_effect=get_mock_session,
        ):
            with patch(
                "app.services.kafka_request_consumer.kafka_service._send",
                new_callable=AsyncMock,
            ) as mock_kafka_send:
                await kafka_consumer._handle_delete_request(payload)

                # Verify error response was sent
                assert mock_kafka_send.called
                call_args = mock_kafka_send.call_args
                response_payload = call_args[0][1]
                assert response_payload["status"] == "error"
                assert "File not found" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_list_request_success(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    test_db,
):
    """Test successful file list request handling."""
    # Create test file metadata
    file_meta = FileMetadata(
        file_key="avatar/user/1/test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=1,
        uploader_id=1,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test-s3/avatars/test.jpg",
        created_at=datetime.now(timezone.utc),
    )
    test_db.add(file_meta)
    await test_db.commit()

    request_id = "test-request-123"
    payload = {
        "request_id": request_id,
        "token": "valid-token",
        "entity_type": "user",
        "entity_id": 1,
        "page": 1,
        "page_size": 20,
    }

    @asynccontextmanager
    async def mock_get_db_session():
        """Mock get_db_session to return test_db."""
        yield test_db

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):

        def get_mock_session():
            return mock_get_db_session()

        with patch(
            "app.services.kafka_request_consumer.get_db_session",
            side_effect=get_mock_session,
        ):
            with patch(
                "app.services.kafka_request_consumer.check_list_files_access",
                new_callable=AsyncMock,
            ) as mock_check_access:
                with patch(
                    "app.services.kafka_request_consumer.kafka_service._send",
                    new_callable=AsyncMock,
                ) as mock_kafka_send:
                    await kafka_consumer._handle_list_request(payload)

                    mock_check_access.assert_called_once_with(test_user, "user", 1)
                    # Verify list response was sent
                    assert mock_kafka_send.called
                    call_args = mock_kafka_send.call_args
                    assert "file.list.response" in str(call_args[0][0])
                    response_payload = call_args[0][1]
                    assert response_payload["status"] == "ok"
                    assert "files" in response_payload
                    assert response_payload["request_id"] == request_id
                    assert response_payload["total"] == 1


@pytest.mark.asyncio
async def test_handle_list_request_missing_token(
    kafka_consumer: KafkaRequestConsumer,
):
    """Test list request with missing token."""
    payload = {"request_id": "test-request-123"}

    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._handle_list_request(payload)

        # Verify error response was sent
        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        response_payload = call_args[0][1]
        assert response_payload["status"] == "error"
        assert "Missing authentication token" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_list_request_missing_request_id(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test list request with missing request_id."""
    payload = {
        "token": "valid-token",
        # Missing request_id
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.kafka_service._send",
            new_callable=AsyncMock,
        ) as mock_kafka_send:
            await kafka_consumer._handle_list_request(payload)

            # Verify error response was sent
            assert mock_kafka_send.called
            call_args = mock_kafka_send.call_args
            response_payload = call_args[0][1]
            assert response_payload["status"] == "error"
            assert "Missing request_id" in response_payload["detail"]


@pytest.mark.asyncio
async def test_handle_list_request_invalid_page(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test list request with page < 1."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "entity_type": "user",
        "entity_id": 1,
        "page": 0,
        "page_size": 20,
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.check_list_files_access",
            new_callable=AsyncMock,
        ):
            with patch(
                "app.services.kafka_request_consumer.kafka_service._send",
                new_callable=AsyncMock,
            ) as mock_kafka_send:
                await kafka_consumer._handle_list_request(payload)

                response_payload = mock_kafka_send.call_args[0][1]
                assert response_payload["status"] == "error"
                assert (
                    response_payload["detail"]
                    == "page must be greater than or equal to 1"
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("page_size", [0, 101])
async def test_handle_list_request_invalid_page_size(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
    page_size: int,
):
    """Test list request with page_size outside [1, 100]."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "entity_type": "user",
        "entity_id": 1,
        "page": 1,
        "page_size": page_size,
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.check_list_files_access",
            new_callable=AsyncMock,
        ):
            with patch(
                "app.services.kafka_request_consumer.kafka_service._send",
                new_callable=AsyncMock,
            ) as mock_kafka_send:
                await kafka_consumer._handle_list_request(payload)

                response_payload = mock_kafka_send.call_args[0][1]
                assert response_payload["status"] == "error"
                assert (
                    response_payload["detail"] == "page_size must be between 1 and 100"
                )


@pytest.mark.asyncio
async def test_handle_list_request_non_integer_pagination(
    kafka_consumer: KafkaRequestConsumer,
    test_user: UserPrincipal,
):
    """Test list request with non-integer page/page_size."""
    payload = {
        "request_id": "test-request-123",
        "token": "valid-token",
        "entity_type": "user",
        "entity_id": 1,
        "page": "one",
        "page_size": "twenty",
    }

    with patch(
        "app.services.kafka_request_consumer.decode_token", return_value=test_user
    ):
        with patch(
            "app.services.kafka_request_consumer.check_list_files_access",
            new_callable=AsyncMock,
        ):
            with patch(
                "app.services.kafka_request_consumer.kafka_service._send",
                new_callable=AsyncMock,
            ) as mock_kafka_send:
                await kafka_consumer._handle_list_request(payload)

                response_payload = mock_kafka_send.call_args[0][1]
                assert response_payload["status"] == "error"
                assert (
                    response_payload["detail"]
                    == "Invalid pagination values: page and page_size must be integers"
                )


@pytest.mark.asyncio
async def test_send_upload_error(kafka_consumer: KafkaRequestConsumer):
    """Test sending upload error response."""
    request_id = "test-request-123"
    detail = "Test error"

    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._send_upload_error(request_id, detail)

        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert settings.KAFKA_TOPIC_FILE_UPLOAD_RESPONSE in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "error"
        assert response_payload["request_id"] == request_id
        assert response_payload["detail"] == detail


@pytest.mark.asyncio
async def test_send_upload_success(kafka_consumer: KafkaRequestConsumer):
    """Test sending upload success response."""
    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._send_upload_success(
            request_id="test-request-123",
            file_id=1,
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            url="http://test-s3/avatars/test.png",
            timestamp="2025-01-01T00:00:00+00:00",
            user_id=1,
        )

        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert settings.KAFKA_TOPIC_FILE_UPLOAD_RESPONSE in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "ok"
        assert response_payload["event"] == "file.upload.response"
        assert response_payload["request_id"] == "test-request-123"


@pytest.mark.asyncio
async def test_send_delete_error(kafka_consumer: KafkaRequestConsumer):
    """Test sending delete error response."""
    request_id = "test-request-123"
    detail = "Test error"

    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._send_delete_error(request_id, detail)

        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert settings.KAFKA_TOPIC_FILE_DELETE_RESPONSE in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "error"
        assert response_payload["request_id"] == request_id
        assert response_payload["detail"] == detail


@pytest.mark.asyncio
async def test_send_delete_success(kafka_consumer: KafkaRequestConsumer):
    """Test sending delete success response."""
    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._send_delete_success(
            request_id="test-request-123",
            file_id=1,
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            timestamp="2025-01-01T00:00:00+00:00",
            user_id=1,
        )

        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert settings.KAFKA_TOPIC_FILE_DELETE_RESPONSE in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "ok"
        assert response_payload["event"] == "file.delete.response"
        assert response_payload["request_id"] == "test-request-123"


@pytest.mark.asyncio
async def test_send_list_error(kafka_consumer: KafkaRequestConsumer):
    """Test sending list error response."""
    request_id = "test-request-123"
    detail = "Test error"

    with patch(
        "app.services.kafka_request_consumer.kafka_service._send",
        new_callable=AsyncMock,
    ) as mock_kafka_send:
        await kafka_consumer._send_list_error(request_id, detail)

        assert mock_kafka_send.called
        call_args = mock_kafka_send.call_args
        assert settings.KAFKA_TOPIC_FILE_LIST_RESPONSE in str(call_args[0][0])
        response_payload = call_args[0][1]
        assert response_payload["status"] == "error"
        assert response_payload["request_id"] == request_id
        assert response_payload["detail"] == detail
