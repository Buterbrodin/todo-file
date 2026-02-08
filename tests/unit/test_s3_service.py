from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.exceptions import S3ServiceError
from app.services.s3_service import S3Service, infer_bucket


@pytest.mark.asyncio
async def test_upload_file_success():
    """Test successful file upload to S3."""
    service = S3Service()
    file_content = BytesIO(b"test file content")
    file_key = "avatars/1/test.jpg"
    bucket = "avatars"

    mock_s3_client = AsyncMock()
    mock_context = MagicMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_s3_client)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_context

    service._session = mock_session
    service._buckets_initialized = True

    with patch("app.services.s3_service.settings") as mock_settings:
        mock_settings.S3_ENDPOINT_URL = "http://localstack:4566"
        mock_settings.S3_ACCESS_KEY_ID = "test"
        mock_settings.S3_SECRET_ACCESS_KEY = "test"
        mock_settings.S3_REGION = "us-east-1"
        mock_settings.S3_USE_SSL = False

        result = await service.upload_file(
            file_content,
            bucket=bucket,
            key=file_key,
            content_type="image/jpeg",
        )

        mock_s3_client.upload_fileobj.assert_called_once()
        assert "avatars" in result
        assert file_key in result


@pytest.mark.asyncio
async def test_upload_file_no_session():
    """Test upload raises error when S3 session is not available."""
    service = S3Service()
    file_content = BytesIO(b"test file content")
    file_key = "avatars/1/test.jpg"
    bucket = "avatars"

    with pytest.raises(S3ServiceError) as exc_info:
        await service.upload_file(
            file_content,
            bucket=bucket,
            key=file_key,
            content_type="image/jpeg",
        )

    assert "unavailable" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_delete_file_success():
    """Test successful file deletion from S3."""
    service = S3Service()
    bucket = "avatars"
    key = "avatars/1/test.jpg"

    mock_s3_client = AsyncMock()
    mock_context = MagicMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_s3_client)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_context

    service._session = mock_session

    with patch("app.services.s3_service.settings") as mock_settings:
        mock_settings.S3_ENDPOINT_URL = "http://localstack:4566"
        mock_settings.S3_ACCESS_KEY_ID = "test"
        mock_settings.S3_SECRET_ACCESS_KEY = "test"
        mock_settings.S3_REGION = "us-east-1"

        result = await service.delete_file(bucket=bucket, key=key)

        mock_s3_client.delete_object.assert_called_once_with(Bucket=bucket, Key=key)
        assert result is True


@pytest.mark.asyncio
async def test_delete_file_no_session():
    """Test delete behavior when S3 session is not available."""
    service = S3Service()
    bucket = "avatars"
    key = "avatars/1/test.jpg"

    with patch.object(service, "_ensure_session"):
        service._session = None
        result = await service.delete_file(bucket=bucket, key=key)

    assert result is False


@pytest.mark.asyncio
async def test_get_file_url():
    """Test file URL generation."""
    service = S3Service()
    bucket = "avatars"
    key = "avatars/1/test.jpg"

    with patch("app.services.s3_service.settings") as mock_settings:
        mock_settings.S3_ENDPOINT_URL = "http://localstack:4566"
        mock_settings.S3_USE_SSL = False

        result = service.get_file_url(bucket=bucket, key=key)

        assert result == f"http://localstack:4566/{bucket}/{key}"


@pytest.mark.asyncio
async def test_update_file():
    """Test file update operation."""
    service = S3Service()
    file_content = BytesIO(b"updated content")
    bucket = "avatars"
    key = "avatars/1/test.jpg"

    with patch.object(service, "upload_file", new_callable=AsyncMock) as mock_upload:
        mock_upload.return_value = f"http://localstack:4566/{bucket}/{key}"

        result = await service.update_file(
            file_content,
            bucket=bucket,
            key=key,
            content_type="image/jpeg",
        )

        mock_upload.assert_called_once_with(
            file_content,
            bucket,
            key,
            "image/jpeg",
        )
        assert result == f"http://localstack:4566/{bucket}/{key}"


@pytest.mark.asyncio
async def test_ensure_buckets():
    """Test bucket creation during initialization."""
    service = S3Service()
    buckets = ["avatars", "project-logos", "task-logos", "task-attachments"]

    mock_s3_client = AsyncMock()
    mock_s3_client.head_bucket.side_effect = Exception("Not found")
    mock_context = MagicMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_s3_client)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_context

    service._session = mock_session

    with patch("app.services.s3_service.settings") as mock_settings:
        mock_settings.S3_ENDPOINT_URL = "http://localstack:4566"
        mock_settings.S3_ACCESS_KEY_ID = "test"
        mock_settings.S3_SECRET_ACCESS_KEY = "test"
        mock_settings.S3_REGION = "us-east-1"
        mock_settings.S3_BUCKET_AVATARS = "avatars"
        mock_settings.S3_BUCKET_PROJECT_LOGOS = "project-logos"
        mock_settings.S3_BUCKET_TASK_LOGOS = "task-logos"
        mock_settings.S3_BUCKET_TASK_ATTACHMENTS = "task-attachments"

        await service._ensure_buckets()

        assert mock_s3_client.create_bucket.call_count == len(buckets)
        assert service._buckets_initialized is True


def test_infer_bucket():
    """Test bucket inference from file type."""
    with patch("app.services.s3_service.settings") as mock_settings:
        mock_settings.S3_BUCKET_AVATARS = "avatars"
        mock_settings.S3_BUCKET_PROJECT_LOGOS = "project-logos"
        mock_settings.S3_BUCKET_TASK_LOGOS = "task-logos"
        mock_settings.S3_BUCKET_TASK_ATTACHMENTS = "task-attachments"

        assert infer_bucket("avatar") == "avatars"
        assert infer_bucket("project_logo") == "project-logos"
        assert infer_bucket("task_logo") == "task-logos"
        assert infer_bucket("task_attachment") == "task-attachments"
        assert infer_bucket("unknown") == "task-attachments"
