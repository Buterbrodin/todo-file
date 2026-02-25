import logging
from typing import Any, BinaryIO, Optional

from app.core.exceptions import S3ServiceError
from app.settings import settings

logger = logging.getLogger(__name__)


class S3Service:
    """Service for S3/MinIO file operations."""

    def __init__(self) -> None:
        """Initialize S3 service."""
        self._session: Optional[Any] = None
        self._buckets_initialized = False

    def _ensure_session(self) -> None:
        """Create aioboto3 session if not exists."""
        if self._session is not None:
            return
        try:
            import aioboto3

            self._session = aioboto3.Session()
        except ModuleNotFoundError:
            logger.warning("aioboto3 not installed, S3 will be disabled")
            self._session = None

    def _get_client_config(self) -> dict[str, Any]:
        """Get S3 client configuration."""
        return {
            "endpoint_url": settings.S3_ENDPOINT_URL,
            "aws_access_key_id": settings.S3_ACCESS_KEY_ID or "test",
            "aws_secret_access_key": settings.S3_SECRET_ACCESS_KEY or "test",
            "region_name": settings.S3_REGION,
        }

    async def _ensure_buckets(self) -> None:
        """Create required buckets if they don't exist."""
        if self._buckets_initialized or self._session is None:
            return
        if not settings.S3_ENDPOINT_URL:
            return

        buckets = [
            settings.S3_BUCKET_AVATARS,
            settings.S3_BUCKET_PROJECT_LOGOS,
            settings.S3_BUCKET_TASK_LOGOS,
            settings.S3_BUCKET_TASK_ATTACHMENTS,
        ]

        try:
            all_ready = True
            async with self._session.client("s3", **self._get_client_config()) as s3:
                for bucket in buckets:
                    try:
                        await s3.head_bucket(Bucket=bucket)
                    except Exception:
                        # Bucket doesn't exist, try to create it
                        try:
                            await s3.create_bucket(Bucket=bucket)
                            logger.info("Created bucket: %s", bucket)
                        except Exception as create_exc:
                            all_ready = False
                            logger.warning(
                                "Failed to create bucket %s: %s", bucket, create_exc
                            )

            self._buckets_initialized = all_ready
        except Exception as exc:
            logger.error("Failed to ensure buckets: %s", exc)

    async def upload_file(
        self,
        file_obj: BinaryIO,
        bucket: str,
        key: str,
        content_type: str,
    ) -> str:
        """
        Upload file to S3.

        Args:
            file_obj: File-like object to upload.
            bucket: Target bucket name.
            key: Object key (path) in bucket.
            content_type: MIME type of the file.

        Returns:
            URL of uploaded file.

        Raises:
            S3ServiceError: If upload fails.
        """
        self._ensure_session()
        if self._session is None:
            raise S3ServiceError("S3 client is unavailable")

        await self._ensure_buckets()

        try:
            if hasattr(file_obj, "seek"):
                file_obj.seek(0)
            async with self._session.client("s3", **self._get_client_config()) as s3:
                await s3.upload_fileobj(
                    file_obj,
                    bucket,
                    key,
                    ExtraArgs={"ContentType": content_type},
                )
            logger.debug("Uploaded file to s3://%s/%s", bucket, key)
        except (AttributeError, TypeError) as exc:
            logger.error("Invalid file object: %s", exc)
            raise S3ServiceError(f"Invalid file object: {exc}") from exc
        except Exception as exc:
            logger.error("Failed to upload file: %s", exc)
            raise S3ServiceError(f"Failed to upload file: {exc}") from exc

        return self.get_file_url(bucket, key)

    async def delete_file(self, bucket: str, key: str) -> bool:
        """
        Delete file from S3.

        Args:
            bucket: Bucket name.
            key: Object key to delete.

        Returns:
            True if deleted successfully, False on error.
        """
        self._ensure_session()
        if self._session is None:
            logger.warning("S3 client is not initialized")
            return False

        try:
            async with self._session.client("s3", **self._get_client_config()) as s3:
                await s3.delete_object(Bucket=bucket, Key=key)
            logger.debug("Deleted file from s3://%s/%s", bucket, key)
            return True
        except (AttributeError, TypeError) as exc:
            logger.error("Invalid bucket or key format: %s", exc)
            return False
        except Exception as exc:
            logger.error("Failed to delete file from s3://%s/%s: %s", bucket, key, exc)
            return False

    def get_file_url(self, bucket: str, key: str) -> str:
        """
        Get public URL for a file.

        Args:
            bucket: Bucket name.
            key: Object key.

        Returns:
            Public URL for the file.
        """
        if not settings.S3_ENDPOINT_URL:
            return f"{settings.FILE_BASE_URL}/{bucket}/{key}"

        base = settings.S3_ENDPOINT_URL.rstrip("/")
        if base.startswith("http"):
            return f"{base}/{bucket}/{key}"

        scheme = "https" if settings.S3_USE_SSL else "http"
        return f"{scheme}://{base}/{bucket}/{key}"

    async def update_file(
        self,
        file_obj: BinaryIO,
        bucket: str,
        key: str,
        content_type: str,
    ) -> str:
        """
        Update existing file in S3.

        Args:
            file_obj: New file content.
            bucket: Bucket name.
            key: Object key.
            content_type: MIME type of the file.

        Returns:
            URL of updated file.
        """
        return await self.upload_file(file_obj, bucket, key, content_type)


def infer_bucket(file_type: str) -> str:
    """
    Determine bucket name based on file type.

    Args:
        file_type: Type of file (avatar, project_logo, etc.).

    Returns:
        Bucket name for the file type.
    """
    mapping = {
        "avatar": settings.S3_BUCKET_AVATARS,
        "project_logo": settings.S3_BUCKET_PROJECT_LOGOS,
        "task_logo": settings.S3_BUCKET_TASK_LOGOS,
        "task_attachment": settings.S3_BUCKET_TASK_ATTACHMENTS,
    }
    return mapping.get(file_type, settings.S3_BUCKET_TASK_ATTACHMENTS)


s3_service = S3Service()
