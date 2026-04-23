import asyncio
import base64
import json
import logging
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Callable, Optional
from uuid import uuid4

from fastapi import HTTPException, status
from sqlalchemy import func, select

from app.backend.db import get_db_session
from app.core.constants import FileAction
from app.core.exceptions import S3ServiceError
from app.core.permissions import (
    check_file_permission,
    check_list_files_access,
    validate_content_type,
    validate_entity_exists,
    validate_entity_type,
    validate_file_magic_bytes,
    validate_file_size,
    validate_file_type,
)
from app.core.security import UserPrincipal, decode_token
from app.models.file import FileMetadata
from app.services.core_client import CoreServiceClient
from app.services.kafka_service import kafka_service
from app.services.s3_service import infer_bucket, s3_service
from app.settings import settings

logger = logging.getLogger(__name__)


class KafkaRequestConsumer:
    """Consumer for processing file operation requests from Kafka."""

    def __init__(self) -> None:
        """Initialize Kafka request consumer."""
        self._consumer: Optional[Any] = None
        self._consumer_cls: Optional[type] = None
        self._running = False
        self._consume_task: Optional[asyncio.Task] = None
        self._core_client: Optional[CoreServiceClient] = None

    def _load_consumer_cls(self) -> None:
        """Load AIOKafkaConsumer class if available."""
        if self._consumer_cls is not None:
            return
        try:
            from aiokafka import AIOKafkaConsumer

            self._consumer_cls = AIOKafkaConsumer
        except ModuleNotFoundError:
            logger.warning("aiokafka not installed, Kafka consumer disabled")
            self._consumer_cls = None

    async def start(self) -> None:
        """Start the Kafka consumer."""
        if self._running:
            logger.debug("Kafka consumer already running")
            return

        if not settings.KAFKA_BOOTSTRAP_SERVERS:
            logger.warning(
                "KAFKA_BOOTSTRAP_SERVERS not configured, Kafka consumer disabled"
            )
            return

        self._load_consumer_cls()
        if not self._consumer_cls:
            logger.warning("aiokafka not available, Kafka consumer disabled")
            return

        # Initialize core service client for dependency injection
        try:
            self._core_client = CoreServiceClient()
        except Exception as exc:
            logger.warning(
                "Failed to initialize core service client: %s. "
                "Permission checks will use fallback initialization.",
                exc,
            )

        try:
            topics = [
                settings.KAFKA_TOPIC_FILE_UPLOAD_REQUEST,
                settings.KAFKA_TOPIC_FILE_DELETE_REQUEST,
                settings.KAFKA_TOPIC_FILE_LIST_REQUEST,
            ]
            logger.info(
                "Starting Kafka consumer: bootstrap_servers=%s, topics=%s",
                settings.KAFKA_BOOTSTRAP_SERVERS,
                topics,
            )
            self._consumer = self._consumer_cls(
                *topics,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="todo-files-request-consumer",
            )
            await self._consumer.start()
            self._running = True
            self._consume_task = asyncio.create_task(self._consume_loop())
            logger.info(
                "Kafka request consumer started successfully: topics=%s",
                topics,
            )
        except Exception as exc:
            logger.error(
                "Failed to start Kafka request consumer: %s", exc, exc_info=True
            )

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        self._running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as exc:
                logger.error("Error stopping Kafka consumer: %s", exc)
            self._consumer = None

        logger.info("Kafka request consumer stopped")

    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        if not self._consumer:
            return

        try:
            async for message in self._consumer:
                if not self._running:
                    break
                try:
                    await self._handle_message(message.topic, message.value)
                except Exception as exc:
                    logger.error("Error handling Kafka message: %s", exc, exc_info=True)
        except asyncio.CancelledError:
            logger.info("Kafka consume loop cancelled")
        except Exception as exc:
            logger.error("Error in Kafka consume loop: %s", exc, exc_info=True)
            self._running = False

    async def _handle_message(self, topic: str, payload: dict) -> None:
        """
        Handle incoming Kafka message.

        Args:
            topic: Kafka topic name.
            payload: Message payload.
        """
        logger.info(
            "Received Kafka message: topic=%s, event=%s", topic, payload.get("event")
        )
        if topic == settings.KAFKA_TOPIC_FILE_UPLOAD_REQUEST:
            await self._handle_upload_request(payload)
        elif topic == settings.KAFKA_TOPIC_FILE_DELETE_REQUEST:
            await self._handle_delete_request(payload)
        elif topic == settings.KAFKA_TOPIC_FILE_LIST_REQUEST:
            await self._handle_list_request(payload)

    async def _resolve_request_user(
        self,
        payload: dict[str, Any],
        request_id: Optional[str],
        error_callback: Callable[[Optional[str], str], Any],
    ) -> Optional[UserPrincipal]:
        """
        Resolve request principal for Kafka-initiated actions.

        Args:
            payload: Raw Kafka payload.
            request_id: Request ID for error reporting.
            error_callback: Async function to call on error: (request_id, message).

        Returns:
            UserPrincipal if credentials are valid, None otherwise.
        """
        token = payload.get("token")
        user_id_raw = payload.get("user_id")
        email = payload.get("email")
        roles_raw = payload.get("roles") or payload.get("role") or []

        if isinstance(roles_raw, str):
            roles = [roles_raw]
        else:
            roles = list(roles_raw) if isinstance(roles_raw, list) else []

        user_id: Optional[int] = None
        if user_id_raw is not None:
            try:
                user_id = self._coerce_int_field(user_id_raw, "user_id")
            except HTTPException as exc:
                logger.warning(
                    "Invalid request user_id: request_id=%s, detail=%s",
                    request_id,
                    exc.detail,
                )
                await error_callback(request_id, str(exc.detail))
                return None
            if user_id <= 0:
                logger.warning(
                    "Invalid request user_id <= 0: request_id=%s, user_id=%s",
                    request_id,
                    user_id,
                )
                await error_callback(request_id, "user_id must be greater than 0")
                return None

        if token:
            internal_token = settings.INTERNAL_API_TOKEN
            if internal_token and token == internal_token:
                if user_id is None:
                    logger.warning(
                        "Invalid internal request: missing user_id, request_id=%s",
                        request_id,
                    )
                    await error_callback(
                        request_id,
                        "user_id is required for internal service requests",
                    )
                    return None
                return UserPrincipal(
                    user_id=user_id,
                    roles=roles,
                    email=email,
                    is_internal_service=True,
                )

            try:
                user = decode_token(token)
            except Exception as exc:
                logger.warning("Invalid token in request: %s", exc)
                await error_callback(request_id, "Invalid authentication token")
                return None

            if user_id is not None and user.id != user_id:
                logger.warning(
                    "JWT subject mismatch: request_id=%s, token_user_id=%s, "
                    "payload_user_id=%s",
                    request_id,
                    user.id,
                    user_id,
                )
                await error_callback(
                    request_id, "Token subject does not match payload user_id"
                )
                return None
            return user

        if user_id is None:
            logger.warning(
                "Invalid request: missing token and user_id, request_id=%s",
                request_id,
            )
            await error_callback(request_id, "Missing authentication credentials")
            return None

        return UserPrincipal(user_id=user_id, roles=roles, email=email)

    def _coerce_int_field(self, value: Any, field_name: str) -> int:
        """Coerce Kafka payload field to int with strict validation."""
        if isinstance(value, bool):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{field_name} must be an integer",
            )
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{field_name} must be an integer",
                )
            try:
                return int(stripped)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{field_name} must be an integer",
                ) from exc

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field_name} must be an integer",
        )

    async def _process_upload_file(
        self,
        request_id: str,
        file_data_b64: str,
        file_name: str,
        content_type: str,
        file_type: str,
        entity_type: str,
        entity_id: int,
        user_id: int,
    ) -> Optional[FileMetadata]:
        """Process file upload to S3 and database."""
        try:
            file_content = base64.b64decode(file_data_b64)
        except Exception as exc:
            logger.error("Failed to decode base64 file data: %s", exc)
            await self._send_upload_error(request_id, "Invalid file data encoding")
            return None

        file_size = len(file_content)
        validate_file_size(file_size, settings.MAX_FILE_SIZE)
        validate_file_magic_bytes(file_content[:16], content_type)

        bucket = infer_bucket(file_type)
        key = f"{file_type}/{entity_type}/{entity_id}/{uuid4().hex}_{file_name}"

        file_obj = BytesIO(file_content)

        try:
            url = await s3_service.upload_file(
                file_obj,
                bucket=bucket,
                key=key,
                content_type=content_type,
            )
        except S3ServiceError as exc:
            logger.error("Failed to upload file to S3: %s", exc)
            await self._send_upload_error(request_id, "File storage unavailable")
            return None

        async with get_db_session() as db:
            meta = FileMetadata(
                file_key=key,
                file_type=file_type,
                entity_type=entity_type,
                entity_id=entity_id,
                uploader_id=user_id,
                original_filename=file_name,
                content_type=content_type,
                file_size=file_size,
                bucket_name=bucket,
                url=url,
            )
            db.add(meta)
            await db.commit()
            await db.refresh(meta)

        return meta

    async def _handle_upload_request(self, payload: dict) -> None:
        """
        Handle file upload request.

        Args:
            payload: Request payload with file data and metadata.
        """
        request_id = payload.get("request_id")
        logger.info("Processing upload request: request_id=%s", request_id)
        try:
            user = await self._resolve_request_user(
                payload,
                request_id,
                self._send_upload_error,
            )
            if not user:
                logger.warning(
                    "Token validation failed for upload request: request_id=%s",
                    request_id,
                )
                return

            file_data_b64 = payload.get("file_data")
            file_name = payload.get("file_name", "unknown")
            content_type = payload.get("content_type", "application/octet-stream")
            file_type = payload.get("file_type")
            entity_type = payload.get("entity_type")
            entity_id_raw = payload.get("entity_id")
            user_id_raw = payload.get("user_id")

            if not all(
                [request_id, file_data_b64, file_type, entity_type, entity_id_raw]
            ):
                logger.warning("Invalid upload request: missing required fields")
                await self._send_upload_error(request_id, "Missing required fields")
                return

            entity_id = self._coerce_int_field(entity_id_raw, "entity_id")
            user_id = (
                self._coerce_int_field(user_id_raw, "user_id")
                if user_id_raw is not None
                else user.id
            )

            # Type narrowing after validation
            assert file_type is not None
            assert entity_type is not None
            assert file_data_b64 is not None

            validate_file_type(file_type)
            validate_entity_type(entity_type)

            try:
                await validate_entity_exists(
                    entity_type, entity_id, user, core_client=self._core_client
                )
            except HTTPException as exc:
                error_detail = str(exc.detail)
                logger.warning(
                    "Entity validation failed: request_id=%s, "
                    "entity_type=%s, entity_id=%s, status=%s, error=%s",
                    request_id,
                    entity_type,
                    entity_id,
                    exc.status_code,
                    error_detail,
                )
                await self._send_upload_error(request_id, error_detail)
                return
            except Exception as exc:
                logger.error(
                    "Unexpected error during entity validation: "
                    "request_id=%s, entity_type=%s, entity_id=%s, error=%s",
                    request_id,
                    entity_type,
                    entity_id,
                    exc,
                    exc_info=True,
                )
                await self._send_upload_error(
                    request_id, "Internal error during validation"
                )
                return

            validate_content_type(content_type, settings.ALLOWED_IMAGE_TYPES)

            # Type narrowing after validation
            assert request_id is not None

            meta = await self._process_upload_file(
                request_id,
                file_data_b64,
                file_name,
                content_type,
                file_type,
                entity_type,
                entity_id,
                user_id,
            )
            if not meta:
                logger.warning(
                    "File upload processing failed: request_id=%s", request_id
                )
                return

            logger.info(
                "File uploaded successfully: request_id=%s, file_id=%s, url=%s",
                request_id,
                meta.id,
                meta.url,
            )

            await kafka_service.send_file_uploaded(
                {
                    "event": "file.uploaded",
                    "file_id": meta.id,
                    "file_type": file_type,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "url": meta.url,
                    "timestamp": (
                        meta.created_at.isoformat() if meta.created_at else None
                    ),
                    "user_id": user_id,
                    "request_id": request_id,
                }
            )
            await self._send_upload_success(
                request_id=request_id,
                file_id=meta.id,
                file_type=file_type,
                entity_type=entity_type,
                entity_id=entity_id,
                url=meta.url,
                timestamp=meta.created_at.isoformat() if meta.created_at else None,
                user_id=user_id,
            )
            logger.debug(
                "File uploaded via Kafka: file_id=%s, request_id=%s",
                meta.id,
                request_id,
            )
        except HTTPException as exc:
            logger.warning(
                "Upload request validation failed: request_id=%s, detail=%s",
                request_id,
                exc.detail,
            )
            await self._send_upload_error(request_id, str(exc.detail))
        except Exception as exc:
            logger.error("Error processing upload request: %s", exc, exc_info=True)
            await self._send_upload_error(request_id, "Internal error during upload")

    async def _handle_delete_request(self, payload: dict) -> None:
        """
        Handle file delete request.

        Args:
            payload: Request payload with file_id.
        """
        request_id = payload.get("request_id")
        try:
            user = await self._resolve_request_user(
                payload, request_id, self._send_delete_error
            )
            if not user:
                return

            file_id_raw = payload.get("file_id")
            if not isinstance(request_id, str) or not request_id or file_id_raw is None:
                logger.warning("Invalid delete request: missing required fields")
                await self._send_delete_error(request_id, "Missing required fields")
                return
            file_id = self._coerce_int_field(file_id_raw, "file_id")

            async with get_db_session() as db:
                res = await db.execute(
                    select(FileMetadata).where(
                        FileMetadata.id == file_id,
                        FileMetadata.deleted_at.is_(None),
                    )
                )
                meta = res.scalar_one_or_none()
                if not meta:
                    logger.warning("File not found for deletion: file_id=%s", file_id)
                    await self._send_delete_error(request_id, "File not found")
                    return

                try:
                    await check_file_permission(
                        user, meta, FileAction.DELETE, core_client=self._core_client
                    )
                except HTTPException as exc:
                    logger.warning(
                        "Permission denied for file deletion: request_id=%s, detail=%s",
                        request_id,
                        exc.detail,
                    )
                    await self._send_delete_error(request_id, str(exc.detail))
                    return
                except Exception as exc:
                    logger.warning("Permission denied for file deletion: %s", exc)
                    await self._send_delete_error(request_id, "Permission denied")
                    return

                try:
                    await s3_service.delete_file(
                        bucket=meta.bucket_name, key=meta.file_key
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to delete file from S3 (bucket=%s, key=%s): %s",
                        meta.bucket_name,
                        meta.file_key,
                        exc,
                    )

                deleted_at = datetime.now(timezone.utc)
                meta.deleted_at = deleted_at
                await db.commit()

            await kafka_service.send_file_deleted(
                {
                    "event": "file.deleted",
                    "file_id": file_id,
                    "file_type": meta.file_type,
                    "entity_type": meta.entity_type,
                    "entity_id": meta.entity_id,
                    "timestamp": deleted_at.isoformat(),
                    "user_id": user.id,
                    "request_id": request_id,
                }
            )
            await self._send_delete_success(
                request_id=request_id,
                file_id=file_id,
                file_type=meta.file_type,
                entity_type=meta.entity_type,
                entity_id=meta.entity_id,
                timestamp=deleted_at.isoformat(),
                user_id=user.id,
            )
            logger.debug(
                "File deleted via Kafka: file_id=%s, request_id=%s",
                file_id,
                request_id,
            )
        except Exception as exc:
            logger.error("Error processing delete request: %s", exc, exc_info=True)
            await self._send_delete_error(request_id, "Internal error during deletion")

    async def _build_list_query(
        self,
        db: Any,
        entity_type: Optional[str],
        entity_id: Optional[int],
        file_type: Optional[str],
    ) -> tuple[Any, int]:
        """Build and execute list query."""
        query = select(FileMetadata).where(FileMetadata.deleted_at.is_(None))

        if entity_type:
            query = query.where(FileMetadata.entity_type == entity_type)
        if entity_id is not None:
            query = query.where(FileMetadata.entity_id == entity_id)
        if file_type:
            query = query.where(FileMetadata.file_type == file_type)

        count_query = select(func.count()).select_from(query.subquery())
        total_res = await db.execute(count_query)
        total = total_res.scalar() or 0

        return query, total

    async def _handle_list_request(self, payload: dict) -> None:
        """
        Handle file list request.

        Args:
            payload: Request payload with filters.
        """
        request_id = payload.get("request_id")
        try:
            if not request_id:
                logger.warning("Invalid list request: missing request_id")
                await self._send_list_error(request_id, "Missing request_id")
                return

            user = await self._resolve_request_user(
                payload, request_id, self._send_list_error
            )
            if not user:
                return

            entity_type = payload.get("entity_type")
            entity_id_raw = payload.get("entity_id")
            file_type = payload.get("file_type")
            page_raw = payload.get("page", 1)
            page_size_raw = payload.get("page_size", 20)
            entity_id = (
                self._coerce_int_field(entity_id_raw, "entity_id")
                if entity_id_raw is not None
                else None
            )

            try:
                page = int(page_raw)
                page_size = int(page_size_raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "Invalid pagination values: page and page_size must be "
                        "integers"
                    ),
                ) from exc

            if page < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="page must be greater than or equal to 1",
                )

            if page_size < 1 or page_size > 100:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="page_size must be between 1 and 100",
                )

            if entity_type:
                validate_entity_type(entity_type)
            if file_type:
                validate_file_type(file_type)

            if self._core_client is not None:
                await check_list_files_access(
                    user,
                    entity_type,
                    entity_id,
                    core_client=self._core_client,
                )
            else:
                await check_list_files_access(user, entity_type, entity_id)

            async with get_db_session() as db:
                query, total = await self._build_list_query(
                    db, entity_type, entity_id, file_type
                )

                offset = (page - 1) * page_size
                query = (
                    query.order_by(FileMetadata.created_at.desc())
                    .offset(offset)
                    .limit(page_size)
                )

                result = await db.execute(query)
                files = list(result.scalars().all())

            files_data = [
                {
                    "id": f.id,
                    "file_type": f.file_type,
                    "entity_type": f.entity_type,
                    "entity_id": f.entity_id,
                    "original_filename": f.original_filename,
                    "content_type": f.content_type,
                    "file_size": f.file_size,
                    "url": f.url,
                    "created_at": f.created_at.isoformat() if f.created_at else None,
                }
                for f in files
            ]

            await kafka_service._send(
                settings.KAFKA_TOPIC_FILE_LIST_RESPONSE,
                {
                    "event": "file.list.response",
                    "status": "ok",
                    "request_id": request_id,
                    "files": files_data,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                },
            )
            logger.debug(
                "File list response sent: request_id=%s, count=%s",
                request_id,
                len(files_data),
            )
        except HTTPException as exc:
            logger.warning(
                "List request validation failed: request_id=%s, detail=%s",
                request_id,
                exc.detail,
            )
            await self._send_list_error(request_id, str(exc.detail))
        except Exception as exc:
            logger.error("Error processing list request: %s", exc, exc_info=True)
            await self._send_list_error(request_id, "Internal error during list")

    async def _send_upload_success(
        self,
        request_id: str,
        file_id: int,
        file_type: str,
        entity_type: str,
        entity_id: int,
        url: str,
        timestamp: Optional[str],
        user_id: int,
    ) -> None:
        """Send upload success response to Kafka."""
        response_payload = {
            "event": "file.upload.response",
            "status": "ok",
            "request_id": request_id,
            "file_id": file_id,
            "file_type": file_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "url": url,
            "timestamp": timestamp,
            "user_id": user_id,
        }
        logger.debug(
            "Sending upload success response: request_id=%s, topic=%s",
            request_id,
            settings.KAFKA_TOPIC_FILE_UPLOAD_RESPONSE,
        )
        success = await kafka_service._send(
            settings.KAFKA_TOPIC_FILE_UPLOAD_RESPONSE,
            response_payload,
        )
        if success:
            logger.info(
                "Upload success response sent successfully: request_id=%s", request_id
            )
        else:
            logger.error(
                "Failed to send upload success response: request_id=%s", request_id
            )

    async def _send_upload_error(self, request_id: Optional[str], detail: str) -> None:
        """Send upload error response to Kafka."""
        await kafka_service._send(
            settings.KAFKA_TOPIC_FILE_UPLOAD_RESPONSE,
            {
                "event": "file.upload.response",
                "status": "error",
                "request_id": request_id,
                "detail": detail,
            },
        )

    async def _send_delete_success(
        self,
        request_id: str,
        file_id: int,
        file_type: str,
        entity_type: str,
        entity_id: int,
        timestamp: str,
        user_id: int,
    ) -> None:
        """Send delete success response to Kafka."""
        await kafka_service._send(
            settings.KAFKA_TOPIC_FILE_DELETE_RESPONSE,
            {
                "event": "file.delete.response",
                "status": "ok",
                "request_id": request_id,
                "file_id": file_id,
                "file_type": file_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "timestamp": timestamp,
                "user_id": user_id,
            },
        )

    async def _send_delete_error(self, request_id: Optional[str], detail: str) -> None:
        """Send delete error response to Kafka."""
        await kafka_service._send(
            settings.KAFKA_TOPIC_FILE_DELETE_RESPONSE,
            {
                "event": "file.delete.response",
                "status": "error",
                "request_id": request_id,
                "detail": detail,
            },
        )

    async def _send_list_error(self, request_id: Optional[str], detail: str) -> None:
        """Send list error response to Kafka."""
        await kafka_service._send(
            settings.KAFKA_TOPIC_FILE_LIST_RESPONSE,
            {
                "event": "file.list.response",
                "status": "error",
                "request_id": request_id,
                "detail": detail,
            },
        )


kafka_request_consumer = KafkaRequestConsumer()
