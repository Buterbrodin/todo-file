import logging
from typing import Literal, Optional

from fastapi import HTTPException, status

from app.core.deps import get_core_service_client
from app.core.exceptions import CoreServiceError
from app.core.security import UserPrincipal
from app.models.file import EntityType, FileMetadata, FileType
from app.services.core_client import CoreServiceClient

logger = logging.getLogger(__name__)


async def check_file_permission(
    user: UserPrincipal,
    file_meta: FileMetadata,
    action: Literal["read", "write", "delete"] = "read",
    core_client: Optional[CoreServiceClient] = None,
) -> None:
    """
    Assert that user has permission to perform action on file.

    Args:
        user: Current authenticated user.
        file_meta: File metadata object.
        action: Action to perform (read, write, delete).
        core_client: Optional core service client (for dependency injection).
            If not provided, will be obtained via get_core_service_client().

    Raises:
        HTTPException: If permission denied.
    """
    if user.is_admin:
        return

    if file_meta.entity_type == EntityType.user:
        if file_meta.entity_id == user.id:
            return
        if action != "read":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only manage your own avatar",
            )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You cannot view other users' avatars",
        )

    if file_meta.entity_type == EntityType.project:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_project_access(
                user.id, file_meta.entity_id, action, user.email
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to project",
                )
            return
        except CoreServiceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc

    if file_meta.entity_type == EntityType.task:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_task_access(
                user.id, file_meta.entity_id, action, user.email
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to task",
                )
            return
        except CoreServiceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access denied",
    )


async def check_list_files_access(
    user: UserPrincipal,
    entity_type: Optional[str],
    entity_id: Optional[int],
    core_client: Optional[CoreServiceClient] = None,
) -> None:
    """
    Check if user has access to list files for the given entity.

    Args:
        user: Current authenticated user.
        entity_type: Type of entity.
        entity_id: ID of the entity.
        core_client: Optional core service client (for dependency injection).
            If not provided, will be obtained via get_core_service_client().

    Raises:
        HTTPException: If access is denied.
    """
    if user.is_admin:
        return

    if not entity_type or entity_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="entity_type and entity_id are required for non-admin listing",
        )

    if entity_type == EntityType.user:
        if entity_id != user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to user files",
            )
    elif entity_type == EntityType.project:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_project_access(
                user.id, entity_id, "read", user.email
            )
        except CoreServiceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc
        if not has_access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to project",
            )
    elif entity_type == EntityType.task:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_task_access(
                user.id, entity_id, "read", user.email
            )
        except CoreServiceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc
        if not has_access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to task",
            )


def validate_file_type(file_type: str) -> None:
    """
    Validate that file_type is one of allowed values.

    Args:
        file_type: File type to validate.

    Raises:
        HTTPException: If file_type is invalid.
    """
    if file_type not in {e.value for e in FileType}:
        allowed = ", ".join(sorted(e.value for e in FileType))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file_type. Allowed: {allowed}",
        )


def validate_entity_type(entity_type: str) -> None:
    """
    Validate that entity_type is one of allowed values.

    Args:
        entity_type: Entity type to validate.

    Raises:
        HTTPException: If entity_type is invalid.
    """
    if entity_type not in {e.value for e in EntityType}:
        allowed = ", ".join(sorted(e.value for e in EntityType))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid entity_type. Allowed: {allowed}",
        )


async def validate_entity_exists(
    entity_type: str,
    entity_id: int,
    user: UserPrincipal,
    core_client: Optional[CoreServiceClient] = None,
) -> None:
    """
    Validate that entity exists and user has permission to upload files for it.

    Args:
        entity_type: Type of entity (user, project, task).
        entity_id: ID of the entity.
        user: Current authenticated user.
        core_client: Optional core service client (for dependency injection).
            If not provided, will be obtained via get_core_service_client().

    Raises:
        HTTPException: If validation fails.
    """
    if entity_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid entity_id",
        )

    if entity_type == EntityType.user:
        if entity_id != user.id and not user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only upload files for yourself",
            )

    if entity_type == EntityType.project:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_project_access(
                user.id, entity_id, "write", user.email
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to project",
                )
        except CoreServiceError as exc:
            logger.warning("Core service project access check failed: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc

    if entity_type == EntityType.task:
        try:
            if core_client is None:
                core_client = get_core_service_client()
            has_access = await core_client.check_task_access(
                user.id, entity_id, "write", user.email
            )
            if not has_access:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied to task",
                )
        except CoreServiceError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Core service unavailable",
            ) from exc


def validate_content_type(content_type: str | None, allowed_types: list[str]) -> None:
    """
    Validate file content type.

    Args:
        content_type: MIME type of the file.
        allowed_types: List of allowed MIME types.

    Raises:
        HTTPException: If content type is not allowed.
    """
    if not content_type or content_type not in allowed_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid content type. Allowed: {', '.join(allowed_types)}",
        )


def validate_file_size(file_size: int, max_size: int) -> None:
    """
    Validate file size.

    Args:
        file_size: Size of file in bytes.
        max_size: Maximum allowed size in bytes.

    Raises:
        HTTPException: If file is too large.
    """
    if file_size > max_size:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File too large. Maximum size: {max_size} bytes",
        )


MAGIC_BYTES = {
    b"\xff\xd8\xff": "image/jpeg",
    b"\x89PNG\r\n\x1a\n": "image/png",
    b"RIFF": "image/webp",
}


def _detect_webp(file_content: bytes) -> bool:
    """
    Detect if file is valid WebP.

    WebP format:
    - Bytes 0-3: "RIFF"
    - Bytes 8-11: "WEBP"

    Args:
        file_content: File content bytes.

    Returns:
        True if valid WebP, False otherwise.
    """
    if len(file_content) < 12:
        return False
    return file_content.startswith(b"RIFF") and file_content[8:12] == b"WEBP"


def validate_file_magic_bytes(
    file_content: bytes,
    declared_content_type: str | None,
) -> None:
    """
    Validate file content by checking magic bytes.

    Args:
        file_content: First bytes of the file.
        declared_content_type: Content type declared by client.

    Raises:
        HTTPException: If file content doesn't match declared type.
    """
    if not file_content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty file",
        )

    detected_type = None

    # Check standard magic bytes
    for magic, mime_type in MAGIC_BYTES.items():
        if file_content.startswith(magic):
            # For RIFF, we need to check if it's actually WebP
            if mime_type == "image/webp":
                if _detect_webp(file_content):
                    detected_type = "image/webp"
            else:
                detected_type = mime_type
            break

    # Double-check WebP format if declared
    if detected_type is None and declared_content_type == "image/webp":
        if _detect_webp(file_content):
            detected_type = "image/webp"

    if detected_type is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File content doesn't match allowed image types",
        )

    if declared_content_type and detected_type != declared_content_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Content type mismatch: {detected_type} vs {declared_content_type}",
        )
