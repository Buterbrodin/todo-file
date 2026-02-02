from typing import Literal

from fastapi import HTTPException, status

from app.core.security import UserPrincipal
from app.models.file import FileMetadata


VALID_FILE_TYPES = {"avatar", "project_logo", "task_logo", "task_attachment"}
VALID_ENTITY_TYPES = {"user", "project", "task"}


async def check_file_permission(
    user: UserPrincipal,
    file_meta: FileMetadata,
    action: Literal["read", "write", "delete"] = "read",
) -> bool:
    """
    Check if user has permission to perform action on file.

    Args:
        user: Current authenticated user.
        file_meta: File metadata object.
        action: Action to perform (read, write, delete).

    Returns:
        True if permission granted.

    Raises:
        HTTPException: If permission denied.
    """
    if user.is_admin:
        return True

    if file_meta.entity_type == "user":
        if file_meta.entity_id == user.id:
            return True
        if action != "read":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only manage your own avatar",
            )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You cannot view other users' avatars",
        )

    if file_meta.entity_type in ("project", "task"):
        return True

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access denied",
    )


def validate_file_type(file_type: str) -> None:
    """
    Validate that file_type is one of allowed values.

    Args:
        file_type: File type to validate.

    Raises:
        HTTPException: If file_type is invalid.
    """
    if file_type not in VALID_FILE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file_type. Allowed: {', '.join(sorted(VALID_FILE_TYPES))}",
        )


def validate_entity_type(entity_type: str) -> None:
    """
    Validate that entity_type is one of allowed values.

    Args:
        entity_type: Entity type to validate.

    Raises:
        HTTPException: If entity_type is invalid.
    """
    if entity_type not in VALID_ENTITY_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid entity_type. Allowed: {', '.join(sorted(VALID_ENTITY_TYPES))}",
        )


async def validate_entity_exists(
    entity_type: str,
    entity_id: int,
    user: UserPrincipal,
) -> None:
    """
    Validate that entity exists and user has permission to upload files for it.

    Args:
        entity_type: Type of entity (user, project, task).
        entity_id: ID of the entity.
        user: Current authenticated user.

    Raises:
        HTTPException: If validation fails.
    """
    if entity_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid entity_id",
        )

    if entity_type == "user":
        if entity_id != user.id and not user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only upload files for yourself",
            )


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


async def validate_file_magic_bytes(
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
    for magic, mime_type in MAGIC_BYTES.items():
        if file_content.startswith(magic):
            detected_type = mime_type
            break

    if detected_type is None:
        if declared_content_type == "image/webp" and b"WEBP" in file_content[:12]:
            detected_type = "image/webp"

    if detected_type is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File content doesn't match allowed image types",
        )

    if declared_content_type and detected_type != declared_content_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File content type mismatch. Detected: {detected_type}, declared: {declared_content_type}",
        )
