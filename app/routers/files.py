import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import UserPrincipal, get_current_user, get_db_session
from app.core.exceptions import CoreServiceError, S3ServiceError
from app.core.permissions import (
    check_file_permission,
    validate_content_type,
    validate_entity_exists,
    validate_entity_type,
    validate_file_magic_bytes,
    validate_file_size,
    validate_file_type,
)
from app.models.file import FileMetadata
from app.schemas.file import FileListResponse, FileResponse
from app.services.core_client import CoreServiceClient
from app.services.kafka_service import kafka_service
from app.services.s3_service import S3Service, infer_bucket, s3_service
from app.settings import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/files", tags=["files"])


def get_s3_service() -> S3Service:
    """Dependency for S3 service."""
    return s3_service


async def check_list_files_access(
    user: UserPrincipal,
    entity_type: Optional[str],
    entity_id: Optional[int],
) -> None:
    """
    Check if user has access to list files for the given entity.

    Args:
        user: Current authenticated user.
        entity_type: Type of entity.
        entity_id: ID of the entity.

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

    if entity_type == "user":
        if entity_id != user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to user files",
            )
    elif entity_type == "project":
        try:
            has_access = await CoreServiceClient().check_project_access(
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
    elif entity_type == "task":
        try:
            has_access = await CoreServiceClient().check_task_access(
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


async def get_file_size(file: UploadFile) -> int:
    """
    Get file size from UploadFile.

    Args:
        file: Uploaded file object.

    Returns:
        File size in bytes.
    """
    if file.size is not None:
        return file.size

    if hasattr(file.file, "seek") and hasattr(file.file, "tell"):
        current_pos = file.file.tell()
        file.file.seek(0, 2)
        size = file.file.tell()
        file.file.seek(current_pos)
        return size

    return 0


@router.post(
    "/upload", response_model=FileResponse, status_code=status.HTTP_201_CREATED
)
async def upload_file(
    file: UploadFile = File(...),
    file_type: str = Form("task_attachment"),
    entity_type: str = Form("task"),
    entity_id: int = Form(0),
    db: AsyncSession = Depends(get_db_session),
    user: UserPrincipal = Depends(get_current_user),
    s3_service: S3Service = Depends(get_s3_service),
) -> FileMetadata:
    """
    Upload a new file.

    Args:
        file: File to upload.
        file_type: Type of file (avatar, project_logo, task_logo, task_attachment).
        entity_type: Type of entity (user, project, task).
        entity_id: ID of the entity.
        db: Database session.
        user: Current authenticated user.
        s3_service: S3 service instance.

    Returns:
        Created file metadata.
    """
    validate_file_type(file_type)
    validate_entity_type(entity_type)
    await validate_entity_exists(entity_type, entity_id, user)

    validate_content_type(file.content_type, settings.ALLOWED_IMAGE_TYPES)

    file_size = await get_file_size(file)
    validate_file_size(file_size, settings.MAX_FILE_SIZE)

    file_content = await file.read()
    await file.seek(0)

    await validate_file_magic_bytes(file_content[:16], file.content_type)

    bucket = infer_bucket(file_type)
    key = f"{file_type}/{entity_type}/{entity_id}/{uuid4().hex}_{file.filename}"

    content_type = file.content_type or "application/octet-stream"

    try:
        url = await s3_service.upload_file(
            file.file,
            bucket=bucket,
            key=key,
            content_type=content_type,
        )
    except S3ServiceError as exc:
        logger.error("Failed to upload file to S3: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="File storage unavailable",
        ) from exc

    meta = FileMetadata(
        file_key=key,
        file_type=file_type,
        entity_type=entity_type,
        entity_id=entity_id,
        original_filename=file.filename or "unknown",
        content_type=content_type,
        file_size=len(file_content),
        bucket_name=bucket,
        url=url,
    )
    db.add(meta)
    await db.commit()
    await db.refresh(meta)

    await kafka_service.send_file_uploaded(
        {
            "event": "file.uploaded",
            "file_id": meta.id,
            "file_type": file_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "url": url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.id,
        }
    )
    return meta


@router.get("/{file_id}", response_model=FileResponse)
async def get_file(
    file_id: int,
    db: AsyncSession = Depends(get_db_session),
    user: UserPrincipal = Depends(get_current_user),
) -> FileMetadata:
    """
    Get file metadata by ID.

    Args:
        file_id: File ID.
        db: Database session.
        user: Current authenticated user.

    Returns:
        File metadata.
    """
    res = await db.execute(
        select(FileMetadata).where(
            FileMetadata.id == file_id, FileMetadata.deleted_at.is_(None)
        )
    )
    meta = res.scalar_one_or_none()
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    await check_file_permission(user, meta, "read")
    return meta


@router.get("/", response_model=FileListResponse)
async def list_files(
    entity_type: Optional[str] = Query(None),
    entity_id: Optional[int] = Query(None),
    file_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db_session),
    user: UserPrincipal = Depends(get_current_user),
) -> FileListResponse:
    """
    List files with optional filters.

    Args:
        entity_type: Filter by entity type.
        entity_id: Filter by entity ID.
        file_type: Filter by file type.
        page: Page number.
        page_size: Items per page.
        db: Database session.
        user: Current authenticated user.

    Returns:
        Paginated list of files.
    """
    stmt = select(FileMetadata).where(FileMetadata.deleted_at.is_(None))

    if entity_type:
        validate_entity_type(entity_type)
    if file_type:
        validate_file_type(file_type)

    await check_list_files_access(user, entity_type, entity_id)

    if entity_type:
        stmt = stmt.where(FileMetadata.entity_type == entity_type)
    if entity_id is not None:
        stmt = stmt.where(FileMetadata.entity_id == entity_id)
    if file_type:
        stmt = stmt.where(FileMetadata.file_type == file_type)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_res = await db.execute(count_stmt)
    total = total_res.scalar_one()

    stmt = stmt.order_by(FileMetadata.created_at.desc())
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)

    res = await db.execute(stmt)
    files = res.scalars().all()

    return FileListResponse(
        files=list(files), total=total, page=page, page_size=page_size
    )


@router.put("/{file_id}", response_model=FileResponse, status_code=status.HTTP_200_OK)
async def update_file(
    file_id: int,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db_session),
    user: UserPrincipal = Depends(get_current_user),
    s3_service: S3Service = Depends(get_s3_service),
) -> FileMetadata:
    """
    Update an existing file.

    Args:
        file_id: File ID to update.
        file: New file content.
        db: Database session.
        user: Current authenticated user.
        s3_service: S3 service instance.

    Returns:
        Updated file metadata.
    """
    res = await db.execute(
        select(FileMetadata).where(
            FileMetadata.id == file_id, FileMetadata.deleted_at.is_(None)
        )
    )
    meta = res.scalar_one_or_none()
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    await check_file_permission(user, meta, "write")

    if file.content_type and file.content_type not in settings.ALLOWED_IMAGE_TYPES:
        allowed = ", ".join(settings.ALLOWED_IMAGE_TYPES)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid content type. Allowed: {allowed}",
        )

    file_size = await get_file_size(file)
    validate_file_size(file_size, settings.MAX_FILE_SIZE)

    file_content = await file.read()
    await file.seek(0)

    await validate_file_magic_bytes(file_content[:16], file.content_type)

    bucket = meta.bucket_name
    key = meta.file_key
    content_type = file.content_type or meta.content_type

    try:
        url = await s3_service.update_file(
            file.file,
            bucket=bucket,
            key=key,
            content_type=content_type,
        )
    except S3ServiceError as exc:
        logger.error("Failed to update file in S3: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="File storage unavailable",
        ) from exc

    meta.url = url
    meta.content_type = content_type
    meta.file_size = len(file_content)
    meta.original_filename = file.filename or meta.original_filename
    await db.commit()
    await db.refresh(meta)

    await kafka_service.send_file_updated(
        {
            "event": "file.updated",
            "file_id": meta.id,
            "url": url,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.id,
        }
    )
    return meta


@router.delete("/{file_id}", status_code=status.HTTP_200_OK)
async def delete_file(
    file_id: int,
    db: AsyncSession = Depends(get_db_session),
    user: UserPrincipal = Depends(get_current_user),
    s3_service: S3Service = Depends(get_s3_service),
) -> dict[str, str]:
    """
    Delete a file (soft delete).

    Args:
        file_id: File ID to delete.
        db: Database session.
        user: Current authenticated user.
        s3_service: S3 service instance.

    Returns:
        Status message.
    """
    res = await db.execute(
        select(FileMetadata).where(
            FileMetadata.id == file_id, FileMetadata.deleted_at.is_(None)
        )
    )
    meta = res.scalar_one_or_none()
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    await check_file_permission(user, meta, "delete")

    try:
        await s3_service.delete_file(bucket=meta.bucket_name, key=meta.file_key)
    except Exception as exc:
        logger.warning(
            "Failed to delete file from S3 (bucket=%s, key=%s): %s",
            meta.bucket_name,
            meta.file_key,
            exc,
        )

    meta.deleted_at = datetime.now(timezone.utc)
    await db.commit()

    await kafka_service.send_file_deleted(
        {
            "event": "file.deleted",
            "file_id": meta.id,
            "file_type": meta.file_type,
            "entity_type": meta.entity_type,
            "entity_id": meta.entity_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.id,
        }
    )
    return {"status": "deleted"}
