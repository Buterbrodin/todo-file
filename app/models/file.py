from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import BigInteger, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy.types import DateTime

from app.backend.db import Base


class FileType(str, Enum):
    """Valid file types for uploads."""

    avatar = "avatar"
    project_logo = "project_logo"
    task_logo = "task_logo"
    task_attachment = "task_attachment"


class EntityType(str, Enum):
    """Valid entity types for file association."""

    user = "user"
    project = "project"
    task = "task"


class FileMetadata(Base):
    """Database model for storing file metadata."""

    __tablename__ = "file_metadata"
    __table_args__ = (
        UniqueConstraint("file_key", name="uq_file_key"),
        Index("idx_entity_type_id", "entity_type", "entity_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_key: Mapped[str] = mapped_column(String(512))
    file_type: Mapped[str] = mapped_column(String(50))
    entity_type: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[int] = mapped_column(Integer)
    uploader_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    original_filename: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str] = mapped_column(String(128))
    file_size: Mapped[int] = mapped_column(BigInteger)
    bucket_name: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(String(1024))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        return (
            f"<FileMetadata id={self.id} file_type={self.file_type!r} "
            f"entity_type={self.entity_type!r} entity_id={self.entity_id}>"
        )
