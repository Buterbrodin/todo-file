from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.sql import func

from app.backend.db import Base


class FileMetadata(Base):
    """Database model for storing file metadata."""

    __tablename__ = "file_metadata"
    __table_args__ = (
        UniqueConstraint("file_key", name="uq_file_key"),
        Index("idx_entity_type_id", "entity_type", "entity_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_key = Column(String(512), nullable=False)
    file_type = Column(String(50), nullable=False)
    entity_type = Column(String(50), nullable=False)
    entity_id = Column(Integer, nullable=False)
    original_filename = Column(String(255), nullable=False)
    content_type = Column(String(128), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    bucket_name = Column(String(255), nullable=False)
    url = Column(String(1024), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    deleted_at = Column(DateTime(timezone=True), nullable=True)
