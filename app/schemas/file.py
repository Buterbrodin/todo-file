from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FileUpload(BaseModel):
    """Schema for file upload request parameters."""

    file_type: str
    entity_type: str
    entity_id: int


class FileResponse(BaseModel):
    """Schema for file metadata response."""

    id: int
    url: str
    file_key: str
    file_type: str
    entity_type: str
    entity_id: int
    uploader_id: Optional[int]
    original_filename: str
    content_type: str
    file_size: int
    bucket_name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class FileListResponse(BaseModel):
    """Schema for paginated file list response."""

    files: list[FileResponse] = Field(default_factory=list)
    total: int
    page: int = 1
    page_size: int = 20
