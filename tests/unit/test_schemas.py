from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.schemas.file import FileListResponse, FileResponse, FileUpdate, FileUpload


class TestFileUpload:
    def test_file_upload_valid(self):
        schema = FileUpload(
            file_type="avatar",
            entity_type="user",
            entity_id=1,
        )

        assert schema.file_type == "avatar"
        assert schema.entity_type == "user"
        assert schema.entity_id == 1

    def test_file_upload_missing_fields(self):
        with pytest.raises(ValidationError):
            FileUpload(file_type="avatar")


class TestFileUpdate:
    def test_file_update_with_type(self):
        schema = FileUpdate(file_type="project_logo")

        assert schema.file_type == "project_logo"

    def test_file_update_empty(self):
        schema = FileUpdate()

        assert schema.file_type is None


class TestFileResponse:
    def test_file_response_valid(self):
        now = datetime.now(timezone.utc)
        schema = FileResponse(
            id=1,
            url="http://localhost:9000/avatars/test.jpg",
            file_key="avatar/user/1/test.jpg",
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            original_filename="test.jpg",
            content_type="image/jpeg",
            file_size=1024,
            bucket_name="avatars",
            created_at=now,
            updated_at=now,
        )

        assert schema.id == 1
        assert schema.file_type == "avatar"
        assert schema.file_size == 1024


class TestFileListResponse:
    def test_file_list_response_empty(self):
        schema = FileListResponse(
            files=[],
            total=0,
        )

        assert schema.files == []
        assert schema.total == 0
        assert schema.page == 1
        assert schema.page_size == 20

    def test_file_list_response_with_files(self):
        now = datetime.now(timezone.utc)
        file_response = FileResponse(
            id=1,
            url="http://localhost:9000/avatars/test.jpg",
            file_key="test.jpg",
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            original_filename="test.jpg",
            content_type="image/jpeg",
            file_size=1024,
            bucket_name="avatars",
            created_at=now,
            updated_at=now,
        )

        schema = FileListResponse(
            files=[file_response],
            total=1,
            page=2,
            page_size=10,
        )

        assert len(schema.files) == 1
        assert schema.total == 1
        assert schema.page == 2
        assert schema.page_size == 10
