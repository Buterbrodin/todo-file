from datetime import datetime, timezone

import pytest

from app.models.file import FileMetadata


class TestFileMetadata:
    def test_file_metadata_creation(self):
        now = datetime.now(timezone.utc)
        file_meta = FileMetadata(
            file_key="avatar/user/1/test.jpg",
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            original_filename="test.jpg",
            content_type="image/jpeg",
            file_size=1024,
            bucket_name="avatars",
            url="http://localhost:9000/avatars/test.jpg",
            created_at=now,
            updated_at=now,
        )

        assert file_meta.file_key == "avatar/user/1/test.jpg"
        assert file_meta.file_type == "avatar"
        assert file_meta.entity_type == "user"
        assert file_meta.entity_id == 1
        assert file_meta.original_filename == "test.jpg"
        assert file_meta.content_type == "image/jpeg"
        assert file_meta.file_size == 1024
        assert file_meta.bucket_name == "avatars"
        assert file_meta.deleted_at is None

    def test_file_metadata_deleted_at(self):
        now = datetime.now(timezone.utc)
        file_meta = FileMetadata(
            file_key="test.jpg",
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            original_filename="test.jpg",
            content_type="image/jpeg",
            file_size=1024,
            bucket_name="avatars",
            url="http://localhost:9000/avatars/test.jpg",
            created_at=now,
            updated_at=now,
            deleted_at=now,
        )

        assert file_meta.deleted_at is not None
        assert file_meta.deleted_at == now

    def test_file_metadata_table_name(self):
        assert FileMetadata.__tablename__ == "file_metadata"
