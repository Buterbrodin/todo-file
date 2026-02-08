from io import BytesIO
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from app.core.exceptions import S3ServiceError
from app.core.security import UserPrincipal
from app.models.file import FileMetadata
from app.routers.files import list_files, update_file, upload_file


class DummyUploadFile:
    def __init__(self, content: bytes, filename: str, content_type: str) -> None:
        self.file = BytesIO(content)
        self.filename = filename
        self.content_type = content_type
        self.size = len(content)

    async def read(self) -> bytes:
        return self.file.read()

    async def seek(self, offset: int) -> None:
        self.file.seek(offset)


class DummyResult:
    def __init__(self, value) -> None:
        self._value = value

    def scalar_one_or_none(self):
        return self._value


@pytest.mark.asyncio
async def test_list_files_non_admin_requires_entity_params():
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await list_files(
            entity_type=None,
            entity_id=None,
            file_type=None,
            db=db,
            user=user,
        )

    assert exc_info.value.status_code == 400
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_list_files_non_admin_project_denied():
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    db = AsyncMock()

    with patch(
        "app.routers.files.CoreServiceClient.check_project_access",
        new=AsyncMock(return_value=False),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await list_files(
                entity_type="project",
                entity_id=1,
                file_type=None,
                db=db,
                user=user,
            )

    assert exc_info.value.status_code == 403
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_upload_file_returns_503_when_storage_fails():
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    db = AsyncMock()
    s3 = AsyncMock()
    s3.upload_file.side_effect = S3ServiceError("s3 down")
    file = DummyUploadFile(
        content=b"\x89PNG\r\n\x1a\n" + b"0" * 32,
        filename="avatar.png",
        content_type="image/png",
    )

    with pytest.raises(HTTPException) as exc_info:
        await upload_file(
            file=file,
            file_type="avatar",
            entity_type="user",
            entity_id=1,
            db=db,
            user=user,
            s3_service=s3,
        )

    assert exc_info.value.status_code == 503
    db.commit.assert_not_called()


@pytest.mark.asyncio
async def test_update_file_returns_503_when_storage_fails():
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    file_meta = FileMetadata(
        id=1,
        file_key="avatar/user/1/original.png",
        file_type="avatar",
        entity_type="user",
        entity_id=1,
        original_filename="original.png",
        content_type="image/png",
        file_size=100,
        bucket_name="avatars",
        url="http://localstack:4566/avatars/avatar/user/1/original.png",
    )

    db = AsyncMock()
    db.execute.return_value = DummyResult(file_meta)

    s3 = AsyncMock()
    s3.update_file.side_effect = S3ServiceError("s3 down")
    file = DummyUploadFile(
        content=b"\x89PNG\r\n\x1a\n" + b"1" * 32,
        filename="new.png",
        content_type="image/png",
    )

    with pytest.raises(HTTPException) as exc_info:
        await update_file(file_id=1, file=file, db=db, user=user, s3_service=s3)

    assert exc_info.value.status_code == 503
    db.commit.assert_not_called()
