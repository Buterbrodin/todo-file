import pytest
from fastapi import HTTPException

from app.core.permissions import (
    check_file_permission,
    validate_content_type,
    validate_entity_exists,
    validate_entity_type,
    validate_file_magic_bytes,
    validate_file_size,
    validate_file_type,
)
from app.core.security import UserPrincipal
from app.models.file import FileMetadata


@pytest.mark.asyncio
async def test_check_file_permission_admin():
    """Test admin has full access to all files."""
    user = UserPrincipal(user_id=1, roles=["admin"], email="admin@example.com")
    file_meta = FileMetadata(
        file_key="test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=2,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test.com/test.jpg",
    )

    result = await check_file_permission(user, file_meta, "read")
    assert result is True


@pytest.mark.asyncio
async def test_check_file_permission_own_avatar():
    """Test user can access their own avatar."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    file_meta = FileMetadata(
        file_key="test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=1,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test.com/test.jpg",
    )

    result = await check_file_permission(user, file_meta, "read")
    assert result is True


@pytest.mark.asyncio
async def test_check_file_permission_other_user_avatar_read():
    """Test user cannot read other user's avatar."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    file_meta = FileMetadata(
        file_key="test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=2,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test.com/test.jpg",
    )

    with pytest.raises(HTTPException) as exc_info:
        await check_file_permission(user, file_meta, "read")
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_check_file_permission_other_user_avatar_write():
    """Test user cannot write to other user's avatar."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    file_meta = FileMetadata(
        file_key="test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=2,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://test.com/test.jpg",
    )

    with pytest.raises(HTTPException) as exc_info:
        await check_file_permission(user, file_meta, "write")
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_check_file_permission_project_file():
    """Test user can access project files."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    file_meta = FileMetadata(
        file_key="test.jpg",
        file_type="project_logo",
        entity_type="project",
        entity_id=1,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="project-logos",
        url="http://test.com/test.jpg",
    )

    result = await check_file_permission(user, file_meta, "read")
    assert result is True


def test_validate_file_type_valid():
    """Test validation passes for valid file types."""
    validate_file_type("avatar")
    validate_file_type("project_logo")
    validate_file_type("task_logo")
    validate_file_type("task_attachment")


def test_validate_file_type_invalid():
    """Test validation fails for invalid file types."""
    with pytest.raises(HTTPException) as exc_info:
        validate_file_type("invalid_type")
    assert exc_info.value.status_code == 400


def test_validate_entity_type_valid():
    """Test validation passes for valid entity types."""
    validate_entity_type("user")
    validate_entity_type("project")
    validate_entity_type("task")


def test_validate_entity_type_invalid():
    """Test validation fails for invalid entity types."""
    with pytest.raises(HTTPException) as exc_info:
        validate_entity_type("invalid_type")
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_validate_entity_exists_own_user():
    """Test validation passes for own user entity."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    await validate_entity_exists("user", 1, user)


@pytest.mark.asyncio
async def test_validate_entity_exists_other_user():
    """Test validation fails for other user entity."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")

    with pytest.raises(HTTPException) as exc_info:
        await validate_entity_exists("user", 2, user)
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_validate_entity_exists_admin():
    """Test admin can access any user entity."""
    user = UserPrincipal(user_id=1, roles=["admin"], email="admin@example.com")
    await validate_entity_exists("user", 2, user)


@pytest.mark.asyncio
async def test_validate_entity_exists_project():
    """Test validation passes for project entity."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")
    await validate_entity_exists("project", 1, user)


@pytest.mark.asyncio
async def test_validate_entity_exists_invalid_id():
    """Test validation fails for invalid entity ID."""
    user = UserPrincipal(user_id=1, roles=["member"], email="user@example.com")

    with pytest.raises(HTTPException) as exc_info:
        await validate_entity_exists("project", 0, user)
    assert exc_info.value.status_code == 400


def test_validate_content_type_valid():
    """Test validation passes for valid content types."""
    validate_content_type("image/jpeg", ["image/jpeg", "image/png"])
    validate_content_type("image/png", ["image/jpeg", "image/png"])


def test_validate_content_type_invalid():
    """Test validation fails for invalid content types."""
    with pytest.raises(HTTPException) as exc_info:
        validate_content_type("text/plain", ["image/jpeg", "image/png"])
    assert exc_info.value.status_code == 400


def test_validate_file_size_valid():
    """Test validation passes for valid file size."""
    validate_file_size(1024, 10240)


def test_validate_file_size_too_large():
    """Test validation fails for oversized files."""
    with pytest.raises(HTTPException) as exc_info:
        validate_file_size(20480, 10240)
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_validate_file_magic_bytes_png():
    """Test validation passes for valid PNG magic bytes."""
    png_magic = b"\x89PNG\r\n\x1a\n" + b"0" * 8
    await validate_file_magic_bytes(png_magic, "image/png")


@pytest.mark.asyncio
async def test_validate_file_magic_bytes_jpeg():
    """Test validation passes for valid JPEG magic bytes."""
    jpeg_magic = b"\xff\xd8\xff" + b"0" * 13
    await validate_file_magic_bytes(jpeg_magic, "image/jpeg")


@pytest.mark.asyncio
async def test_validate_file_magic_bytes_invalid():
    """Test validation fails for invalid file content."""
    invalid_content = b"not an image"

    with pytest.raises(HTTPException) as exc_info:
        await validate_file_magic_bytes(invalid_content, "image/png")
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_validate_file_magic_bytes_empty():
    """Test validation fails for empty file."""
    with pytest.raises(HTTPException) as exc_info:
        await validate_file_magic_bytes(b"", "image/png")
    assert exc_info.value.status_code == 400
