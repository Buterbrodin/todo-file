import pytest
from fastapi import status

from app.models.file import FileMetadata


@pytest.mark.asyncio
async def test_upload_file_success(authenticated_client, test_db, test_image_file):
    """Test successful file upload."""
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert "id" in data
    assert "url" in data
    assert data["file_type"] == "avatar"
    assert data["entity_type"] == "user"
    assert data["entity_id"] == 1


@pytest.mark.asyncio
async def test_upload_file_too_large(authenticated_client, test_large_file):
    """Test upload rejection for oversized files."""
    test_large_file.seek(0)
    files = {"file": ("large.png", test_large_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "too large" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_upload_file_invalid_content_type(
    authenticated_client, test_invalid_file
):
    """Test upload rejection for invalid content type."""
    test_invalid_file.seek(0)
    files = {"file": ("test.txt", test_invalid_file, "text/plain")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "content type" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_upload_file_invalid_file_type(authenticated_client, test_image_file):
    """Test upload rejection for invalid file_type parameter."""
    test_image_file.seek(0)
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "invalid_type",
        "entity_type": "user",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
async def test_upload_file_invalid_entity_type(authenticated_client, test_image_file):
    """Test upload rejection for invalid entity_type parameter."""
    test_image_file.seek(0)
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "invalid",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
async def test_upload_file_unauthorized(client, test_image_file):
    """Test upload rejection without authentication."""
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 1,
    }

    response = await client.post("/api/files/upload", files=files, data=data)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_upload_file_other_user(authenticated_client, test_image_file):
    """Test upload rejection when uploading for another user."""
    test_image_file.seek(0)
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 2,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_get_file_success(authenticated_client, test_file_metadata):
    """Test successful file retrieval."""
    response = await authenticated_client.get(f"/api/files/{test_file_metadata.id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == test_file_metadata.id
    assert data["file_key"] == test_file_metadata.file_key


@pytest.mark.asyncio
async def test_get_file_not_found(authenticated_client):
    """Test file retrieval for non-existent file."""
    response = await authenticated_client.get("/api/files/99999")

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_get_file_unauthorized(client, test_file_metadata):
    """Test file retrieval without authentication."""
    response = await client.get(f"/api/files/{test_file_metadata.id}")

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_list_files_success(authenticated_client, test_file_metadata):
    """Test successful file listing."""
    response = await authenticated_client.get("/api/files/")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "files" in data
    assert "total" in data
    assert "page" in data
    assert "page_size" in data
    assert data["total"] >= 1


@pytest.mark.asyncio
async def test_list_files_with_filters(authenticated_client, test_file_metadata):
    """Test file listing with filters."""
    response = await authenticated_client.get(
        "/api/files/?entity_type=user&entity_id=1&file_type=avatar"
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data["files"], list)


@pytest.mark.asyncio
async def test_list_files_pagination(authenticated_client, test_file_metadata):
    """Test file listing pagination."""
    response = await authenticated_client.get("/api/files/?page=1&page_size=10")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["page"] == 1
    assert data["page_size"] == 10


@pytest.mark.asyncio
async def test_update_file_success(
    authenticated_client, test_file_metadata, test_image_file
):
    """Test successful file update."""
    test_image_file.seek(0)
    files = {"file": ("updated.png", test_image_file, "image/png")}
    data = {}

    response = await authenticated_client.put(
        f"/api/files/{test_file_metadata.id}",
        files=files,
        data=data,
    )

    assert response.status_code == status.HTTP_200_OK
    response_data = response.json()
    assert response_data["id"] == test_file_metadata.id


@pytest.mark.asyncio
async def test_update_file_not_found(authenticated_client, test_image_file):
    """Test file update for non-existent file."""
    test_image_file.seek(0)
    files = {"file": ("updated.png", test_image_file, "image/png")}
    data = {}

    response = await authenticated_client.put(
        "/api/files/99999", files=files, data=data
    )

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_update_file_unauthorized(client, test_file_metadata, test_image_file):
    """Test file update without authentication."""
    test_image_file.seek(0)
    files = {"file": ("updated.png", test_image_file, "image/png")}
    data = {}

    response = await client.put(
        f"/api/files/{test_file_metadata.id}",
        files=files,
        data=data,
    )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_delete_file_success(authenticated_client, test_file_metadata):
    """Test successful file deletion."""
    response = await authenticated_client.delete(f"/api/files/{test_file_metadata.id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "deleted"


@pytest.mark.asyncio
async def test_delete_file_not_found(authenticated_client):
    """Test file deletion for non-existent file."""
    response = await authenticated_client.delete("/api/files/99999")

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_delete_file_unauthorized(client, test_file_metadata):
    """Test file deletion without authentication."""
    response = await client.delete(f"/api/files/{test_file_metadata.id}")

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_admin_can_access_other_user_file(admin_client, test_db):
    """Test admin can access another user's file."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    file_meta = FileMetadata(
        file_key="avatar/user/999/test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=999,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://localstack:4566/avatars/avatar/user/999/test.jpg",
        created_at=now,
        updated_at=now,
    )
    test_db.add(file_meta)
    await test_db.commit()
    await test_db.refresh(file_meta)

    response = await admin_client.get(f"/api/files/{file_meta.id}")

    assert response.status_code == status.HTTP_200_OK


@pytest.mark.asyncio
async def test_admin_can_delete_other_user_file(admin_client, test_db):
    """Test admin can delete another user's file."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    file_meta = FileMetadata(
        file_key="avatar/user/999/delete_test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=999,
        original_filename="delete_test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://localstack:4566/avatars/avatar/user/999/delete_test.jpg",
        created_at=now,
        updated_at=now,
    )
    test_db.add(file_meta)
    await test_db.commit()
    await test_db.refresh(file_meta)

    response = await admin_client.delete(f"/api/files/{file_meta.id}")

    assert response.status_code == status.HTTP_200_OK


@pytest.mark.asyncio
async def test_upload_project_logo(authenticated_client, test_db, test_image_file):
    """Test uploading project logo."""
    test_image_file.seek(0)
    files = {"file": ("logo.png", test_image_file, "image/png")}
    data = {
        "file_type": "project_logo",
        "entity_type": "project",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["file_type"] == "project_logo"
    assert data["entity_type"] == "project"


@pytest.mark.asyncio
async def test_upload_task_attachment(authenticated_client, test_db, test_image_file):
    """Test uploading task attachment."""
    test_image_file.seek(0)
    files = {"file": ("attachment.png", test_image_file, "image/png")}
    data = {
        "file_type": "task_attachment",
        "entity_type": "task",
        "entity_id": 1,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["file_type"] == "task_attachment"
    assert data["entity_type"] == "task"


@pytest.mark.asyncio
async def test_list_files_empty(authenticated_client, test_db):
    """Test file listing when no files exist."""
    response = await authenticated_client.get(
        "/api/files/?entity_type=user&entity_id=999"
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["total"] == 0
    assert data["files"] == []


@pytest.mark.asyncio
async def test_update_file_with_new_type(
    authenticated_client, test_file_metadata, test_image_file
):
    """Test updating file with new file type."""
    test_image_file.seek(0)
    files = {"file": ("updated.png", test_image_file, "image/png")}
    data = {"file_type": "avatar"}

    response = await authenticated_client.put(
        f"/api/files/{test_file_metadata.id}",
        files=files,
        data=data,
    )

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["file_type"] == "avatar"


@pytest.mark.asyncio
async def test_get_deleted_file_returns_404(
    authenticated_client, test_db, test_file_metadata
):
    """Test that deleted files return 404."""
    await authenticated_client.delete(f"/api/files/{test_file_metadata.id}")

    response = await authenticated_client.get(f"/api/files/{test_file_metadata.id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_upload_file_invalid_entity_id(authenticated_client, test_image_file):
    """Test upload rejection for invalid entity ID."""
    test_image_file.seek(0)
    files = {"file": ("test.png", test_image_file, "image/png")}
    data = {
        "file_type": "avatar",
        "entity_type": "user",
        "entity_id": 0,
    }

    response = await authenticated_client.post(
        "/api/files/upload", files=files, data=data
    )

    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
async def test_list_files_invalid_file_type_filter(authenticated_client):
    """Test file listing with invalid file type filter."""
    response = await authenticated_client.get("/api/files/?file_type=invalid")

    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
async def test_list_files_invalid_entity_type_filter(authenticated_client):
    """Test file listing with invalid entity type filter."""
    response = await authenticated_client.get("/api/files/?entity_type=invalid")

    assert response.status_code == status.HTTP_400_BAD_REQUEST
