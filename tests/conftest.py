import asyncio
import sys
from collections.abc import AsyncGenerator
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.backend.db import Base, get_db
from app.core.deps import get_current_user
from app.core.security import UserPrincipal
from app.main import app
from app.models.file import FileMetadata

TEST_TIMEOUT_SECONDS = 15

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "app") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "app"))


class AsyncSessionAdapter:
    """Lightweight async adapter over sync SQLAlchemy Session for tests."""

    def __init__(self, session: Session) -> None:
        self._session = session

    async def add(self, instance) -> None:
        """Add instance to session asynchronously."""
        self._session.add(instance)

    async def execute(self, *args, **kwargs):
        return self._session.execute(*args, **kwargs)

    async def commit(self) -> None:
        self._session.commit()

    async def refresh(self, instance) -> None:
        self._session.refresh(instance)

    async def close(self) -> None:
        self._session.close()


@pytest.fixture(autouse=True)
async def async_test_timeout_guard() -> AsyncGenerator[None, None]:
    """Fail async tests that run longer than configured timeout."""
    task = asyncio.current_task()
    if task is None:
        yield
        return

    loop = asyncio.get_running_loop()
    handle = loop.call_later(TEST_TIMEOUT_SECONDS, task.cancel)
    try:
        yield
    except asyncio.CancelledError as exc:
        raise AssertionError(
            f"Test timed out after {TEST_TIMEOUT_SECONDS} seconds"
        ) from exc
    finally:
        handle.cancel()


@pytest.fixture(autouse=True)
def integration_external_services_stub(request, monkeypatch):
    """Stub external service I/O for integration tests to avoid hangs."""
    if "tests/integration/" not in request.node.nodeid.replace("\\", "/"):
        return

    async def _project_access(*args, **kwargs) -> bool:
        return True

    async def _task_access(*args, **kwargs) -> bool:
        return True

    async def _upload_file(_file_obj, bucket: str, key: str, content_type: str) -> str:
        return f"http://test-s3/{bucket}/{key}"

    async def _update_file(_file_obj, bucket: str, key: str, content_type: str) -> str:
        return f"http://test-s3/{bucket}/{key}"

    async def _delete_file(*args, **kwargs) -> bool:
        return True

    async def _get_http_client():
        return AsyncMock()

    monkeypatch.setattr(
        "app.core.permissions.CoreServiceClient.check_project_access",
        _project_access,
    )
    monkeypatch.setattr(
        "app.core.permissions.CoreServiceClient.check_task_access",
        _task_access,
    )
    monkeypatch.setattr("app.services.s3_service.s3_service.upload_file", _upload_file)
    monkeypatch.setattr("app.services.s3_service.s3_service.update_file", _update_file)
    monkeypatch.setattr("app.services.s3_service.s3_service.delete_file", _delete_file)
    monkeypatch.setattr("app.services.core_client.get_http_client", _get_http_client)
    monkeypatch.setattr("app.services.core_client.close_http_client", AsyncMock())
    monkeypatch.setattr("app.main.db.ensure_engine", lambda: None)
    monkeypatch.setattr("app.main.s3_service._ensure_session", lambda: None)
    monkeypatch.setattr("app.main.s3_service._ensure_buckets", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.start", AsyncMock())
    monkeypatch.setattr("app.main.kafka_service.stop", AsyncMock())


@pytest.fixture(scope="function")
async def test_db() -> AsyncGenerator[AsyncSessionAdapter, None]:
    """Create test database with in-memory SQLite."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)

    sync_session_maker = sessionmaker(
        engine,
        expire_on_commit=False,
    )
    sync_session = sync_session_maker()
    session = AsyncSessionAdapter(sync_session)
    try:
        yield session
    finally:
        await session.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


@pytest.fixture
async def client(
    test_db: AsyncSessionAdapter,
) -> AsyncGenerator[AsyncClient, None]:
    """Create test HTTP client with database override."""

    async def override_get_db() -> AsyncSessionAdapter:
        return test_db

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture
def test_user() -> UserPrincipal:
    """Create test user principal."""
    return UserPrincipal(user_id=1, roles=["member"], email="test@example.com")


@pytest.fixture
def test_admin() -> UserPrincipal:
    """Create test admin principal."""
    return UserPrincipal(user_id=2, roles=["admin"], email="admin@example.com")


@pytest.fixture
async def authenticated_client(
    client: AsyncClient,
    test_user: UserPrincipal,
) -> AsyncGenerator[AsyncClient, None]:
    """Create authenticated test client."""

    async def override_get_current_user() -> UserPrincipal:
        return test_user

    app.dependency_overrides[get_current_user] = override_get_current_user
    yield client
    app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
async def admin_client(
    client: AsyncClient,
    test_admin: UserPrincipal,
) -> AsyncGenerator[AsyncClient, None]:
    """Create admin authenticated test client."""

    async def override_get_current_user() -> UserPrincipal:
        return test_admin

    app.dependency_overrides[get_current_user] = override_get_current_user
    yield client
    app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def test_image_file() -> BytesIO:
    """Create valid PNG test file."""
    image_data = b"\x89PNG\r\n\x1a\n" + b"0" * 100
    return BytesIO(image_data)


@pytest.fixture
def test_large_file() -> BytesIO:
    """Create file exceeding max size limit."""
    return BytesIO(b"x" * (11 * 1024 * 1024))


@pytest.fixture
def test_invalid_file() -> BytesIO:
    """Create invalid file (not an image)."""
    return BytesIO(b"not an image content")


@pytest.fixture
async def test_file_metadata(test_db: AsyncSessionAdapter) -> FileMetadata:
    """Create test file metadata in database."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    file_meta = FileMetadata(
        file_key="avatar/user/1/1234567890_test.jpg",
        file_type="avatar",
        entity_type="user",
        entity_id=1,
        original_filename="test.jpg",
        content_type="image/jpeg",
        file_size=1024,
        bucket_name="avatars",
        url="http://localstack:4566/avatars/avatar/user/1/1234567890_test.jpg",
        created_at=now,
        updated_at=now,
    )
    await test_db.add(file_meta)
    await test_db.commit()
    await test_db.refresh(file_meta)
    return file_meta
