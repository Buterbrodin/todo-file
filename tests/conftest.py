import sys
from collections.abc import AsyncGenerator
from io import BytesIO
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.backend.db import Base
from app.core.deps import get_current_user, get_db_session
from app.core.security import UserPrincipal
from app.main import app
from app.models.file import FileMetadata

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "app") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "app"))


@pytest.fixture(scope="function")
async def test_db() -> AsyncGenerator[AsyncSession, None]:
    """Create test database with in-memory SQLite."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_maker = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    async with async_session_maker() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.fixture
async def client(test_db: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Create test HTTP client with database override."""
    async def override_get_db_session() -> AsyncSession:
        return test_db

    app.dependency_overrides[get_db_session] = override_get_db_session
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
async def test_file_metadata(test_db: AsyncSession) -> FileMetadata:
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
    test_db.add(file_meta)
    await test_db.commit()
    await test_db.refresh(file_meta)
    return file_meta
