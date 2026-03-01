import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.settings import settings


class Base(DeclarativeBase):
    pass


engine: Optional[AsyncEngine] = None
AsyncSessionLocal: Optional[async_sessionmaker[AsyncSession]] = None
_init_lock = asyncio.Lock()


def ensure_engine() -> None:
    """
    Ensure database engine and session maker are initialized.

    Creates engine lazily to avoid driver import during test collection.
    Note: This is a synchronous function for backward compatibility.
    For async contexts, use ensure_engine_async() instead.
    """
    global engine, AsyncSessionLocal
    if engine is not None and AsyncSessionLocal is not None:
        return

    engine = create_async_engine(
        str(settings.DATABASE_URL),
        echo=settings.DEBUG,
        future=True,
    )
    AsyncSessionLocal = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def ensure_engine_async() -> None:
    """
    Ensure database engine and session maker are initialized (async version).

    Uses a lock to prevent race conditions during concurrent initialization.
    This should be used in async contexts (e.g., lifespan handler).
    """
    global engine, AsyncSessionLocal
    async with _init_lock:
        if engine is not None and AsyncSessionLocal is not None:
            return

        engine = create_async_engine(
            str(settings.DATABASE_URL),
            echo=settings.DEBUG,
            future=True,
        )
        AsyncSessionLocal = async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )


def get_engine() -> AsyncEngine:
    """
    Get the database engine.

    Returns:
        AsyncEngine instance.

    Raises:
        RuntimeError: If engine is not initialized.
    """
    ensure_engine()
    if engine is None:
        raise RuntimeError("Database engine not initialized")
    return engine


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get database session dependency.

    Yields:
        AsyncSession for database operations.
    """
    await ensure_engine_async()
    if AsyncSessionLocal is None:
        raise RuntimeError("Database session maker not initialized")
    async with AsyncSessionLocal() as session:
        yield session


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database session.

    Use this outside of FastAPI dependency injection context
    (e.g., in Kafka consumers).

    Yields:
        AsyncSession for database operations.
    """
    await ensure_engine_async()
    if AsyncSessionLocal is None:
        raise RuntimeError("Database session maker not initialized")
    async with AsyncSessionLocal() as session:
        yield session
