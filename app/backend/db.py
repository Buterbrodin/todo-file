from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

from app.settings import settings

Base = declarative_base()

engine: Optional[AsyncEngine] = None
AsyncSessionLocal: Optional[async_sessionmaker[AsyncSession]] = None


def ensure_engine() -> None:
    """
    Ensure database engine and session maker are initialized.

    Creates engine lazily to avoid driver import during test collection.
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


def get_engine() -> AsyncEngine:
    """
    Get the database engine.

    Returns:
        AsyncEngine instance.
    """
    ensure_engine()
    assert engine is not None
    return engine


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get database session dependency.

    Yields:
        AsyncSession for database operations.
    """
    ensure_engine()
    assert AsyncSessionLocal is not None
    async with AsyncSessionLocal() as session:
        yield session
