import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend import db
from app.backend.db import get_db
from app.routers import files
from app.services.core_client import close_http_client
from app.services.kafka_request_consumer import kafka_request_consumer
from app.services.kafka_service import kafka_service
from app.services.s3_service import s3_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Application lifespan handler.

    Initializes DB engine, S3 buckets and Kafka producer on startup,
    cleans up resources on shutdown.
    """
    try:
        logger.info("Initializing database...")
        db.ensure_engine()
        logger.info("Starting Kafka services...")
        await kafka_service.start()
        await kafka_request_consumer.start()
        logger.info("Initializing S3 service...")
        s3_service._ensure_session()
        await s3_service._ensure_buckets()
        logger.info("Application startup complete")
        yield
    finally:
        logger.info("Shutting down application...")
        await kafka_request_consumer.stop()
        await kafka_service.stop()
        await close_http_client()
        if db.engine:
            await db.engine.dispose()
        logger.info("Application shutdown complete")


app = FastAPI(title="todo-files", lifespan=lifespan)
app.include_router(files.router)


@app.get("/health", tags=["health"])
async def health_check(db: AsyncSession = Depends(get_db)) -> dict[str, str]:
    """
    Health check endpoint.

    Verifies database connectivity and returns service status.

    Returns:
        Status dict with 'status' key.
    """
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc
    return {"status": "ok"}
