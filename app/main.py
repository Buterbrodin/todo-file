from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.backend.db import engine
from app.routers import files
from app.services.core_client import close_http_client
from app.services.kafka_service import kafka_service
from app.services.s3_service import s3_service


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Application lifespan handler.

    Initializes S3 buckets and Kafka producer on startup,
    cleans up resources on shutdown.
    """
    try:
        await kafka_service.start()
        await s3_service._ensure_buckets()
        yield
    finally:
        await kafka_service.stop()
        await close_http_client()
        if engine:
            await engine.dispose()


app = FastAPI(title="todo-files", lifespan=lifespan)
app.include_router(files.router)
