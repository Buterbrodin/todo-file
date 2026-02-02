from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.backend.db import engine
from app.routers import files
from app.services.kafka_service import kafka_service
from app.services.s3_service import S3Service


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Application lifespan handler.

    Initializes S3 buckets and Kafka producer on startup,
    cleans up resources on shutdown.
    """
    s3_service = S3Service()
    try:
        await kafka_service.start()
        await s3_service._ensure_buckets()
        yield
    finally:
        await kafka_service.stop()
        if engine:
            await engine.dispose()


app = FastAPI(title="todo-files", lifespan=lifespan)
app.include_router(files.router)
