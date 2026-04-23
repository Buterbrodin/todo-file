from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field, HttpUrl, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    APP_NAME: str = "todo-files"
    DEBUG: bool = False

    DATABASE_URL: PostgresDsn = Field(
        default=PostgresDsn(
            "postgresql+asyncpg://todo_files:todo_files@localhost:5432/todo_files"
        )
    )
    DATABASE_URL_SYNC: PostgresDsn = Field(
        default=PostgresDsn(
            "postgresql+psycopg://todo_files:todo_files@localhost:5432/todo_files"
        )
    )

    S3_ENDPOINT_URL: Optional[str] = None
    S3_ACCESS_KEY_ID: Optional[str] = None
    S3_SECRET_ACCESS_KEY: Optional[str] = None
    S3_REGION: str = "us-east-1"
    S3_USE_SSL: bool = False
    S3_BUCKET_AVATARS: str = "avatars"
    S3_BUCKET_PROJECT_LOGOS: str = "project-logos"
    S3_BUCKET_TASK_LOGOS: str = "task-logos"
    S3_BUCKET_TASK_ATTACHMENTS: str = "task-attachments"
    S3_PRESIGNED_EXPIRE_SECONDS: int = 3600

    MAX_FILE_SIZE: int = 10 * 1024 * 1024
    ALLOWED_IMAGE_TYPES: list[str] = Field(
        default_factory=lambda: ["image/jpeg", "image/png", "image/webp"]
    )

    KAFKA_BOOTSTRAP_SERVERS: Optional[str] = "localhost:9092"
    KAFKA_TOPIC_FILE_UPLOADED: str = "file.uploaded"
    KAFKA_TOPIC_FILE_DELETED: str = "file.deleted"
    KAFKA_TOPIC_FILE_UPDATED: str = "file.updated"
    KAFKA_TOPIC_FILE_UPLOAD_REQUEST: str = "file.upload.request"
    KAFKA_TOPIC_FILE_UPLOAD_RESPONSE: str = "file.upload.response"
    KAFKA_TOPIC_FILE_DELETE_REQUEST: str = "file.delete.request"
    KAFKA_TOPIC_FILE_DELETE_RESPONSE: str = "file.delete.response"
    KAFKA_TOPIC_FILE_LIST_REQUEST: str = "file.list.request"
    KAFKA_TOPIC_FILE_LIST_RESPONSE: str = "file.list.response"

    JWT_SECRET: str = Field(
        default="change_me_change_me_change_me_12345",
        min_length=32,
    )
    JWT_ALG: str = "HS256"
    INTERNAL_API_TOKEN: Optional[str] = None

    AUTH_SERVICE_BASE_URL: Optional[HttpUrl] = None
    AUTH_USER_EXISTS_PATH: str = "/internal/users/exists"

    CORE_SERVICE_BASE_URL: Optional[HttpUrl] = HttpUrl("http://localhost:8000")
    CORE_INTERNAL_TOKEN: Optional[str] = "change_me_internal_token"

    FILE_BASE_URL: str = "http://localhost:8002"

    ENV: Literal["local", "dev", "prod", "test"] = "local"

    @field_validator("DEBUG", mode="before")
    @classmethod
    def _parse_debug(cls, value):
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"release", "prod", "production"}:
                return False
            if normalized in {"debug", "dev", "local"}:
                return True
        return value


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
