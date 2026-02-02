import asyncio
import json
import logging
from typing import Any, Optional

from app.settings import settings

logger = logging.getLogger(__name__)


class KafkaService:
    """Service for sending events to Kafka."""

    def __init__(self) -> None:
        """Initialize Kafka service."""
        self._producer: Optional[Any] = None
        self._producer_cls: Optional[type] = None
        self._started = False
        self._max_retries = 3
        self._retry_delay = 1.0

    def _load_producer_cls(self) -> None:
        """Load AIOKafkaProducer class if available."""
        if self._producer_cls is not None:
            return
        try:
            from aiokafka import AIOKafkaProducer

            self._producer_cls = AIOKafkaProducer
        except ModuleNotFoundError:
            logger.warning("aiokafka not installed, Kafka will be disabled")
            self._producer_cls = None

    async def start(self) -> None:
        """Start the Kafka producer."""
        if self._started or not settings.KAFKA_BOOTSTRAP_SERVERS:
            return

        self._load_producer_cls()
        if not self._producer_cls:
            return

        try:
            self._producer = self._producer_cls(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
            )
            await self._producer.start()
            self._started = True
            logger.info("Kafka producer started")
        except Exception as exc:
            logger.error("Failed to start Kafka producer: %s", exc)
            self._producer = None

    async def stop(self) -> None:
        """Stop the Kafka producer gracefully."""
        if self._producer:
            try:
                await self._producer.stop()
                logger.info("Kafka producer stopped")
            except Exception as exc:
                logger.error("Error stopping Kafka producer: %s", exc)
            finally:
                self._producer = None
                self._started = False

    async def _send(self, topic: str, payload: dict[str, Any]) -> bool:
        """
        Send message to Kafka topic with retry logic.

        Args:
            topic: Kafka topic name.
            payload: Message payload.

        Returns:
            True if message was sent successfully.
        """
        if not settings.KAFKA_BOOTSTRAP_SERVERS:
            return False

        if not self._started:
            await self.start()

        if not self._producer:
            logger.warning("Kafka producer not available, message dropped")
            return False

        message = json.dumps(payload).encode("utf-8")

        for attempt in range(self._max_retries):
            try:
                await self._producer.send_and_wait(topic, message)
                return True
            except Exception as exc:
                logger.warning(
                    "Kafka send attempt %d/%d failed: %s",
                    attempt + 1,
                    self._max_retries,
                    exc,
                )
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay * (attempt + 1))

        logger.error(
            "Failed to send message to Kafka after %d attempts", self._max_retries
        )
        return False

    async def send_file_uploaded(self, payload: dict[str, Any]) -> bool:
        """
        Send file.uploaded event.

        Args:
            payload: Event payload containing file info.

        Returns:
            True if sent successfully.
        """
        return await self._send(settings.KAFKA_TOPIC_FILE_UPLOADED, payload)

    async def send_file_deleted(self, payload: dict[str, Any]) -> bool:
        """
        Send file.deleted event.

        Args:
            payload: Event payload containing file info.

        Returns:
            True if sent successfully.
        """
        return await self._send(settings.KAFKA_TOPIC_FILE_DELETED, payload)

    async def send_file_updated(self, payload: dict[str, Any]) -> bool:
        """
        Send file.updated event.

        Args:
            payload: Event payload containing file info.

        Returns:
            True if sent successfully.
        """
        return await self._send(settings.KAFKA_TOPIC_FILE_UPDATED, payload)


kafka_service = KafkaService()
