#!/usr/bin/env python
"""
Simple Kafka consumer to verify messages.

Usage:
    python -m tests.test_kafka_consumer
"""
import asyncio
import json
import logging
from datetime import datetime

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


async def consume_kafka_messages():
    """Consume and log Kafka messages."""
    consumer = AIOKafkaConsumer(
        "file.uploaded",
        "file.deleted",
        "file.updated",
        bootstrap_servers="kafka:9092",
        group_id="test-consumer",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info("Listening for Kafka messages...")
    logger.info("=" * 80)

    try:
        async for message in consumer:
            timestamp = datetime.now().isoformat()
            logger.info(
                "\n[%s] Message received on topic: %s",
                timestamp,
                message.topic,
            )
            logger.info("   Payload: %s", json.dumps(message.value, indent=2))
            logger.info("-" * 80)
    except KeyboardInterrupt:
        logger.info("\n\nConsumer stopped")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_kafka_messages())
