#!/usr/bin/env python
"""
Simple Kafka consumer to verify messages.

Usage:
    python -m tests.test_kafka_consumer
"""
import asyncio
import json
from datetime import datetime

from aiokafka import AIOKafkaConsumer


async def consume_kafka_messages():
    """Consume and print Kafka messages."""
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
    print("🎧 Listening for Kafka messages...")
    print("=" * 80)

    try:
        async for message in consumer:
            timestamp = datetime.now().isoformat()
            print(f"\n📨 [{timestamp}] Message received on topic: {message.topic}")
            print(f"   Payload: {json.dumps(message.value, indent=2)}")
            print("-" * 80)
    except KeyboardInterrupt:
        print("\n\n✅ Consumer stopped")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_kafka_messages())
