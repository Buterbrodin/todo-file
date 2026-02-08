#!/usr/bin/env python
"""
Create Kafka topics for the application.

Usage:
    python -m tests.create_kafka_topics
"""
import asyncio
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def create_topics():
    """Create required Kafka topics."""
    admin_client = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin_client.start()

    topics = [
        NewTopic(
            name="file.uploaded",
            num_partitions=1,
            replication_factor=1,
            topic_configs={},
        ),
        NewTopic(
            name="file.deleted",
            num_partitions=1,
            replication_factor=1,
            topic_configs={},
        ),
        NewTopic(
            name="file.updated",
            num_partitions=1,
            replication_factor=1,
            topic_configs={},
        ),
    ]

    try:
        result = await admin_client.create_topics(topics, validate_only=False)

        for topic_name, future in result.items():
            try:
                await future
                print(f"✅ Topic '{topic_name}' created successfully")
            except Exception as exc:
                if "already exists" in str(exc):
                    print(f"ℹ️  Topic '{topic_name}' already exists")
                else:
                    print(f"❌ Failed to create topic '{topic_name}': {exc}")
    except Exception as exc:
        print(f"❌ Error: {exc}")
        sys.exit(1)
    finally:
        await admin_client.stop()


if __name__ == "__main__":
    print("🔧 Creating Kafka topics...")
    print("=" * 60)
    asyncio.run(create_topics())
    print("=" * 60)
    print("✅ Done!")
