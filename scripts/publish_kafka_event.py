#!/usr/bin/env python3
"""
Simple script to publish test TemplatePublishEvent messages to Kafka.

Usage:
    pip install kafka-python
    python publish_kafka_event.py [--count N] [--template TPL-ID] [--firm FIRM-ID]

Example:
    python publish_kafka_event.py --count 5 --template TPL-AUDIT-2024 --firm FIRM-001
"""

import argparse
import json
import uuid
import sys
from typing import List


def generate_event(
    template_id: str = "TPL-AUDIT-2024",
    version: str = "v5.2",
    affected_firms: List[str] = None
) -> dict:
    """Create a TemplatePublishEvent payload matching our Java record."""
    return {
        "eventId": f"evt-1",
        "templateId": template_id,
        "version": version,
        "affectedFirms": affected_firms or ["FIRM-001", "FIRM-002", "FIRM-003"]
    }


def publish_single_event(producer, topic: str, event: dict) -> tuple:
    """
    Send one event to Kafka and return partition/offset info.

    NOTE: we pass the raw str key and the raw dict value. The producer's
    configured serializers handle encoding — encoding here as well would
    double-encode and raise AttributeError on the second pass.
    """
    future = producer.send(topic, key=event["eventId"], value=event)
    metadata = future.get(timeout=10)

    print(f"OK Published {event['eventId']}")
    print(f"   Template: {event['templateId']} {event['version']}")
    print(f"   Affected firms: {len(event['affectedFirms'])}")
    print(f"   Partition: {metadata.partition}, Offset: {metadata.offset}\n")

    return (metadata.partition, metadata.offset)


def main():
    parser = argparse.ArgumentParser(description="Publish test TemplatePublishEvent to Kafka")
    parser.add_argument("--count", "-n", type=int, default=1, help="Number of events to publish")
    parser.add_argument("--template", "-t", type=str, default="TPL-AUDIT-2024", help="Template ID")
    parser.add_argument("--version", "-v", type=str, default="v5.2", help="Template version")
    parser.add_argument("--firm", "-f", type=str, action="append", help="Add firm ID (repeat for multiple)")
    parser.add_argument("--brokers", "-b", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="template-publishes", help="Kafka topic name")

    args = parser.parse_args()

    # Build affected firms list
    firms = args.firm or ["FIRM-001", "FIRM-002", "FIRM-003"]

    print(f"\n{'='*60}")
    print(f"Kafka Event Publisher - Template Update Flow")
    print(f"{'='*60}")
    print(f"Bootstrap Servers: {args.brokers}")
    print(f"Topic: {args.topic}")
    print(f"Events to publish: {args.count}")
    print(f"Firms: {', '.join(firms)}\n")

    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError

        # Configure producer.
        # Serializers receive the raw Python objects passed to send().
        producer = KafkaProducer(
            bootstrap_servers=[args.brokers],
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )

        print(f"Connected to Kafka broker(s)...")

        # Publish events
        total = 0
        failures = 0

        for i in range(args.count):
            event = generate_event(
                template_id=args.template,
                version=args.version,
                affected_firms=firms
            )

            try:
                publish_single_event(producer, args.topic, event)
                total += 1
            except KafkaError as e:
                print(f"X Failed to publish {event['eventId']}: {e}")
                failures += 1

            # Small delay between events to avoid overwhelming Kafka
            if args.count > 1:
                import time
                time.sleep(0.1)

        # Flush remaining messages
        producer.flush()
        producer.close()

        print(f"{'='*60}")
        print(f"Results: {total} succeeded, {failures} failed")
        print(f"{'='*60}\n")

        if failures > 0:
            sys.exit(1)

    except ImportError:
        print("\nError: kafka-python library not installed")
        print("Install it with: pip install kafka-python\n")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
