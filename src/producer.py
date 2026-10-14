"""Kafka producer for Mars weather observations.

Polls NASA (or the bundled sample data) on an interval and publishes
each sol observation as a JSON message to Kafka. Run this alongside the
Spark streaming job — one terminal for the producer, one for Spark.

Example:
    python src/producer.py --once --use-sample
    python src/producer.py --interval-seconds 120
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from nasa_client import get_observations

logger = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.kafka_client_id,
        # Serialize dicts to UTF-8 JSON bytes.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        # Wait for the leader to acknowledge — good enough for a demo,
        # and much faster than waiting for all replicas.
        acks=1,
        retries=5,
        linger_ms=50,
    )


def publish_once(producer: KafkaProducer, use_sample: bool = False) -> int:
    """Fetch observations and publish each one. Returns message count."""
    observations = get_observations(use_sample=use_sample)
    sent = 0

    for obs in observations:
        sol = obs.get("sol")
        try:
            # Use the sol as the key so all records for a sol land
            # on the same partition, keeping ordering per sol.
            future = producer.send(
                settings.kafka_topic, key=sol, value=obs
            )
            future.get(timeout=10)
            sent += 1
            logger.info("Published sol %s to %s", sol, settings.kafka_topic)
        except KafkaError as exc:
            logger.error("Failed to publish sol %s: %s", sol, exc)

    # Block until everything buffered is actually sent.
    producer.flush()
    return sent


def run_loop(interval_seconds: int, use_sample: bool) -> None:
    """Poll forever until interrupted with Ctrl+C."""
    producer = build_producer()
    logger.info(
        "Starting producer: topic=%s brokers=%s interval=%ds",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        interval_seconds,
    )
    try:
        while True:
            count = publish_once(producer, use_sample=use_sample)
            logger.info("Cycle complete: %d messages sent", count)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Mars weather observations to Kafka"
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=settings.poll_interval_seconds,
        help="Seconds between polls (default: %(default)s)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Publish once and exit instead of looping",
    )
    parser.add_argument(
        "--use-sample",
        action="store_true",
        help="Use bundled sample data instead of the NASA API",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    args = parse_args()

    if args.once:
        producer = build_producer()
        try:
            count = publish_once(producer, use_sample=args.use_sample)
            print(f"Published {count} messages")
        finally:
            producer.close()
    else:
        run_loop(
            interval_seconds=args.interval_seconds,
            use_sample=args.use_sample,
        )


if __name__ == "__main__":
    main()
