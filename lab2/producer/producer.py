#!/usr/bin/env python3
"""
Producer for simulating stream of user events into Apache Kafka.
Supports configurable event rate, out-of-order event timestamps (for Watermark testing),
and session activity bursts (for Session Window testing).
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import UTC, datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Producer")

EVENT_TYPES = [
    ("view", 0.50, 0.0),
    ("click", 0.30, 0.0),
    ("add_to_cart", 0.12, 0.0),
    ("purchase", 0.05, 10.0),
    ("logout", 0.03, 0.0),
]

PAGES = [
    "/home",
    "/catalog",
    "/catalog/electronics",
    "/catalog/books",
    "/product/laptop-pro",
    "/product/phone-max",
    "/cart",
    "/checkout",
    "/profile",
]


def create_kafka_producer(bootstrap_servers: str):
    """
    Creates Kafka producer with fallback support for confluent_kafka and kafka-python.
    """
    # Try confluent_kafka first
    try:
        from confluent_kafka import Producer as ConfluentProducer

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "user-event-producer",
            "linger.ms": 5,
            "acks": "1",
        }
        c_prod = ConfluentProducer(conf)

        class ConfluentWrapper:
            def __init__(self, prod):
                self._prod = prod

            def send(self, topic, key, value):
                self._prod.produce(
                    topic=topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value.encode("utf-8") if isinstance(value, str) else value,
                )
                self._prod.poll(0)

            def flush(self):
                self._prod.flush()

        logger.info("Using confluent_kafka producer.")
        return ConfluentWrapper(c_prod)
    except ImportError:
        pass

    # Try kafka-python
    try:
        from kafka import KafkaProducer

        k_prod = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            linger_ms=5,
            acks=1,
        )

        class KafkaPythonWrapper:
            def __init__(self, prod):
                self._prod = prod

            def send(self, topic, key, value):
                self._prod.send(topic, key=key, value=value)

            def flush(self):
                self._prod.flush()

        logger.info("Using kafka-python producer.")
        return KafkaPythonWrapper(k_prod)
    except ImportError:
        pass

    raise RuntimeError(
        "Neither 'confluent-kafka' nor 'kafka-python' / 'kafka-python-ng' is installed. "
        "Please run: pip install kafka-python-ng or pip install confluent-kafka"
    )


def generate_event(user_id: str, out_of_order_delay_sec: float = 0.0) -> dict:
    """
    Generates a synthetic user event with simulated event time and actual send time.
    """
    now = time.time()
    event_time_epoch = now - out_of_order_delay_sec
    event_dt = datetime.fromtimestamp(event_time_epoch, tz=UTC)

    # Weighted choice of event type
    types = [t[0] for t in EVENT_TYPES]
    weights = [t[1] for t in EVENT_TYPES]
    event_type = random.choices(types, weights=weights, k=1)[0]

    amount = 0.0
    if event_type == "purchase":
        amount = round(random.uniform(15.0, 450.0), 2)
    elif event_type == "add_to_cart":
        amount = round(random.uniform(5.0, 150.0), 2)

    page = random.choice(PAGES)
    if event_type == "purchase":
        page = "/checkout"
    elif event_type == "cart" or event_type == "add_to_cart":
        page = "/cart"

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "page": page,
        "amount": amount,
        "timestamp": event_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "event_epoch_ms": int(event_time_epoch * 1000),
        "sent_epoch_ms": int(now * 1000),
    }


def run_producer(
    bootstrap_servers: str,
    topic: str,
    rate: float,
    total_events: int,
    num_users: int,
    out_of_order_ratio: float,
    max_delay_sec: float,
):
    """
    Runs the event emission loop.
    """
    logger.info(
        f"Starting producer -> bootstrap: {bootstrap_servers}, topic: {topic}, "
        f"rate: {rate} msg/s, users: {num_users}, out-of-order ratio: {out_of_order_ratio * 100:.1f}%"
    )

    producer = create_kafka_producer(bootstrap_servers)
    users = [f"user_{i}" for i in range(1, num_users + 1)]

    count = 0
    start_time = time.time()
    last_log_time = start_time

    interval = 1.0 / rate if rate > 0 else 0.0

    try:
        while True:
            if total_events > 0 and count >= total_events:
                break

            user_id = random.choice(users)

            # Determine if this message is delayed (out-of-order)
            delay = 0.0
            if out_of_order_ratio > 0 and random.random() < out_of_order_ratio:
                delay = random.uniform(1.0, max_delay_sec)

            event = generate_event(user_id=user_id, out_of_order_delay_sec=delay)
            payload = json.dumps(event)

            producer.send(topic=topic, key=user_id, value=payload)
            count += 1

            now = time.time()
            if now - last_log_time >= 5.0:
                elapsed = now - start_time
                avg_rate = count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Produced {count} events ({avg_rate:.1f} msg/s). Last event: {event['user_id']} {event['event_type']} @ {event['timestamp']}"
                )
                last_log_time = now

            if interval > 0:
                # Basic throttle
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        logger.info(f"Flushing producer... Total events sent: {count}")
        producer.flush()
        elapsed = time.time() - start_time
        logger.info(
            f"Producer stopped. Sent {count} messages in {elapsed:.2f}s "
            f"({count / elapsed if elapsed > 0 else 0:.1f} msg/s average)."
        )


def main():
    parser = argparse.ArgumentParser(description="Kafka User Events Stream Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="user-events",
        help="Kafka destination topic (default: user-events)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Events per second. Set 0 for max speed (default: 20)",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=0,
        help="Total events to send (0 = infinite, default: 0)",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=25,
        help="Number of simulated users (default: 25)",
    )
    parser.add_argument(
        "--out-of-order-ratio",
        type=float,
        default=0.15,
        help="Ratio of late/out-of-order events [0.0 - 1.0] (default: 0.15)",
    )
    parser.add_argument(
        "--max-delay-sec",
        type=float,
        default=15.0,
        help="Maximum out-of-order delay in seconds (default: 15.0)",
    )

    args = parser.parse_args()
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate=args.rate,
        total_events=args.total,
        num_users=args.num_users,
        out_of_order_ratio=args.out_of_order_ratio,
        max_delay_sec=args.max_delay_sec,
    )


if __name__ == "__main__":
    main()
