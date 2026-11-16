from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
CUSTOMERS = [f"customer-{number:03d}" for number in range(1, 21)]
LOCATIONS = ["New York", "London", "Singapore", "Toronto", "Berlin"]
MERCHANTS = ["grocery", "electronics", "travel", "restaurant", "fuel"]


def make_transaction(
    customer_id: str | None = None,
    amount: Decimal | None = None,
    location: str | None = None,
    event_time: datetime | None = None,
) -> dict[str, str | float]:
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id or random.choice(CUSTOMERS),
        "amount": float(amount if amount is not None else Decimal(random.randint(5, 1200))),
        "currency": "USD",
        "location": location or random.choice(LOCATIONS),
        "merchant": random.choice(MERCHANTS),
        "event_time": (event_time or datetime.now(timezone.utc)).isoformat(),
    }
    return transaction


def generated_cycle() -> list[dict[str, str | float]]:
    """Generate normal traffic and repeatable velocity/location/high-value examples."""
    now = datetime.now(timezone.utc)
    batch = [make_transaction(event_time=now) for _ in range(8)]
    suspicious_customer = f"customer-{random.randint(1, 20):03d}"
    for offset, city in enumerate(LOCATIONS[:4]):
        batch.append(
            make_transaction(
                customer_id=suspicious_customer,
                amount=Decimal("125.00"),
                location=city,
                event_time=now + timedelta(seconds=offset),
            )
        )
    batch.append(
        make_transaction(
            customer_id=suspicious_customer,
            amount=Decimal("7500.00"),
            location=LOCATIONS[0],
            event_time=now + timedelta(seconds=5),
        )
    )
    return batch


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )
    logger.info("Publishing transaction events to %s at %s", TOPIC, BOOTSTRAP_SERVERS)
    try:
        while True:
            for transaction in generated_cycle():
                try:
                    producer.send(
                        TOPIC,
                        key=str(transaction["customer_id"]),
                        value=transaction,
                    ).get(timeout=15)
                except KafkaError:
                    logger.exception(
                        "Could not publish transaction %s",
                        transaction["transaction_id"],
                    )
                    raise
            producer.flush()
            time.sleep(INTERVAL_SECONDS)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
