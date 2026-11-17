from __future__ import annotations

import csv
import json
import logging
import os
import random
import socket
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

def kafka_bootstrap_servers() -> str:
    host = os.getenv("KAFKA_BOOTSTRAP_HOST")
    if host:
        return f"{host}:{os.getenv('KAFKA_BOOTSTRAP_PORT', '9092')}"
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


BOOTSTRAP_SERVERS = kafka_bootstrap_servers()
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
DEMO_SAMPLE_DIR = Path(os.getenv("DEMO_SAMPLE_DIR", "/models"))
DEMO_FRAUD_RATE = float(os.getenv("DEMO_FRAUD_RATE", "0.05"))
LOCATIONS = ["New York", "London", "Singapore", "Toronto", "Berlin"]
MERCHANTS = ["grocery", "electronics", "travel", "restaurant", "fuel"]
def load_demo_samples() -> dict[str, list[dict[str, str]]]:
    samples = {}
    for scenario in ("normal", "fraud"):
        sample_path = DEMO_SAMPLE_DIR / f"demo-{scenario}.csv"
        with sample_path.open(newline="") as sample_file:
            samples[scenario] = list(csv.DictReader(sample_file))
        if not samples[scenario]:
            raise ValueError(f"No {scenario} examples found in {sample_path}")
    if not 0 <= DEMO_FRAUD_RATE <= 1:
        raise ValueError("DEMO_FRAUD_RATE must be between 0 and 1")
    return samples


def make_transaction(
    sample: dict[str, str],
    event_time: datetime | None = None,
) -> dict[str, str | float | int | bool]:
    timestamp = event_time or datetime.now(timezone.utc)
    customer_id = f"C{uuid.uuid4().hex[:10].upper()}"
    merchant_destination = bool(int(sample["destination_is_merchant"]))
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "amount": float(sample["amount"]),
        "currency": "SIM",
        "location": random.choice(LOCATIONS),
        "merchant": random.choice(MERCHANTS),
        "event_time": timestamp.isoformat(),
        "step": int(sample["step"]),
        "type": sample["type"],
        "nameOrig": customer_id,
        "oldbalanceOrg": float(sample["oldbalanceOrg"]),
        "newbalanceOrig": float(sample["newbalanceOrig"]),
        "nameDest": (
            f"M{uuid.uuid4().hex[:10].upper()}"
            if merchant_destination
            else f"C{uuid.uuid4().hex[:10].upper()}"
        ),
        "oldbalanceDest": float(sample["oldbalanceDest"]),
        "newbalanceDest": float(sample["newbalanceDest"]),
        "isFlaggedFraud": bool(int(sample["isFlaggedFraud"])),
    }
    return transaction


def generated_cycle(
    samples: dict[str, list[dict[str, str]]],
) -> list[dict[str, str | float | int | bool]]:
    """Stream PaySim-shaped samples without publishing their training labels."""
    now = datetime.now(timezone.utc)
    batch = [
        make_transaction(random.choice(samples["normal"]), event_time=now)
        for _ in range(8)
    ]
    if random.random() < DEMO_FRAUD_RATE:
        batch.append(make_transaction(random.choice(samples["fraud"]), event_time=now))
    return batch


def wait_for_kafka(timeout: int = 300) -> None:
    host, port = BOOTSTRAP_SERVERS.split(",", maxsplit=1)[0].rsplit(":", 1)
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=3):
                return
        except OSError as error:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for Kafka at {host}:{port}"
                ) from error
            time.sleep(2)


def main() -> None:
    samples = load_demo_samples()
    wait_for_kafka()
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
            for transaction in generated_cycle(samples):
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
