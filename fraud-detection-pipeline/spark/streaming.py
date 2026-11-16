from __future__ import annotations

import logging
import os
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "local-dev-only"),
}
HIGH_AMOUNT_THRESHOLD = float(os.getenv("HIGH_AMOUNT_THRESHOLD", "5000"))
VELOCITY_COUNT_THRESHOLD = int(os.getenv("VELOCITY_COUNT_THRESHOLD", "4"))
FRAUD_WINDOW = os.getenv("FRAUD_WINDOW", "5 minutes")
FRAUD_WINDOW_SLIDE = os.getenv("FRAUD_WINDOW_SLIDE", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")

TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("merchant", T.StringType()),
        T.StructField("event_time", T.StringType()),
    ]
)


def postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def write_partitions(
    rows: Iterable,
    statement: str,
    columns: int,
) -> None:
    connection = postgres_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    statement,
                    (tuple(row) for row in rows),
                    template="(" + ",".join(["%s"] * columns) + ")",
                    page_size=500,
                )
    finally:
        connection.close()


TRANSACTION_UPSERT = """
    INSERT INTO transactions (
        transaction_id, customer_id, amount, currency, location, merchant,
        event_time, is_high_amount
    ) VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        location = EXCLUDED.location,
        merchant = EXCLUDED.merchant,
        event_time = EXCLUDED.event_time,
        is_high_amount = EXCLUDED.is_high_amount
"""

ALERT_UPSERT = """
    INSERT INTO fraud_alerts (
        alert_id, alert_type, customer_id, window_start, window_end,
        metric_value, details
    ) VALUES %s
    ON CONFLICT (alert_id) DO UPDATE SET
        metric_value = EXCLUDED.metric_value,
        details = EXCLUDED.details
"""


def write_transactions(batch: DataFrame, batch_id: int) -> None:
    output = (
        batch.select(
            "transaction_id",
            "customer_id",
            "amount",
            "currency",
            "location",
            "merchant",
            "event_time",
            "is_high_amount",
        )
        .dropDuplicates(["transaction_id"])
    )
    output.foreachPartition(
        lambda rows: write_partitions(rows, TRANSACTION_UPSERT, 8)
    )
    logger.info("Upserted transaction microbatch %s", batch_id)


def write_alerts(batch: DataFrame, batch_id: int) -> None:
    output = batch.select(
        "alert_id",
        "alert_type",
        "customer_id",
        "window_start",
        "window_end",
        "metric_value",
        "details",
    ).dropDuplicates(["alert_id"])
    output.foreachPartition(lambda rows: write_partitions(rows, ALERT_UPSERT, 7))
    logger.info("Upserted fraud-alert microbatch %s", batch_id)


def log_invalid_events(batch: DataFrame, batch_id: int) -> None:
    invalid_count = batch.count()
    if invalid_count:
        logger.error(
            "Dropped %s malformed transaction event(s) in Kafka microbatch %s",
            invalid_count,
            batch_id,
        )


def add_high_amount_flag(transactions: DataFrame) -> DataFrame:
    return transactions.withColumn(
        "is_high_amount", F.col("amount") >= HIGH_AMOUNT_THRESHOLD
    )


def build_alerts(transactions: DataFrame) -> DataFrame:
    windowed = F.window(F.col("event_time"), FRAUD_WINDOW, FRAUD_WINDOW_SLIDE)
    metrics = (
        transactions.groupBy(windowed.alias("window"), "customer_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.collect_set("location").alias("locations"),
        )
    )
    candidates = metrics.select(
        "window",
        "customer_id",
        F.explode(
            F.array(
                F.when(
                    F.col("transaction_count") >= VELOCITY_COUNT_THRESHOLD,
                    F.struct(
                        F.lit("transaction_velocity").alias("alert_type"),
                        F.col("transaction_count").alias("metric_value"),
                        F.to_json(
                            F.struct(
                                F.col("transaction_count").alias(
                                    "transactions_in_window"
                                )
                            )
                        ).alias("details"),
                    ),
                ),
                F.when(
                    F.size("locations") > 1,
                    F.struct(
                        F.lit("multiple_locations").alias("alert_type"),
                        F.size("locations").alias("metric_value"),
                        F.to_json(
                            F.struct(
                                F.col("locations"),
                                F.size("locations").alias("location_count"),
                            )
                        ).alias("details"),
                    ),
                ),
            )
        ).alias("alert"),
    ).filter(F.col("alert").isNotNull())

    return (
        candidates.select(
            "window",
            "customer_id",
            "alert.alert_type",
            "alert.metric_value",
            "alert.details",
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn(
            "alert_id",
            F.sha2(
                F.concat_ws(
                    ":",
                    "alert_type",
                    "customer_id",
                    F.col("window_start").cast("string"),
                    F.col("window_end").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "alert_id",
            "alert_type",
            "customer_id",
            "window_start",
            "window_end",
            "metric_value",
            "details",
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("real-time-fraud-detection")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw_events.select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("event")
    ).select("event.*")
    parsed = parsed.withColumn("event_time", F.to_timestamp("event_time"))
    valid_event = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("location").isNotNull()
        & F.col("event_time").isNotNull()
    )
    transactions = (
        add_high_amount_flag(parsed.filter(valid_event)).withWatermark(
            "event_time", WATERMARK_DELAY
        )
    )

    invalid_query = (
        parsed.filter(~valid_event)
        .writeStream.outputMode("append")
        .foreachBatch(log_invalid_events)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/invalid-events")
        .start()
    )
    transaction_query = (
        transactions.writeStream.outputMode("append")
        .foreachBatch(write_transactions)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/transactions")
        .start()
    )

    alert_query = (
        build_alerts(transactions)
        .writeStream.outputMode("append")
        .foreachBatch(write_alerts)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts")
        .start()
    )
    logger.info("Streaming %s from Kafka at %s", TOPIC, BOOTSTRAP_SERVERS)
    _ = invalid_query
    _ = transaction_query
    _ = alert_query
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
