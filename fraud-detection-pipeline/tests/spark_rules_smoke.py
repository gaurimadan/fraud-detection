import sys
import time
from datetime import datetime

sys.path.insert(0, "/app")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import streaming
from streaming import add_high_amount_flag, build_alerts


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("fraud-rules-smoke-test")
        .getOrCreate()
    )
    schema = StructType(
        [
            StructField("customer_id", StringType()),
            StructField("location", StringType()),
            StructField("event_time", TimestampType()),
            StructField("amount", DoubleType()),
        ]
    )
    events = [
        ("customer-001", location, datetime(2026, 1, 1, 0, 0, second), amount)
        for second, (location, amount) in enumerate(
            [
                ("New York", 4999.99),
                ("London", 5000.00),
                ("New York", 7500.00),
                ("New York", 100.00),
            ]
        )
    ]

    try:
        transactions = add_high_amount_flag(spark.createDataFrame(events, schema))
        flags = {row.amount: row.is_high_amount for row in transactions.collect()}
        expected_flags = {4999.99: False, 5000.0: True, 7500.0: True, 100.0: False}
        if flags != expected_flags:
            raise AssertionError(f"Expected high-amount flags {expected_flags}, got {flags}")

        alerts = build_alerts(transactions).collect()
        observed = {(row.alert_type, row.metric_value) for row in alerts}
        expected = {("transaction_velocity", 4), ("multiple_locations", 2)}
        if observed != expected:
            raise AssertionError(f"Expected {expected}, got {observed}")

        streaming.FRAUD_WINDOW = "10 seconds"
        streaming.FRAUD_WINDOW_SLIDE = "5 seconds"
        streaming.WATERMARK_DELAY = "2 seconds"
        rate = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        stream_events = rate.select(
            F.lit("stream-customer").alias("customer_id"),
            F.when(F.col("value") % 2 == 0, "New York")
            .otherwise("London")
            .alias("location"),
            F.col("timestamp").alias("event_time"),
        ).withWatermark("event_time", streaming.WATERMARK_DELAY)
        query = (
            build_alerts(stream_events)
            .writeStream.format("memory")
            .queryName("fraud_alerts_smoke")
            .outputMode("append")
            .trigger(processingTime="2 seconds")
            .start()
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                streaming_alert_types = {
                    row.alert_type
                    for row in spark.table("fraud_alerts_smoke")
                    .select("alert_type")
                    .distinct()
                    .collect()
                }
                if {
                    "transaction_velocity",
                    "multiple_locations",
                }.issubset(streaming_alert_types) and not query.status[
                    "isTriggerActive"
                ]:
                    break
                time.sleep(0.25)
            else:
                raise AssertionError(
                    "Timed out waiting for both streaming fraud alert types"
                )
        finally:
            query.stop()

        print("Spark fraud-rule and Structured Streaming smoke tests passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
