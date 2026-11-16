from __future__ import annotations

import logging
import os
from typing import Iterable

import joblib
import pandas as pd
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
MODEL_PATH = os.getenv("MODEL_PATH", "/models/fraud-model.joblib")
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "local-dev-only"),
}

TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("merchant", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("step", T.IntegerType()),
        T.StructField("type", T.StringType()),
        T.StructField("nameOrig", T.StringType()),
        T.StructField("oldbalanceOrg", T.DoubleType()),
        T.StructField("newbalanceOrig", T.DoubleType()),
        T.StructField("nameDest", T.StringType()),
        T.StructField("oldbalanceDest", T.DoubleType()),
        T.StructField("newbalanceDest", T.DoubleType()),
        T.StructField("isFlaggedFraud", T.BooleanType()),
    ]
)

MIGRATE_TRANSACTIONS_SQL = """
    ALTER TABLE transactions
        ADD COLUMN IF NOT EXISTS step INTEGER NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS transaction_type TEXT NOT NULL DEFAULT 'PAYMENT',
        ADD COLUMN IF NOT EXISTS name_orig TEXT NOT NULL DEFAULT '',
        ADD COLUMN IF NOT EXISTS name_dest TEXT NOT NULL DEFAULT '',
        ADD COLUMN IF NOT EXISTS oldbalance_org DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS newbalance_orig DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS oldbalance_dest DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS newbalance_dest DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS is_flagged_fraud BOOLEAN NOT NULL DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS fraud_probability DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS is_fraud_predicted BOOLEAN NOT NULL DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS prediction_threshold DOUBLE PRECISION NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS model_version TEXT NOT NULL DEFAULT 'unknown'
"""


def postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def ensure_database_schema() -> None:
    connection = postgres_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(MIGRATE_TRANSACTIONS_SQL)
    finally:
        connection.close()


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
        event_time, step, transaction_type, name_orig, name_dest,
        oldbalance_org, newbalance_orig, oldbalance_dest, newbalance_dest,
        is_flagged_fraud, fraud_probability, is_fraud_predicted,
        prediction_threshold, model_version
    ) VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        location = EXCLUDED.location,
        merchant = EXCLUDED.merchant,
        event_time = EXCLUDED.event_time,
        step = EXCLUDED.step,
        transaction_type = EXCLUDED.transaction_type,
        name_orig = EXCLUDED.name_orig,
        name_dest = EXCLUDED.name_dest,
        oldbalance_org = EXCLUDED.oldbalance_org,
        newbalance_orig = EXCLUDED.newbalance_orig,
        oldbalance_dest = EXCLUDED.oldbalance_dest,
        newbalance_dest = EXCLUDED.newbalance_dest,
        is_flagged_fraud = EXCLUDED.is_flagged_fraud,
        fraud_probability = EXCLUDED.fraud_probability,
        is_fraud_predicted = EXCLUDED.is_fraud_predicted,
        prediction_threshold = EXCLUDED.prediction_threshold,
        model_version = EXCLUDED.model_version
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
            "step",
            F.col("type").alias("transaction_type"),
            F.col("nameOrig").alias("name_orig"),
            F.col("nameDest").alias("name_dest"),
            F.col("oldbalanceOrg").alias("oldbalance_org"),
            F.col("newbalanceOrig").alias("newbalance_orig"),
            F.col("oldbalanceDest").alias("oldbalance_dest"),
            F.col("newbalanceDest").alias("newbalance_dest"),
            F.col("isFlaggedFraud").alias("is_flagged_fraud"),
            "fraud_probability",
            "is_fraud_predicted",
            "prediction_threshold",
            "model_version",
        )
        .dropDuplicates(["transaction_id"])
    )
    output.foreachPartition(
        lambda rows: write_partitions(rows, TRANSACTION_UPSERT, 20)
    )
    logger.info("Upserted transaction microbatch %s", batch_id)


def write_alerts(batch: DataFrame, batch_id: int) -> None:
    output = (
        batch.select(
            "transaction_id",
            "customer_id",
            "event_time",
            "fraud_probability",
            "prediction_threshold",
            "model_version",
        )
        .dropDuplicates(["transaction_id"])
        .select(
            F.col("transaction_id").alias("alert_id"),
            F.lit("ml_fraud_prediction").alias("alert_type"),
            "customer_id",
            F.col("event_time").alias("window_start"),
            F.col("event_time").alias("window_end"),
            F.round(F.col("fraud_probability") * 10000).cast("integer").alias(
                "metric_value"
            ),
            F.to_json(
                F.struct(
                    "fraud_probability",
                    "prediction_threshold",
                    "model_version",
                )
            ).alias("details"),
        )
    )
    output.foreachPartition(lambda rows: write_partitions(rows, ALERT_UPSERT, 7))
    logger.info("Upserted ML fraud-prediction microbatch %s", batch_id)


def log_invalid_events(batch: DataFrame, batch_id: int) -> None:
    invalid_count = batch.count()
    if invalid_count:
        logger.error(
            "Dropped %s malformed transaction event(s) in Kafka microbatch %s",
            invalid_count,
            batch_id,
        )


def build_probability_udf(spark: SparkSession, artifact: dict):
    model_broadcast = spark.sparkContext.broadcast(artifact)
    feature_columns = artifact["feature_columns"]
    fraud_prior = float(artifact["fraud_prior"])

    @F.udf(returnType=T.DoubleType())
    def predict_probability(
        transaction_type,
        name_dest,
        step,
        amount,
        oldbalance_org,
        newbalance_orig,
        oldbalance_dest,
        newbalance_dest,
        is_flagged_fraud,
    ):
        if any(
            value is None
            for value in (
                transaction_type,
                name_dest,
                step,
                amount,
                oldbalance_org,
                newbalance_orig,
                oldbalance_dest,
                newbalance_dest,
                is_flagged_fraud,
            )
        ):
            return None

        model_features = pd.DataFrame(
            [
                {
                    "type": transaction_type,
                    "destination_is_merchant": int(name_dest.startswith("M")),
                    "step": step,
                    "amount": amount,
                    "oldbalanceOrg": oldbalance_org,
                    "newbalanceOrig": newbalance_orig,
                    "oldbalanceDest": oldbalance_dest,
                    "newbalanceDest": newbalance_dest,
                    "isFlaggedFraud": int(is_flagged_fraud),
                }
            ],
            columns=feature_columns,
        )
        balanced_probability = float(
            model_broadcast.value["pipeline"].predict_proba(model_features)[0, 1]
        )
        numerator = balanced_probability * fraud_prior
        denominator = numerator + (1.0 - balanced_probability) * (1.0 - fraud_prior)
        if denominator == 0:
            return 0.0
        return numerator / denominator

    return predict_probability


def main() -> None:
    if not os.path.isfile(MODEL_PATH):
        raise FileNotFoundError(
            f"Trained model not found at {MODEL_PATH}; model-trainer must complete first"
        )
    artifact = joblib.load(MODEL_PATH)
    model_version = str(artifact["model_version"])
    stored_threshold = float(artifact["threshold"])
    threshold_setting = os.getenv("FRAUD_PROBABILITY_THRESHOLD", "").strip()
    prediction_threshold = (
        float(threshold_setting) if threshold_setting else stored_threshold
    )
    if not 0 <= prediction_threshold <= 1:
        raise ValueError("FRAUD_PROBABILITY_THRESHOLD must be between 0 and 1")

    spark = (
        SparkSession.builder.appName("real-time-fraud-detection")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    ensure_database_schema()
    probability_udf = build_probability_udf(spark, artifact)

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
    valid_event = F.coalesce(
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("location").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("step").isNotNull()
        & F.col("type").isNotNull()
        & F.col("nameOrig").isNotNull()
        & F.col("oldbalanceOrg").isNotNull()
        & F.col("newbalanceOrig").isNotNull()
        & F.col("nameDest").isNotNull()
        & F.col("oldbalanceDest").isNotNull()
        & F.col("newbalanceDest").isNotNull()
        & F.col("isFlaggedFraud").isNotNull(),
        F.lit(False),
    )
    valid_events = parsed.filter(valid_event)
    scored = (
        valid_events.withColumn(
            "fraud_probability",
            probability_udf(
                "type",
                "nameDest",
                "step",
                "amount",
                "oldbalanceOrg",
                "newbalanceOrig",
                "oldbalanceDest",
                "newbalanceDest",
                "isFlaggedFraud",
            ),
        )
        .withColumn("prediction_threshold", F.lit(prediction_threshold))
        .withColumn(
            "is_fraud_predicted",
            F.col("fraud_probability") >= F.col("prediction_threshold"),
        )
        .withColumn("model_version", F.lit(model_version))
    )

    invalid_query = (
        parsed.filter(~valid_event)
        .writeStream.outputMode("append")
        .foreachBatch(log_invalid_events)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/ml-invalid-events")
        .start()
    )
    transaction_query = (
        scored.writeStream.outputMode("append")
        .foreachBatch(write_transactions)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/ml-transactions")
        .start()
    )
    alert_query = (
        scored.filter(F.col("is_fraud_predicted"))
        .writeStream.outputMode("append")
        .foreachBatch(write_alerts)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/ml-alerts")
        .start()
    )
    logger.info(
        "Streaming %s using model %s with fraud threshold %.8f",
        TOPIC,
        model_version,
        prediction_threshold,
    )
    _ = invalid_query
    _ = transaction_query
    _ = alert_query
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
