# Real-Time Fraud Detection Pipeline

A local demonstration pipeline that generates transaction events, publishes
them to Kafka, processes them with Spark Structured Streaming, persists
transactions and fraud alerts in PostgreSQL, and visualizes the results in
Grafana.

## Architecture

```text
Python generator -> Kafka topic `transactions` -> Spark Structured Streaming
                                                  |-> PostgreSQL `transactions`
                                                  `-> PostgreSQL `fraud_alerts`
                                                            |
                                                        Grafana
```

The generator sends ordinary traffic plus intentionally suspicious examples.
Spark records every valid transaction and sets `is_high_amount` when the amount
is at least USD 5,000. It also emits event-time window alerts for:

- **Transaction velocity:** at least four transactions for one customer in a
  five-minute sliding window (window advances every minute).
- **Multiple locations:** transactions for one customer from more than one
  location in the same window.

The streaming aggregations use a one-minute event-time watermark by default.
Window alerts are therefore finalized after the watermark advances, rather
than immediately when the first matching event arrives. PostgreSQL writes use
stable transaction and alert IDs with `ON CONFLICT` upserts so microbatch
retries do not duplicate records.

## Run locally

Requirements: Docker Engine and the Docker Compose plugin.

```bash
cd "/home/gauri/fraud detection/fraud-detection-pipeline"
cp .env.example .env
docker compose up --build
```

The first Spark startup downloads the Spark Kafka connector and PostgreSQL JDBC
driver, so it needs internet access. The first run also builds the producer and
Spark images and may take several minutes. Check service health and logs:

```bash
docker compose ps
docker compose logs -f producer spark
```

The producer continuously sends normal and intentionally suspicious sample
transactions. Spark consumes them from the beginning of the Kafka topic and
stores transactions and windowed alerts in PostgreSQL. Alerts are finalized
after the one-minute watermark advances, so allow a couple of minutes for
windowed alerts to appear.

Open the provisioned dashboard at [http://localhost:3000](http://localhost:3000)
(Grafana's default local login is `admin` / `admin`; Grafana prompts to change
the password). Kafka is exposed at `localhost:9092`, and PostgreSQL at
`localhost:5432` (`fraud` / `local-dev-only`).

You can inspect stored data directly from another terminal:

```bash
docker compose exec -T postgres psql -U fraud -d fraud -c \
  "SELECT COUNT(*) AS transactions, COUNT(*) FILTER (WHERE is_high_amount) AS high_value FROM transactions;"
docker compose exec -T postgres psql -U fraud -d fraud -c \
  "SELECT alert_type, customer_id, window_start, metric_value FROM fraud_alerts ORDER BY window_start DESC LIMIT 10;"
```

Stop the stack with `docker compose down`. To also remove persisted PostgreSQL
data and Spark checkpoints, run `docker compose down -v`.

## Configure thresholds

Copy `.env.example` to `.env` and adjust the values:

- `HIGH_AMOUNT_THRESHOLD`: per-transaction amount threshold, default `5000`.
- `VELOCITY_COUNT_THRESHOLD`: minimum window transaction count, default `4`.
- `FRAUD_WINDOW`: event-time window size, default `5 minutes`.
- `FRAUD_WINDOW_SLIDE`: how often windows advance, default `1 minute`.
- `WATERMARK_DELAY`: allowed lateness, default `1 minute`.

These settings are for demonstration. The sample event generator includes
USD 7,500 examples; `producer/rules.py` documents the default USD 5,000 rule.
The stream applies the configurable threshold from `.env`.

## Tests

The focused unit tests cover the high-amount rule:

```bash
cd "/home/gauri/fraud detection/fraud-detection-pipeline"
python -m unittest discover -s tests -v
```

## Notes before production use

- The Compose credentials are deliberately simple local-demo values. Use
  secrets management, authentication, TLS, and network restrictions for any
  non-local deployment.
- The pipeline is a starter, not a financial compliance system. Tune thresholds
  using labeled data, add customer/account context, monitor false positives,
  and establish an audited case-review process before real decisions are made.
- Kafka runs as a single local broker with plaintext transport; PostgreSQL and
  Spark are configured for local development rather than high availability.
- Spark filters malformed records before persistence and logs their counts.
  Add a dead-letter topic and operational metrics before relying on untrusted
  production input.
