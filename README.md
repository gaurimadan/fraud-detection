# Real-Time Fraud Detection Pipeline

A local demonstration pipeline that trains a gradient-boosting fraud classifier
on the PaySim dataset, generates PaySim-shaped transaction events, scores them
with Spark Structured Streaming, persists predictions in PostgreSQL, and
visualizes the results in Grafana.

## Architecture

```text
PaySim Fraud.csv
      |
      v
model-trainer -> persisted GradientBoosting + preprocessing pipeline
                                      |
Python producer -> Kafka topic -> Spark loads model and scores events
                                      |-> PostgreSQL transactions + predictions
                                      `-> Grafana dashboard
```

The model follows the notebook's `GradientBoostingClassifier` approach. Its
features are transaction type, step, amount, origin/destination balances, and
the dataset's existing `isFlaggedFraud` signal, plus whether the destination
is a merchant (derived from its `M`/`C` prefix). Account IDs themselves are left
out because label-encoding them does not generalize to unseen accounts. The
trainer splits the data before class balancing, uses a bounded balanced sample,
does not scale features for a tree model, corrects the balanced model scores for
PaySim's fraud prevalence, and selects a probability threshold on a held-out
validation sample. It reports prevalence-weighted metrics on a separate test
split. The threshold and test metrics are saved with the model.

The producer samples feature rows from the held-out PaySim test set,
refreshes transaction IDs and timestamps, and never publishes their labels.
`DEMO_FRAUD_RATE` is the chance per producer batch of selecting one
fraud-shaped validation example (default `0.05`, roughly 0.6% of generated
events). This is intentional demo oversampling, not a representation of real
fraud prevalence.
Spark writes the model's probability and decision for each event. Predictions
above the saved threshold become `fraud_alerts` records immediately, rather
than waiting for event-time windows.

## Run locally

Requirements: Docker Engine, Docker Compose v2, and the PaySim `Fraud.csv`
training file. The dataset is not committed to this project. Download
`Fraud.csv` from the [Kaggle dataset page](https://www.kaggle.com/datasets/vardhansiramdasu/fraudulent-transactions-prediction)
and place it at `data/Fraud.csv`. The dataset is about 471 MB extracted.

```bash
cd "/home/gauri/fraud detection/fraud-detection-pipeline"
mkdir -p data
# Download Fraud.csv from Kaggle and save it as data/Fraud.csv.
cp -n .env.example .env
docker compose up --build
```

The first run builds the images, trains and saves the model, and downloads the
Spark Kafka connector and PostgreSQL JDBC driver. Keep internet access enabled
and allow several minutes for startup. The CSV is mounted read-only and
excluded from Docker build contexts. Check service health and logs:

```bash
docker compose ps
docker compose logs -f producer spark
```

The producer continuously sends sample transactions. Spark consumes them from
the beginning of the Kafka topic, stores every scored transaction, and writes
model predictions above the selected threshold to PostgreSQL.

Open the provisioned dashboard at [http://localhost:3000](http://localhost:3000)
(Grafana's default local login is `admin` / `admin`; Grafana prompts to change
the password). Kafka is exposed at `localhost:9092`, and PostgreSQL at
`localhost:5432` (`fraud` / `local-dev-only`).

You can inspect stored data directly from another terminal:

```bash
docker compose exec -T postgres psql -U fraud -d fraud -c \
  "SELECT COUNT(*) AS transactions, COUNT(*) FILTER (WHERE is_fraud_predicted) AS model_predictions FROM transactions;"
docker compose exec -T postgres psql -U fraud -d fraud -c \
  "SELECT customer_id, details->>'fraud_probability' AS probability, details->>'prediction_threshold' AS threshold, window_start FROM fraud_alerts WHERE alert_type = 'ml_fraud_prediction' ORDER BY window_start DESC LIMIT 10;"
```

Stop the stack with `docker compose down`. To also remove persisted PostgreSQL
data, model, and Spark checkpoints, run `docker compose down -v`. Existing
database rows are preserved during the schema migration; the dashboard filters
for new ML prediction records.

## Model training and retraining

`MODEL_MAX_ROWS_PER_CLASS` limits the per-class training and validation samples
(default `20000`). `DEMO_FRAUD_RATE` changes only the demo event selection; it
does not affect training or model scoring. Training is class-balanced only after the train/validation
split; saved probabilities are adjusted to the original fraud prevalence. The
threshold is selected to maximize prevalence-weighted validation F1; final
reported metrics use a separate untouched test split. To override
the saved threshold, set `FRAUD_PROBABILITY_THRESHOLD` in `.env` to a number
between `0` and `1`.

To deliberately retrain with the current CSV and replace the saved model:

```bash
docker compose run --rm -e FORCE_RETRAIN=1 model-trainer
docker compose restart producer spark
```

## Tests

The focused unit tests cover the model's training-sample logic and probability
correction, plus the producer's model-input format. Run them while the stack is
up:

```bash
cd "/home/gauri/fraud detection/fraud-detection-pipeline"
docker compose run --rm model-trainer python3 -m unittest discover -s /app/tests -p 'test_model_training.py' -v
docker compose exec producer python -m unittest discover -s /app/tests -p 'test_producer.py' -v
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
- Spark filters malformed records before scoring and logs their counts.
  Add a dead-letter topic and operational metrics before relying on untrusted
  production input.
