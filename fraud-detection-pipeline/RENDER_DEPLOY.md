# Deploy the fraud detection pipeline on Render

This deployment keeps Kafka, Spark Structured Streaming, PostgreSQL, and
Grafana in the cloud. The model is trained locally and bundled into the
producer and Spark images because Render disks are attached to individual
services, not shared between them.

## Prepare the model bundle

Download the PaySim `Fraud.csv` file into `data/Fraud.csv`, then run:

```bash
bash scripts/prepare_render_model.sh
```

Commit the refreshed `render-model/` files. Do not commit `data/Fraud.csv`,
`.env`, or passwords. The model bundle contains the trained model and demo
feature samples, not the original training dataset.

## Create the Render deployment

1. Push the project to a GitHub repository, including the root-level
   `render.yaml` and the `fraud-detection-pipeline/render-model/` files.
2. In Render, create a **Blueprint** from that repository and select
   `render.yaml` at the repository root.
3. Enter a strong unique value for `GF_SECURITY_ADMIN_PASSWORD` when prompted.
4. Wait for Kafka, PostgreSQL, the producer, Spark, and Grafana to finish
   deploying. The producer and Spark wait for their TCP dependencies at startup;
   Spark's Docker image supplies its `spark-submit` command and creates the
   database tables when it connects.
5. Open the public `fraud-grafana` service URL. Sign in as `admin` with the
   password entered during Blueprint creation.

The public Grafana `onrender.com` URL is the shareable project link. Keep it
public only if you are comfortable with the dashboard being accessible to
anyone who has the link; use a strong password and do not expose PostgreSQL
or Kafka publicly.

This is a multi-service, always-on deployment with persistent Kafka and Spark
disks plus a managed PostgreSQL database. Render charges for the selected
resources; this stack is not suitable for a free-only deployment. The model is
synthetic PaySim demo data and should not be used for real financial decisions.
