#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f data/Fraud.csv ]]; then
  echo "Missing data/Fraud.csv. Add the PaySim dataset before preparing the Render model." >&2
  exit 1
fi

mkdir -p render-model
docker compose run --build --rm -e FORCE_RETRAIN=1 model-trainer
docker compose run --rm \
  -v "$ROOT_DIR/render-model:/export" \
  --entrypoint /bin/sh model-trainer \
  -c 'cp /models/fraud-model.joblib /models/demo-normal.csv /models/demo-fraud.csv /models/training-metrics.json /export/'
echo "Updated deployment artifacts in render-model/."
