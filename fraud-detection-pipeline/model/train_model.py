from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

MODEL_VERSION = "paysim-gradient-boosting-v3"
TARGET_COLUMN = "isFraud"
CATEGORICAL_FEATURES = ["type"]
NUMERIC_FEATURES = [
    "destination_is_merchant",
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
]
FEATURE_COLUMNS = CATEGORICAL_FEATURES + NUMERIC_FEATURES
REQUIRED_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFlaggedFraud",
    TARGET_COLUMN,
]


def collect_samples(
    csv_path: Path,
    train_limit_per_class: int,
    validation_limit_per_class: int,
    validation_fraction: float,
    random_seed: int,
) -> tuple[dict[str, dict[int, pd.DataFrame]], dict[str, dict[int, int]]]:
    rng = np.random.default_rng(random_seed)
    sample_limits = {
        "train": train_limit_per_class,
        "validation": validation_limit_per_class,
        "test": validation_limit_per_class,
    }
    samples = {
        split: {label: pd.DataFrame() for label in (0, 1)}
        for split in sample_limits
    }
    population_counts = {
        split: {label: 0 for label in (0, 1)} for split in sample_limits
    }

    for chunk in pd.read_csv(csv_path, usecols=REQUIRED_COLUMNS, chunksize=200_000):
        if chunk[REQUIRED_COLUMNS].isnull().any().any():
            raise ValueError("Training data contains missing feature or target values")

        labels = pd.to_numeric(chunk[TARGET_COLUMN], errors="raise").astype(int)
        if not labels.isin([0, 1]).all():
            raise ValueError("isFraud must contain only 0 and 1")
        chunk["destination_is_merchant"] = (
            chunk["nameDest"].astype(str).str.startswith("M").astype(int)
        )
        split_values = rng.random(len(chunk))

        for split, split_mask in (
            ("train", split_values < 0.7),
            (
                "validation",
                (split_values >= 0.7) & (split_values < 0.7 + validation_fraction),
            ),
            ("test", split_values >= 0.7 + validation_fraction),
        ):
            split_chunk = chunk.loc[split_mask, FEATURE_COLUMNS].copy()
            split_labels = labels.loc[split_mask]
            for label in (0, 1):
                class_rows = split_chunk.loc[split_labels == label].copy()
                population_counts[split][label] += len(class_rows)
                if class_rows.empty:
                    continue
                class_rows["_sample_key"] = rng.random(len(class_rows))
                candidates = pd.concat(
                    [samples[split][label], class_rows],
                    ignore_index=True,
                )
                samples[split][label] = (
                    candidates.nsmallest(sample_limits[split], "_sample_key")
                    .drop(columns="_sample_key")
                    .reset_index(drop=True)
                )

    for split in sample_limits:
        if not samples[split][0].empty and not samples[split][1].empty:
            continue
        raise ValueError(f"Dataset does not contain both classes in {split} split")

    return samples, population_counts


def balanced_frame(
    class_samples: dict[int, pd.DataFrame],
    random_seed: int,
) -> tuple[pd.DataFrame, pd.Series]:
    rows_per_class = min(len(class_samples[0]), len(class_samples[1]))
    frames = []
    for label in (0, 1):
        frame = class_samples[label].sample(
            n=rows_per_class,
            random_state=random_seed + label,
        ).copy()
        frame[TARGET_COLUMN] = label
        frames.append(frame)
    combined = pd.concat(frames, ignore_index=True)
    return combined[FEATURE_COLUMNS], combined[TARGET_COLUMN]


def corrected_probabilities(
    balanced_probabilities: np.ndarray,
    fraud_prior: float,
) -> np.ndarray:
    numerator = balanced_probabilities * fraud_prior
    denominator = numerator + (1.0 - balanced_probabilities) * (1.0 - fraud_prior)
    return np.divide(
        numerator,
        denominator,
        out=np.zeros_like(numerator),
        where=denominator != 0,
    )


def train_model(
    csv_path: Path,
    output_path: Path,
    max_rows_per_class: int,
    random_seed: int = 42,
) -> dict[str, float | int | str]:
    samples, population_counts = collect_samples(
        csv_path=csv_path,
        train_limit_per_class=max_rows_per_class,
        validation_limit_per_class=max_rows_per_class,
        validation_fraction=0.15,
        random_seed=random_seed,
    )
    train_x, train_y = balanced_frame(samples["train"], random_seed)
    validation_x, validation_y = balanced_frame(samples["validation"], random_seed)
    test_x, test_y = balanced_frame(samples["test"], random_seed)

    preprocessor = ColumnTransformer(
        transformers=[
            ("transaction_type", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ]
    )
    pipeline = Pipeline(
        steps=[
            ("features", preprocessor),
            (
                "classifier",
                GradientBoostingClassifier(
                    n_estimators=100,
                    learning_rate=0.1,
                    max_depth=3,
                    random_state=random_seed,
                ),
            ),
        ]
    )
    pipeline.fit(train_x, train_y)

    train_population = population_counts["train"]
    fraud_prior = train_population[1] / sum(train_population.values())
    validation_probabilities = corrected_probabilities(
        pipeline.predict_proba(validation_x)[:, 1],
        fraud_prior,
    )
    validation_counts = population_counts["validation"]
    validation_weights = np.asarray(
        [
            validation_counts[int(label)] / int((validation_y == label).sum())
            for label in validation_y
        ],
        dtype=float,
    )

    precisions, recalls, thresholds = precision_recall_curve(
        validation_y,
        validation_probabilities,
        sample_weight=validation_weights,
    )
    f1_values = 2 * precisions[:-1] * recalls[:-1] / (
        precisions[:-1] + recalls[:-1] + 1e-12
    )
    threshold = float(thresholds[int(np.nanargmax(f1_values))])
    test_probabilities = corrected_probabilities(
        pipeline.predict_proba(test_x)[:, 1],
        fraud_prior,
    )
    test_counts = population_counts["test"]
    test_weights = np.asarray(
        [
            test_counts[int(label)] / int((test_y == label).sum())
            for label in test_y
        ],
        dtype=float,
    )
    predictions = test_probabilities >= threshold
    metrics = {
        "test_average_precision": float(
            average_precision_score(
                test_y,
                test_probabilities,
                sample_weight=test_weights,
            )
        ),
        "test_roc_auc": float(
            roc_auc_score(
                test_y,
                test_probabilities,
                sample_weight=test_weights,
            )
        ),
        "test_precision": float(
            precision_score(
                test_y,
                predictions,
                sample_weight=test_weights,
                zero_division=0,
            )
        ),
        "test_recall": float(
            recall_score(
                test_y,
                predictions,
                sample_weight=test_weights,
                zero_division=0,
            )
        ),
        "test_f1": float(
            f1_score(
                test_y,
                predictions,
                sample_weight=test_weights,
                zero_division=0,
            )
        ),
    }
    artifact = {
        "model_version": MODEL_VERSION,
        "pipeline": pipeline,
        "feature_columns": FEATURE_COLUMNS,
        "fraud_prior": fraud_prior,
        "threshold": threshold,
        "threshold_selection_split": "validation",
        "metrics": metrics,
        "population_counts": population_counts,
        "training_rows_per_class": len(train_x) // 2,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    joblib.dump(artifact, temporary_path)
    os.replace(temporary_path, output_path)
    metrics_path = output_path.with_name("training-metrics.json")
    metrics_path.write_text(
        json.dumps(
            {key: value for key, value in artifact.items() if key != "pipeline"},
            indent=2,
        )
    )
    for label, sample_name in (
        (0, "demo-normal.csv"),
        (1, "demo-fraud.csv"),
    ):
        demo_rows = test_x.loc[test_y == label]
        demo_rows.to_csv(output_path.with_name(sample_name), index=False)

    print(f"Saved {MODEL_VERSION} to {output_path}")
    print(f"Fraud prior: {fraud_prior:.6%}")
    print(f"Prediction threshold (validation F1): {threshold:.8f}")
    print(f"Training rows per class: {len(train_x) // 2}")
    print(f"Weighted untouched-test metrics: {json.dumps(metrics, sort_keys=True)}")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the PaySim fraud classifier")
    parser.add_argument("--input", type=Path, default=Path("/data/Fraud.csv"))
    parser.add_argument("--output", type=Path, default=Path("/models/fraud-model.joblib"))
    parser.add_argument(
        "--max-rows-per-class",
        type=int,
        default=int(os.getenv("MODEL_MAX_ROWS_PER_CLASS", "20000")),
    )
    args = parser.parse_args()

    if not args.input.is_file():
        raise FileNotFoundError(
            f"Training data not found: {args.input}. Download Fraud.csv from "
            "https://www.kaggle.com/datasets/vardhansiramdasu/fraudulent-transactions-prediction"
        )
    if args.max_rows_per_class < 1:
        raise ValueError("--max-rows-per-class must be greater than zero")
    demo_samples_exist = all(
        args.output.with_name(name).is_file()
        for name in ("demo-normal.csv", "demo-fraud.csv")
    )
    if (
        args.output.exists()
        and demo_samples_exist
        and os.getenv("FORCE_RETRAIN") != "1"
    ):
        existing_artifact = joblib.load(args.output)
        if existing_artifact.get("model_version") == MODEL_VERSION:
            print(f"Using existing model artifact: {args.output}")
            return
        print(
            "Existing model version differs from "
            f"{MODEL_VERSION}; retraining from {args.input}"
        )
    train_model(args.input, args.output, args.max_rows_per_class)


if __name__ == "__main__":
    main()
