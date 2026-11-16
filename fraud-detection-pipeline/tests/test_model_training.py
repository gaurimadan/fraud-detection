from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "model"))
from train_model import balanced_frame, collect_samples, corrected_probabilities


class ModelTrainingTests(unittest.TestCase):
    def test_balanced_sampling_keeps_both_classes_even(self):
        samples = {
            0: self.feature_frame("PAYMENT", 8),
            1: self.feature_frame("TRANSFER", 3),
        }

        features, labels = balanced_frame(samples, random_seed=42)

        self.assertEqual(len(features), 6)
        self.assertEqual(labels.value_counts().to_dict(), {0: 3, 1: 3})

    @staticmethod
    def feature_frame(transaction_type, row_count):
        return pd.DataFrame(
            {
                "type": [transaction_type] * row_count,
                "destination_is_merchant": [0] * row_count,
                "step": range(row_count),
                "amount": range(row_count),
                "oldbalanceOrg": range(row_count),
                "newbalanceOrig": range(row_count),
                "oldbalanceDest": range(row_count),
                "newbalanceDest": range(row_count),
                "isFlaggedFraud": [0] * row_count,
            }
        )

    def test_balanced_model_scores_are_corrected_for_rare_fraud_prior(self):
        probability = corrected_probabilities(np.asarray([0.5]), fraud_prior=0.01)

        self.assertAlmostEqual(float(probability[0]), 0.01)

    def test_collection_splits_rows_and_retains_each_target_class(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            csv_path = Path(temporary_directory) / "Fraud.csv"
            columns = [
                "step",
                "type",
                "amount",
                "nameDest",
                "oldbalanceOrg",
                "newbalanceOrig",
                "oldbalanceDest",
                "newbalanceDest",
                "isFlaggedFraud",
                "isFraud",
            ]
            with csv_path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                for index in range(100):
                    label = index % 2
                    writer.writerow(
                        {
                            "step": index + 1,
                            "type": "TRANSFER" if label else "PAYMENT",
                            "amount": 100 + index,
                            "nameDest": "M123" if label else "C123",
                            "oldbalanceOrg": 1000 + index,
                            "newbalanceOrig": 900 + index,
                            "oldbalanceDest": 2000 + index,
                            "newbalanceDest": 2100 + index,
                            "isFlaggedFraud": label,
                            "isFraud": label,
                        }
                    )

            samples, population_counts = collect_samples(
                csv_path,
                train_limit_per_class=20,
                validation_limit_per_class=20,
                validation_fraction=0.2,
                random_seed=42,
            )

        self.assertGreater(len(samples["train"][0]), 0)
        self.assertGreater(len(samples["train"][1]), 0)
        self.assertGreater(len(samples["validation"][0]), 0)
        self.assertGreater(len(samples["validation"][1]), 0)
        self.assertGreater(len(samples["test"][0]), 0)
        self.assertGreater(len(samples["test"][1]), 0)
        for label in (0, 1):
            self.assertEqual(
                sum(
                    population_counts[split][label]
                    for split in ("train", "validation", "test")
                ),
                50,
            )
        self.assertEqual(
            sum(population_counts["train"].values())
            + sum(population_counts["validation"].values())
            + sum(population_counts["test"].values()),
            100,
        )


if __name__ == "__main__":
    unittest.main()
