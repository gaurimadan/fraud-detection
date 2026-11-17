from __future__ import annotations

import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "/app")
from producer import generated_cycle, kafka_bootstrap_servers, make_transaction


NORMAL_FEATURES = {
    "type": "PAYMENT",
    "destination_is_merchant": "1",
    "step": "100",
    "amount": "500.0",
    "oldbalanceOrg": "1000.0",
    "newbalanceOrig": "500.0",
    "oldbalanceDest": "0.0",
    "newbalanceDest": "0.0",
    "isFlaggedFraud": "0",
}

FRAUD_FEATURES = {
    **NORMAL_FEATURES,
    "type": "TRANSFER",
    "destination_is_merchant": "0",
    "oldbalanceOrg": "500.0",
    "newbalanceOrig": "0.0",
}


class ProducerTests(unittest.TestCase):
    def test_render_kafka_host_overrides_legacy_bootstrap_value(self):
        with patch.dict(
            "os.environ",
            {
                "KAFKA_BOOTSTRAP_HOST": "kafka-private-host",
                "KAFKA_BOOTSTRAP_PORT": "9092",
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            },
        ):
            self.assertEqual(kafka_bootstrap_servers(), "kafka-private-host:9092")

    def test_transaction_contains_model_features_but_no_training_label(self):
        transaction = make_transaction(NORMAL_FEATURES)

        self.assertEqual(transaction["type"], "PAYMENT")
        self.assertEqual(transaction["step"], 100)
        self.assertTrue(transaction["nameDest"].startswith("M"))
        self.assertNotIn("isFraud", transaction)

    def test_generated_cycle_can_emit_fraud_shaped_validation_event(self):
        samples = {"normal": [NORMAL_FEATURES], "fraud": [FRAUD_FEATURES]}
        with patch("producer.DEMO_FRAUD_RATE", 1.0):
            events = generated_cycle(samples)

        self.assertEqual(len(events), 9)
        self.assertEqual(events[-1]["type"], "TRANSFER")
        self.assertFalse(events[-1]["nameDest"].startswith("M"))
        self.assertNotIn("isFraud", events[-1])


if __name__ == "__main__":
    unittest.main()
