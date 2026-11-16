import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "producer"))
from rules import HIGH_AMOUNT_THRESHOLD, is_high_amount


class HighAmountRuleTests(unittest.TestCase):
    def test_amount_at_threshold_is_flagged(self):
        self.assertTrue(is_high_amount(HIGH_AMOUNT_THRESHOLD))

    def test_amount_below_threshold_is_not_flagged(self):
        self.assertFalse(is_high_amount(Decimal("4999.99")))

    def test_string_amount_is_supported(self):
        self.assertTrue(is_high_amount("5000.01"))


if __name__ == "__main__":
    unittest.main()
