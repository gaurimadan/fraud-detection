from __future__ import annotations

from decimal import Decimal

HIGH_AMOUNT_THRESHOLD = Decimal("5000.00")


def is_high_amount(amount: Decimal | int | float | str) -> bool:
    """Return whether an individual transaction meets the high-amount rule."""
    return Decimal(str(amount)) >= HIGH_AMOUNT_THRESHOLD
