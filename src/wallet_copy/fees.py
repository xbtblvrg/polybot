"""Fee helpers for Polymarket wallet-copy accounting."""

from __future__ import annotations

from typing import Any


POLYMARKET_EMBEDDED_FEE_RATE = 0.0
POLYMARKET_EMBEDDED_FEE_FORMULA = "fee_usd = rate * shares * price * (1 - price)"
POLYMARKET_EMBEDDED_FEE_SOURCE = "live_ledger.expected_vs_realized_fee.realized_fee_usd_nonnull_count_zero"
POLYMARKET_UNVALIDATED_PROPOSED_FEE_RATE = 0.069997697
POLYMARKET_UNVALIDATED_PROPOSED_FEE_SOURCE = "polygon_receipt_pusd_debit_fit_1629_rows_2026-08-02"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def expected_polymarket_buy_fee_usd(
    *,
    shares: Any,
    price: Any,
    rate: float = POLYMARKET_EMBEDDED_FEE_RATE,
) -> float:
    """Return expected embedded pUSD fee for a binary-market BUY fill."""

    fill_shares = max(0.0, _num(shares))
    fill_price = _num(price)
    if fill_shares <= 0.0 or fill_price <= 0.0 or fill_price >= 1.0:
        return 0.0
    return round(float(rate) * fill_shares * fill_price * (1.0 - fill_price), 6)


def modeled_unvalidated_polymarket_buy_fee_usd(*, shares: Any, price: Any) -> float:
    """Return the measured receipt-premium model for diagnostics only."""

    return expected_polymarket_buy_fee_usd(
        shares=shares,
        price=price,
        rate=POLYMARKET_UNVALIDATED_PROPOSED_FEE_RATE,
    )
