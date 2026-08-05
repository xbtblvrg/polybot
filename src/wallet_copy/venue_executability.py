"""Venue-executable evidence helpers shared by paper scoring and live gates."""

from __future__ import annotations

from typing import Any, Literal

from src.trade_executor import (
    WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
    WALLET_COPY_MIN_SHARES,
)
from src.wallet_copy.models import num

# The live maker path permits a $1 nominal order to expand only as far as
# the executor's $2.50 minimum-share hard cap. Require the measured forward
# population to retain at least that same nominal/cap fraction.
VENUE_NOMINAL_MIN_ORDER_USD = 1.0
VENUE_REACHABLE_SHARE_MIN_PCT = (
    100.0
    * VENUE_NOMINAL_MIN_ORDER_USD
    / WALLET_COPY_MIN_SHARE_HARD_CAP_USD
)
VENUE_EVIDENCE_AUTHORITY = "venue_executable_full_stream_rescore"
VenueOrderType = Literal["maker", "taker"]


def venue_minimum_max_price(
    min_order_usd: float = VENUE_NOMINAL_MIN_ORDER_USD,
    *,
    order_type: VenueOrderType = "maker",
) -> float:
    """Derive the price ceiling for maker shares or taker notional."""

    if order_type == "maker":
        minimum_units = WALLET_COPY_MIN_SHARES
    elif order_type == "taker":
        # FAK/FOK market BUYs are USDC-denominated.  They must clear the
        # nominal dollar floor, but the resting-order five-share minimum does
        # not apply (trade_executor.py's is_market_buy branch).
        return 1.0
    else:
        raise ValueError(f"unsupported venue order type: {order_type}")
    return max(WALLET_COPY_MIN_SHARE_HARD_CAP_USD, min_order_usd) / minimum_units


def venue_execution_price(row: dict[str, Any]) -> float | None:
    """Return the first truthy price field used by the venue predicate."""

    for field in ("entry_price", "fill_price", "avg_fill_price", "source_price", "price"):
        value = row.get(field)
        if value:
            return num(value)
    return None


def venue_discard_reason(
    row: dict[str, Any],
    *,
    min_order_usd: float,
    order_type: VenueOrderType = "maker",
) -> str | None:
    """Classify why the selected execution geometry discards a row."""

    price = venue_execution_price(row)
    if price is None:
        return "price_field_absent"
    if price <= 0:
        return "price_nonpositive"
    if min_order_usd <= 0:
        return "min_order_usd_nonpositive"
    if price > venue_minimum_max_price(
        min_order_usd, order_type=order_type
    ) + 1e-9:
        return "price_above_venue_minimum_max_price"
    return None


def venue_discard_price_band(price: float | None) -> str:
    """Bucket distance above the venue minimum-share price ceiling."""

    if price is None or price <= 0:
        return "unknown"
    if price <= 0.50:
        return "<=0.50"
    for upper, label in (
        (0.60, "0.50-0.60"),
        (0.70, "0.60-0.70"),
        (0.80, "0.70-0.80"),
        (0.90, "0.80-0.90"),
        (1.00, "0.90-1.00"),
    ):
        if price <= upper:
            return label
    return ">1.00"


def row_is_venue_executable(
    row: dict[str, Any],
    *,
    min_order_usd: float,
    order_type: VenueOrderType = "maker",
) -> bool:
    """Return whether the single-source venue classifier accepts the row."""

    return venue_discard_reason(
        row,
        min_order_usd=min_order_usd,
        order_type=order_type,
    ) is None


def venue_gate_summary(cell: dict[str, Any]) -> dict[str, Any]:
    """Return only explicitly-authoritative venue evidence, failing closed."""
    venue = cell.get("venue_executable_full_stream_rescore")
    authority = cell.get("evidence_authority")
    if isinstance(venue, dict) and authority is None:
        authority = venue.get("evidence_authority")
    if (
        not isinstance(venue, dict)
        or authority != VENUE_EVIDENCE_AUTHORITY
        or venue.get("advisory_only") is True
    ):
        return {
            "reason": "venue_authority_missing_stale_artifact",
            "f1_pass": False,
            "f1_venue_reachable_admissible": False,
            "evidence_authority": authority,
        }
    return venue
