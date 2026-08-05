"""Copyability gate for wallet-copy live tracking.

The gate is not a profitability filter. It only answers whether a wallet BUY is
fresh and executable enough to be counted as a required copy event in the
paper/live-parity tracker. Rejected events stay logged as filtered evidence so a
green state cannot be created by silently hiding stale or unmarketable rows.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.wallet_copy.models import WalletEvent, num


@dataclass(frozen=True)
class CopyabilityPolicy:
    policy_id: str = "wallet_copy_copyability_gate_v1"
    max_event_age_s: float = 10.0
    max_wallet_fetch_duration_s: float = 2.0
    min_clob_fill_ratio: float = 0.999
    require_clob_book: bool = True

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CopyabilityDecision:
    accepted: bool
    reason: str
    policy_id: str
    details: dict[str, Any]

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _book(evidence: dict[str, Any]) -> dict[str, Any]:
    value = evidence.get("clob_book") if isinstance(evidence, dict) else {}
    return value if isinstance(value, dict) else {}


def _wallet_api(evidence: dict[str, Any]) -> dict[str, Any]:
    value = evidence.get("wallet_api") if isinstance(evidence, dict) else {}
    return value if isinstance(value, dict) else {}


def _min_slippage_to_fill_bps(*, best_ask: float, source_price: float) -> float | None:
    if best_ask <= 0 or source_price <= 0:
        return None
    return round(max(0.0, (best_ask / source_price - 1.0) * 10_000.0), 6)


def score_copyability(
    event: WalletEvent,
    evidence: dict[str, Any],
    policy: CopyabilityPolicy | None = None,
) -> CopyabilityDecision:
    """Return whether a tracked wallet event should be a required paper copy."""

    cfg = policy or CopyabilityPolicy()
    action = event.action.upper()
    wallet_api = _wallet_api(evidence)
    book = _book(evidence)
    fetch_duration_s = num(wallet_api.get("fetch_duration_s"), -1.0)
    event_age_s = num(wallet_api.get("event_age_s"), event.age_s if event.age_s is not None else -1.0)
    details = {
        "wallet_action": action,
        "event_age_s": round(event_age_s, 6) if event_age_s >= 0 else None,
        "event_age_at_source_fetch_start_s": wallet_api.get("event_age_at_source_fetch_start_s"),
        "max_event_age_s": float(cfg.max_event_age_s),
        "wallet_fetch_duration_s": round(fetch_duration_s, 6) if fetch_duration_s >= 0 else None,
        "wallet_fetch_duration_basis": wallet_api.get("fetch_duration_basis"),
        "source_fetch_duration_s": wallet_api.get("source_fetch_duration_s"),
        "wallet_batch_fetch_duration_s": wallet_api.get("wallet_batch_fetch_duration_s"),
        "max_wallet_fetch_duration_s": float(cfg.max_wallet_fetch_duration_s),
        "wallet_route_status": wallet_api.get("route_status"),
        "wallet_route_class": wallet_api.get("route_class"),
        "wallet_route_report_id": wallet_api.get("route_report_id"),
        "wallet_data_api_source": wallet_api.get("data_api_source"),
        "wallet_data_api_query_param": wallet_api.get("data_api_query_param"),
        "clob_book_status": book.get("status"),
        "clob_instant_fill_status": book.get("instant_fill_status"),
        "clob_blocking_reason": book.get("blocking_reason"),
        "clob_route_status": book.get("route_status"),
        "clob_route_class": book.get("route_class"),
        "clob_route_report_id": book.get("route_report_id"),
        "clob_book_timestamp": book.get("book_timestamp"),
        "clob_book_hash": book.get("book_hash"),
        "clob_best_ask": book.get("best_ask"),
        "clob_max_copy_price": book.get("max_copy_price"),
        "clob_fill_ratio": book.get("fill_ratio"),
        "clob_fillable_usd": book.get("fillable_usd"),
        "copy_size_usd": book.get("copy_size_usd"),
        "source_price": event.price,
        "min_slippage_to_fill_bps": _min_slippage_to_fill_bps(
            best_ask=num(book.get("best_ask")),
            source_price=float(event.price),
        ),
    }

    def decision(accepted: bool, reason: str, blockers: list[str] | None = None) -> CopyabilityDecision:
        final_details = dict(details)
        final_blockers = list(blockers or [])
        final_details["blockers"] = final_blockers
        final_details["primary_blocker"] = final_blockers[0] if final_blockers else None
        return CopyabilityDecision(accepted, reason, cfg.policy_id, final_details)

    if action != "BUY":
        return decision(True, "lifecycle_event_not_buy")

    blockers: list[str] = []
    if event.event_ts is None:
        blockers.append("missing_event_timestamp")
    if event_age_s < 0:
        blockers.append("missing_wallet_api_age")
    if event_age_s >= 0 and event_age_s > float(cfg.max_event_age_s):
        blockers.append("event_age_above_cap")
    if fetch_duration_s >= 0 and fetch_duration_s > float(cfg.max_wallet_fetch_duration_s):
        blockers.append("fetch_duration_above_cap")
    if cfg.require_clob_book and str(book.get("status") or "") != "OK":
        blockers.append("book_missing_or_not_ok")
    best_ask = num(book.get("best_ask"))
    max_copy_price = num(book.get("max_copy_price"))
    if best_ask <= 0:
        blockers.append("no_ask_liquidity")
    if max_copy_price > 0 and best_ask > max_copy_price:
        blockers.append("best_ask_above_slippage_cap")
    fill_ratio = num(book.get("fill_ratio"))
    if fill_ratio < float(cfg.min_clob_fill_ratio):
        blockers.append("depth_below_min_fill_ratio")
    if blockers:
        return decision(False, blockers[0], blockers)
    return decision(True, "accepted")
