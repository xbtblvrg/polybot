#!/usr/bin/env python3
"""Measure early 01a BTC-5m signal supply for active or explicit wallets."""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json

WINDOW_RE = re.compile(r"btc-(?:updown|up-or-down)-5m-(\d+)")
SIGNAL_EVENTS = {
    "wallet_copy_live_profit_latency_suppression_reject",
    "wallet_copy_live_order",
}
OFFSET_BINS = (
    (45.0, "lt_45s"),
    (60.0, "45_60s"),
    (75.0, "60_75s"),
    (90.0, "75_90s"),
    (120.0, "90_120s"),
    (180.0, "120_180s"),
    (float("inf"), "gte_180s"),
)


def _json_rows(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _wallet(value: Any) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if re.fullmatch(r"0x[0-9a-f]{40}", candidate) else ""


def active_roster_wallets(guard_state: dict[str, Any]) -> list[str]:
    """Return the runtime roster, falling back to the persisted active set."""
    for key in ("active_set_runtime", "active_set"):
        active_set = guard_state.get(key)
        if not isinstance(active_set, dict):
            continue
        wallets = {
            wallet
            for member in active_set.get("members") or []
            if isinstance(member, dict)
            and (member.get("enabled") is not False)
            and (wallet := _wallet(member.get("source_wallet") or member.get("wallet")))
        }
        if wallets:
            return sorted(wallets)
    selected = _wallet(guard_state.get("source_wallet"))
    return [selected] if selected else []


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value: Any) -> float | None:
    numeric = _float(value)
    if numeric is not None:
        return numeric
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _offset_bin(offset_s: float) -> str:
    return next(name for ceiling, name in OFFSET_BINS if offset_s < ceiling)


def _signal(row: dict[str, Any]) -> dict[str, Any] | None:
    event = str(row.get("event") or "")
    if event not in SIGNAL_EVENTS:
        return None
    # Every approved suppression is still observed source supply. Restricting this
    # input to the 60s reject would erase early signals rejected by a later gate.
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    slug = str(row.get("market_slug") or payload.get("market_slug") or "")
    match = WINDOW_RE.search(slug)
    if not match:
        return None
    start = float(match.group(1))
    offset = _float(row.get("window_time_s"))
    offset_basis = "window_time_s"
    if offset is None:
        offset = _float(payload.get("window_time_s"))
    if offset is None:
        submitted_at = _timestamp(row.get("submitted_at") or payload.get("submitted_at"))
        if submitted_at is not None:
            offset = submitted_at - start
            offset_basis = "submitted_at"
    price = _float(row.get("limit_price"))
    if price is None:
        price = _float(payload.get("limit_price"))
    wallet = _wallet(row.get("source_wallet") or payload.get("source_wallet"))
    outcome = str(row.get("outcome") or payload.get("outcome") or "").strip().lower()
    if not wallet or offset is None or offset < -300.0 or price is None or not outcome:
        return None
    return {
        "wallet": wallet,
        "market_slug": slug,
        "window_start_ts": start,
        "outcome": outcome,
        "offset_s": offset,
        "offset_basis": offset_basis,
        "price": price,
    }


def build_report(
    event_rows: Iterable[dict[str, Any]],
    *,
    wallets: Iterable[str],
    generated_at: str,
) -> dict[str, Any]:
    selected = sorted({_wallet(wallet) for wallet in wallets} - {""})
    selected_set = set(selected)
    earliest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in event_rows:
        signal = _signal(row)
        if signal is None or signal["wallet"] not in selected_set:
            continue
        key = (signal["wallet"], signal["market_slug"], signal["outcome"])
        if key not in earliest or signal["offset_s"] < earliest[key]["offset_s"]:
            earliest[key] = signal

    by_wallet: dict[str, dict[str, Any]] = {}
    for wallet in selected:
        signals = [row for row in earliest.values() if row["wallet"] == wallet]
        observed = bool(signals)
        starts = [row["window_start_ts"] for row in signals]
        span_days = ((max(starts) - min(starts) + 300.0) / 86400.0) if starts else None
        histogram = {name: 0 for _, name in OFFSET_BINS}
        offset_basis_counts = {"window_time_s": 0, "submitted_at": 0}
        for row in signals:
            histogram[_offset_bin(row["offset_s"])] += 1
            offset_basis_counts[row["offset_basis"]] += 1
        in_band = [row for row in signals if 0.25 <= row["price"] < 0.32]
        in_band_windows = {row["market_slug"] for row in in_band}
        qualifying_windows = {row["market_slug"] for row in in_band if row["offset_s"] < 60.0}
        rate: float | str = (
            len(qualifying_windows) / len(in_band_windows)
            if in_band_windows
            else (0.0 if observed else "UNOBSERVED")
        )
        daily: float | str = round(len(qualifying_windows) / span_days, 6) if span_days is not None else "UNOBSERVED"
        by_wallet[wallet] = {
            "observation_status": "OBSERVED" if observed else "UNOBSERVED",
            "observed_unique_window_outcome_signals": len(signals),
            "observed_unique_windows": len({row["market_slug"] for row in signals}),
            "observation_start_window_ts": min(starts) if starts else None,
            "observation_end_window_ts": max(starts) if starts else None,
            "observation_span_days": round(span_days, 6) if span_days is not None else None,
            "span_days_below_1": span_days is not None and span_days < 1.0,
            "rate_confidence": (
                "LOW_CONFIDENCE_SUB_DAY_SPAN"
                if span_days is not None and span_days < 1.0
                else ("OBSERVED_AT_LEAST_ONE_DAY" if span_days is not None else "UNOBSERVED")
            ),
            "offset_histogram": histogram,
            "offset_basis_counts": offset_basis_counts,
            "in_band_01a_windows": len(in_band_windows),
            "in_band_01a_within_60s_windows": len(qualifying_windows),
            "in_band_01a_within_60s_rate": round(rate, 6) if isinstance(rate, float) else rate,
            "qualifying_window_count": len(qualifying_windows),
            "span_days": round(span_days, 6) if span_days is not None else None,
            "qualifying_01a_windows_per_day_contributed": daily,
        }

    ranked = sorted(
        (
            {
                "wallet": wallet,
                "qualifying_01a_windows_per_day_contributed": metrics["qualifying_01a_windows_per_day_contributed"],
                "qualifying_window_count": metrics["qualifying_window_count"],
                "span_days": metrics["span_days"],
                "in_band_01a_within_60s_rate": metrics["in_band_01a_within_60s_rate"],
                "in_band_01a_windows": metrics["in_band_01a_windows"],
            }
            for wallet, metrics in by_wallet.items()
            if metrics["observation_status"] == "OBSERVED"
        ),
        key=lambda row: (-row["qualifying_01a_windows_per_day_contributed"], row["wallet"]),
    )
    observed_signals = [row for row in earliest.values() if row["wallet"] in selected_set]
    common_starts = [row["window_start_ts"] for row in observed_signals]
    common_span_days = (
        (max(common_starts) - min(common_starts) + 300.0) / 86400.0
        if common_starts
        else None
    )
    union_qualifying_windows = {
        row["market_slug"]
        for row in observed_signals
        if 0.25 <= row["price"] < 0.32 and row["offset_s"] < 60.0
    }
    aggregate: float | str = (
        round(len(union_qualifying_windows) / common_span_days, 6)
        if common_span_days is not None
        else "UNOBSERVED"
    )
    unobserved_wallets = [wallet for wallet, metrics in by_wallet.items() if metrics["observation_status"] == "UNOBSERVED"]
    return {
        "kind": "window_time_offset_supply",
        "generated_at": generated_at,
        "flow_stage": "MEASURE/MONEY/MINE",
        "paper_only": True,
        "live_mutation": False,
        "measurement_contract": {
            "signal_unit": "earliest observed buy per wallet/market/outcome",
            "01a_price_band": {"min_inclusive": 0.25, "max_exclusive": 0.32},
            "early_gate_s": 60.0,
            "pre_window_join_guard_min_offset_s": -300.0,
            "offset_basis": "window_time_s, falling back to submitted_at minus window start",
            "offset_semantics": "our-clock upper bound on source earliness; safe for admission, forbidden as sole CUT evidence",
            "rate_denominator": "unique 01a market windows",
            "per_day_denominator": "true first-to-last observed BTC-5m window span plus 300s",
            "aggregate_semantics": "union of distinct qualifying windows over one common observed span",
        },
        "wallet_source": "explicit_or_active_roster",
        "wallets": selected,
        "per_wallet": by_wallet,
        "ranking": ranked,
        "unobserved_wallets": unobserved_wallets,
        "observed_wallet_count": len(ranked),
        "unobserved_wallet_count": len(unobserved_wallets),
        "union_qualifying_window_count": len(union_qualifying_windows),
        "union_span_days": round(common_span_days, 6) if common_span_days is not None else None,
        "union_span_days_below_1": common_span_days is not None and common_span_days < 1.0,
        "union_rate_confidence": (
            "LOW_CONFIDENCE_SUB_DAY_SPAN"
            if common_span_days is not None and common_span_days < 1.0
            else ("OBSERVED_AT_LEAST_ONE_DAY" if common_span_days is not None else "UNOBSERVED")
        ),
        "union_qualifying_01a_windows_per_day": aggregate,
        "aggregate_qualifying_01a_windows_per_day": aggregate,
        "first_rung_target_qualifying_01a_windows_per_day": 30.0,
        "first_rung_gap_qualifying_01a_windows_per_day": (
            round(max(0.0, 30.0 - aggregate), 6) if isinstance(aggregate, float) else "UNOBSERVED"
        ),
        "status": (
            "PARTIAL_COVERAGE_LOW_CONFIDENCE_SUB_DAY_SPAN"
            if common_span_days is not None and common_span_days < 1.0 and unobserved_wallets
            else "LOW_CONFIDENCE_SUB_DAY_SPAN"
            if common_span_days is not None and common_span_days < 1.0
            else "FIRST_RUNG_PASS"
            if isinstance(aggregate, float) and aggregate >= 30.0
            else ("PARTIAL_COVERAGE_FIRST_RUNG_SHORT" if unobserved_wallets else "FIRST_RUNG_SHORT")
        ),
    }


def _parse_wallets(values: list[str]) -> list[str]:
    return [wallet for value in values for wallet in value.split(",") if wallet.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--wallets", action="append", default=[], help="wallet or comma-separated wallets; overrides active roster")
    parser.add_argument("--output", default="data/research/window_time_offset_supply_latest.json")
    args = parser.parse_args()

    explicit = _parse_wallets(args.wallets)
    if explicit:
        wallets = explicit
    else:
        guard_path = Path(args.guard_state)
        guard = json.loads(guard_path.read_text(encoding="utf-8")) if guard_path.exists() else {}
        wallets = active_roster_wallets(guard if isinstance(guard, dict) else {})
    if not wallets:
        parser.error("no valid --wallets and no active roster wallets found")
    generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    report = build_report(_json_rows(Path(args.events)), wallets=wallets, generated_at=generated_at)
    atomic_write_json(Path(args.output), report)
    print(json.dumps({
        "generated_at": generated_at,
        "status": report["status"],
        "wallets": report["wallets"],
        "aggregate_qualifying_01a_windows_per_day": report["aggregate_qualifying_01a_windows_per_day"],
        "ranking": report["ranking"],
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
