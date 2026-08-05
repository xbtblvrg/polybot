#!/usr/bin/env python3
"""Attribute f418 outcomes after the immutable 0.50-0.70 band-gate cut."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


ACTIVATION_UTC = "2026-07-23T21:42:00Z"
F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
FEE_RATE = 0.069997697
WINDOW_RE = re.compile(r"btc-(?:updown|up-or-down)-5m-(\d+)")


def _parse_ts(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _jsonl(path: Path) -> Iterable[dict[str, Any]]:
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


def _winner(row: dict[str, Any]) -> str:
    value = str(row.get("winning_outcome") or "").strip().lower()
    if value:
        return value
    direction = str(row.get("direction") or "").strip().lower()
    return direction if direction in {"up", "down"} else ""


def _resolution_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in rows:
        winner = _winner(row)
        if not winner:
            continue
        for value in (row.get("condition_id"), row.get("market"), row.get("market_slug")):
            key = str(value or "").strip().lower()
            if key:
                out[key] = winner
    return out


def _window_start(slug: Any) -> float | None:
    match = WINDOW_RE.search(str(slug or "").lower())
    return float(match.group(1)) if match else None


def _bucket(value: float | None, cuts: tuple[float, ...], labels: tuple[str, ...]) -> str:
    if value is None:
        return "missing"
    for cut, label in zip(cuts, labels):
        if value < cut:
            return label
    return labels[-1]


def _price_bucket(value: float | None) -> str:
    return _bucket(
        value,
        (0.25, 0.40, 0.50, 0.60, 0.70, float("inf")),
        ("lt_0.25", "0.25_0.40", "0.40_0.50", "0.50_0.60", "0.60_0.70", "gte_0.70"),
    )


def _age_bucket(value: float | None) -> str:
    return _bucket(value, (5, 10, 20, 30, float("inf")), ("lt_5", "5_10", "10_20", "20_30", "gte_30"))


def _offset_bucket(value: float | None) -> str:
    return _bucket(
        value,
        (30, 60, 120, 180, float("inf")),
        ("lt_30", "30_60", "60_120", "120_180", "gte_180"),
    )


def _fee(shares: float, price: float) -> float:
    return FEE_RATE * shares * price * (1.0 - price)


def _walk_intent_metadata(value: Any, out: dict[str, dict[str, Any]]) -> None:
    if isinstance(value, dict):
        intent_id = str(value.get("intent_id") or "")
        if intent_id:
            target = out.setdefault(intent_id, {})
            for key in (
                "event_age_s",
                "event_ts",
                "market_slug",
                "observed_ts",
                "outcome",
                "source_wallet",
            ):
                candidate = value.get(key)
                if candidate is not None and target.get(key) is None:
                    target[key] = candidate
        for item in value.values():
            _walk_intent_metadata(item, out)
    elif isinstance(value, list):
        for item in value:
            _walk_intent_metadata(item, out)


def _metadata_index(rows: Iterable[dict[str, Any]], activation_ts: float) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_ts = _parse_ts(row.get("generated_at") or row.get("ts"))
        if row_ts is not None and row_ts < activation_ts:
            continue
        _walk_intent_metadata(row, out)
    return out


def _resolved_row(
    *,
    cohort: str,
    intent_id: str,
    slug: str,
    condition_id: str,
    outcome: str,
    price: float,
    shares: float,
    cost: float,
    expected_fee: float,
    fak_outcome: str,
    source_age_s: float | None,
    event_ts: float | None,
    winner: str,
    observed_ts: float,
) -> dict[str, Any]:
    won = outcome.strip().lower() == winner
    pnl = (shares if won else 0.0) - cost - expected_fee
    start = _window_start(slug)
    offset = event_ts - start if event_ts is not None and start is not None else None
    return {
        "cohort": cohort,
        "intent_id": intent_id,
        "market_slug": slug,
        "condition_id": condition_id,
        "outcome": outcome,
        "winning_outcome": winner,
        "won": won,
        "price": round(price, 6),
        "shares": round(shares, 6),
        "cost_usd": round(cost, 6),
        "expected_fee_usd": round(expected_fee, 6),
        "post_fee_pnl_usd": round(pnl, 6),
        "source_age_s": round(source_age_s, 6) if source_age_s is not None else None,
        "window_offset_s": round(offset, 6) if offset is not None else None,
        "price_cell": _price_bucket(price),
        "source_age_cell": _age_bucket(source_age_s),
        "fak_outcome_cell": fak_outcome,
        "window_offset_cell": _offset_bucket(offset),
        "observed_ts": observed_ts,
    }


def build_report(
    *,
    event_rows: Iterable[dict[str, Any]],
    resolution_rows: Iterable[dict[str, Any]],
    metadata_rows: Iterable[dict[str, Any]] = (),
    generated_at: str,
    activation_utc: str = ACTIVATION_UTC,
    min_resolved_windows: int = 20,
) -> dict[str, Any]:
    activation_ts = _parse_ts(activation_utc)
    if activation_ts is None:
        raise ValueError(f"invalid activation timestamp: {activation_utc}")
    resolutions = _resolution_index(resolution_rows)
    metadata = _metadata_index(metadata_rows, activation_ts)

    denied: dict[str, dict[str, Any]] = {}
    accepted: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        row_ts = _parse_ts(row.get("ts") or row.get("timestamp"))
        if row_ts is None or row_ts < activation_ts:
            continue
        event = str(row.get("event") or "")
        if event == "wallet_copy_live_entry_price_band_gate_counterfactual":
            key = str(row.get("market_slug") or row.get("condition_id") or "")
            denied.setdefault(key, row)
            continue
        if event != "wallet_copy_live_lifecycle" or str(row.get("status") or "") not in {
            "LIVE_FILLED",
            "LIVE_REJECTED",
        }:
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if str(payload.get("execution_role") or "") != "taker":
            continue
        intent_id = str(row.get("intent_id") or "")
        current = accepted.get(intent_id)
        if current is None or str(row.get("status")) == "LIVE_FILLED":
            accepted[intent_id] = row

    resolved: list[dict[str, Any]] = []
    for row in denied.values():
        slug = str(row.get("market_slug") or "")
        condition = str(row.get("condition_id") or "")
        winner = resolutions.get(condition.lower()) or resolutions.get(slug.lower())
        if not winner:
            continue
        intent_id = str(row.get("intent_id") or "")
        meta = metadata.get(intent_id, {})
        price = float(row.get("limit_price") or 0.0)
        shares = float(row.get("shares") or 0.0)
        cost = float(row.get("copy_size_usd") or shares * price)
        resolved.append(
            _resolved_row(
                cohort="denied_counterfactual",
                intent_id=intent_id,
                slug=slug,
                condition_id=condition,
                outcome=str(row.get("outcome") or ""),
                price=price,
                shares=shares,
                cost=cost,
                expected_fee=float(row.get("expected_fee_usd") or _fee(shares, price)),
                fak_outcome="DENIED_BEFORE_FAK",
                source_age_s=float(meta["event_age_s"]) if meta.get("event_age_s") is not None else None,
                event_ts=float(meta["event_ts"]) if meta.get("event_ts") is not None else None,
                winner=winner,
                observed_ts=float(_parse_ts(row.get("ts")) or activation_ts),
            )
        )

    for intent_id, row in accepted.items():
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        profile = (
            payload.get("wallet_copy_execute_live_profile")
            if isinstance(payload.get("wallet_copy_execute_live_profile"), dict)
            else {}
        )
        latency = (
            payload.get("wallet_copy_latency_budget")
            if isinstance(payload.get("wallet_copy_latency_budget"), dict)
            else {}
        )
        meta = metadata.get(intent_id, {})
        slug = str(profile.get("market_slug") or meta.get("market_slug") or "")
        condition = str(profile.get("condition_id") or "")
        winner = resolutions.get(condition.lower()) or resolutions.get(slug.lower())
        if not winner:
            continue
        status = str(row.get("status") or "")
        filled = status == "LIVE_FILLED"
        shares = float(payload.get("response_fill_size_shares") or payload.get("fill_size_shares") or 0.0)
        cost = float(payload.get("response_filled_size_usd") or payload.get("filled_size_usd") or 0.0)
        price = float(payload.get("response_fill_price") or payload.get("entry_price") or 0.0)
        event_ts = latency.get("source_fill_block_ts")
        source_age = None
        if event_ts is not None:
            built_ts = latency.get("intent_built_ts")
            source_age = max(0.0, float(built_ts) - float(event_ts)) if built_ts is not None else None
        if source_age is None and meta.get("event_age_s") is not None:
            source_age = float(meta["event_age_s"])
        resolved.append(
            _resolved_row(
                cohort="accepted_live",
                intent_id=intent_id,
                slug=slug,
                condition_id=condition,
                outcome=str(profile.get("outcome") or meta.get("outcome") or payload.get("outcome") or ""),
                price=price,
                shares=shares,
                cost=cost,
                expected_fee=_fee(shares, price) if filled else 0.0,
                fak_outcome="FAK_FILLED" if filled else f"FAK_{str(payload.get('error_class') or 'REJECTED').upper()}",
                source_age_s=source_age,
                event_ts=float(event_ts) if event_ts is not None else None,
                winner=winner,
                observed_ts=float(_parse_ts(row.get("ts")) or activation_ts),
            )
        )

    resolved.sort(key=lambda row: (float(row["observed_ts"]), str(row["cohort"]), str(row["intent_id"])))
    by_window = {str(row["market_slug"]) for row in resolved}
    split = max(1, int(len(resolved) * 0.8)) if resolved else 0
    train = resolved[:split]
    holdout = resolved[split:]

    cells: dict[tuple[str, str, str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"resolved_rows": 0, "resolved_windows": set(), "post_fee_pnl_usd": 0.0}
    )
    for row in resolved:
        key = (
            str(row["cohort"]),
            str(row["price_cell"]),
            str(row["source_age_cell"]),
            str(row["fak_outcome_cell"]),
            str(row["window_offset_cell"]),
        )
        cell = cells[key]
        cell["resolved_rows"] += 1
        cell["resolved_windows"].add(str(row["market_slug"]))
        cell["post_fee_pnl_usd"] += float(row["post_fee_pnl_usd"])
    cell_rows = [
        {
            "cohort": key[0],
            "price_cell": key[1],
            "source_age_cell": key[2],
            "fak_outcome_cell": key[3],
            "window_offset_cell": key[4],
            "resolved_rows": value["resolved_rows"],
            "resolved_windows": len(value["resolved_windows"]),
            "post_fee_pnl_usd": round(value["post_fee_pnl_usd"], 6),
        }
        for key, value in cells.items()
    ]
    cell_rows.sort(key=lambda row: (row["post_fee_pnl_usd"], -row["resolved_windows"]))

    def cohort_summary(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        selected = [row for row in rows if row["cohort"] == name]
        return {
            "resolved_rows": len(selected),
            "resolved_windows": len({str(row["market_slug"]) for row in selected}),
            "post_fee_pnl_usd": round(sum(float(row["post_fee_pnl_usd"]) for row in selected), 6),
            "wins": sum(1 for row in selected if row["won"]),
            "losses": sum(1 for row in selected if not row["won"]),
        }

    acceptance_ready = len(by_window) >= int(min_resolved_windows) and bool(holdout)
    return {
        "schema_version": 1,
        "kind": "f418_post_band_gate_residual_loss_causal_shadow",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "activation_utc": activation_utc,
        "source_wallet": F418,
        "paper_only": True,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
        "status": "HOLDOUT_READY" if acceptance_ready else "ACCRUING",
        "gate": {
            "minimum_resolved_accepted_or_denied_windows": int(min_resolved_windows),
            "resolved_accepted_or_denied_windows": len(by_window),
            "holdout_rows": len(holdout),
            "pass": acceptance_ready,
            "rule": ">=20 resolved accepted/denied counterfactual windows plus chronological 20% holdout",
        },
        "cohorts": {
            "accepted_live": cohort_summary("accepted_live", resolved),
            "denied_counterfactual": cohort_summary("denied_counterfactual", resolved),
        },
        "train": {
            "rows": len(train),
            "post_fee_pnl_usd": round(sum(float(row["post_fee_pnl_usd"]) for row in train), 6),
        },
        "holdout": {
            "rows": len(holdout),
            "post_fee_pnl_usd": round(sum(float(row["post_fee_pnl_usd"]) for row in holdout), 6),
        },
        "cells": cell_rows,
        "rows": resolved[-200:],
        "decision_rule": (
            "name a surviving toxic cell or quantify deny-saved loss only after the sample gate; "
            "this reporter never mutates live policy"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--guard-log", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--resolution-log", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument(
        "--output",
        default="data/research/f418_post_band_gate_residual_loss_causal_shadow_latest.json",
    )
    parser.add_argument("--activation-utc", default=ACTIVATION_UTC)
    parser.add_argument("--min-resolved-windows", type=int, default=20)
    args = parser.parse_args()
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report = build_report(
        event_rows=_jsonl(ROOT / args.event_log) or (),
        resolution_rows=_jsonl(ROOT / args.resolution_log) or (),
        metadata_rows=_jsonl(ROOT / args.guard_log) or (),
        generated_at=generated_at,
        activation_utc=args.activation_utc,
        min_resolved_windows=args.min_resolved_windows,
    )
    atomic_write_json(ROOT / args.output, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "resolved_windows": report["gate"]["resolved_accepted_or_denied_windows"],
                "accepted_post_fee_pnl_usd": report["cohorts"]["accepted_live"]["post_fee_pnl_usd"],
                "denied_counterfactual_post_fee_pnl_usd": report["cohorts"]["denied_counterfactual"][
                    "post_fee_pnl_usd"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
