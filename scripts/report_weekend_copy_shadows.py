#!/usr/bin/env python3
"""Build preregistered weekend-skew and FAK-requote shadow packets.

Flow stage: HUNT/PROMOTE_PREP. Read-only analysis: this module never changes
live configuration, selection, sizing, or order submission.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_live_order_reject_attribution import (  # noqa: E402
    _default_resolutions_path,
    _market_window_start_s,
    _num,
    _parse_ts,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_REJECT_REPORT = "data/research/live_order_reject_attribution_latest.json"
DEFAULT_REGISTRY = "data/research/experiment_preregistry.jsonl"
WINDOW_OUTPUT = "data/research/paper_copy_weekend_window_sign_skew_latest.json"
HOUR_OUTPUT = "data/research/paper_copy_weekend_hour_of_day_skew_latest.json"
FAK_OUTPUT = "data/research/paper_copy_fak_nomatch_requote_latest.json"
WINDOW_EXPERIMENT = "copy-weekend-window-sign-skew-shadow-20260720"
HOUR_EXPERIMENT = "copy-weekend-hour-of-day-skew-shadow-20260720"
FAK_EXPERIMENT = "copy-fak-nomatch-requote-shadow-20260720"


def _rooted(path: str | Path) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _registration_times(path: Path) -> dict[str, str]:
    found: dict[str, str] = {}
    if not path.exists():
        return found
    import json

    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        experiment_id = str(row.get("experiment_id") or "")
        if experiment_id in {WINDOW_EXPERIMENT, HOUR_EXPERIMENT, FAK_EXPERIMENT}:
            found[experiment_id] = str(row.get("registered_at") or "")
    return found


def _status(order: dict[str, Any]) -> str:
    return str(order.get("final_status") or order.get("status") or "").upper()


def _price_band(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def _offset_band(offset_s: float) -> str:
    if offset_s < 60:
        return "00_000_060"
    if offset_s < 120:
        return "01_060_120"
    if offset_s < 180:
        return "02_120_180"
    return "03_180_300"


def _source_size(order: dict[str, Any]) -> float:
    source = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
    inventory = metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {}
    return _num(source.get("wallet_usdc_size"), _num(inventory.get("source_inventory_usd")))


def _source_size_band(value: float) -> str:
    if value < 5:
        return "00_0_5"
    if value < 20:
        return "01_5_20"
    return "02_20_inf"


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = round(sum(_num(row.get("pnl_usd")) for row in rows), 6)
    cost = round(sum(_num(row.get("cost_usd")) for row in rows), 6)
    return {
        "windows": len(rows),
        "positive": sum(_num(row.get("pnl_usd")) > 0 for row in rows),
        "negative": sum(_num(row.get("pnl_usd")) < 0 for row in rows),
        "pnl_usd": pnl,
        "cost_usd": cost,
        "roi_pct": round(pnl / cost * 100.0, 6) if cost else None,
    }


def _group(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "unknown")].append(row)
    return [{key: name, **_summary(items)} for name, items in sorted(grouped.items())]


def _window_rows(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    discovery_cutoff_ts: float,
    discovery_day_utc: str,
) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in orders:
        if _status(order) != "FILLED":
            continue
        submitted_ts = _parse_ts(order.get("submitted_at"))
        start_s = _market_window_start_s(order.get("market_slug"))
        if submitted_ts is None or start_s is None:
            continue
        if submitted_ts > discovery_cutoff_ts:
            continue
        # The governing weekend probe is a UTC-day posture declared by Fable;
        # do not silently reinterpret it using the calendar weekday.
        if datetime.fromtimestamp(start_s, UTC).date().isoformat() != discovery_day_utc:
            continue
        event = score_order(order, resolutions)
        if not event.get("resolved"):
            continue
        grouped[str(order.get("market_slug") or "")].append({"order": order, "event": event, "submitted_ts": submitted_ts, "start_s": start_s})

    rows: list[dict[str, Any]] = []
    for market_slug, items in sorted(grouped.items()):
        first = min(items, key=lambda item: item["submitted_ts"])
        prices = [_num(item["event"].get("limit_price"), _num(item["order"].get("limit_price"))) for item in items]
        source_sizes = [_source_size(item["order"]) for item in items]
        offset_s = max(0.0, first["submitted_ts"] - first["start_s"])
        pnl = sum(_num(item["event"].get("pnl_usd")) for item in items)
        cost = sum(_num(item["event"].get("cost_usd")) for item in items)
        hour = datetime.fromtimestamp(first["start_s"], UTC).hour
        rows.append(
            {
                "market_slug": market_slug,
                "window_start_utc": datetime.fromtimestamp(first["start_s"], UTC).isoformat().replace("+00:00", "Z"),
                "utc_hour": hour,
                "utc_3h_block": f"{(hour // 3) * 3:02d}_{(hour // 3) * 3 + 2:02d}",
                "entry_offset_s": round(offset_s, 6),
                "entry_offset_band": _offset_band(offset_s),
                "late_window": offset_s >= 180.0,
                "mean_entry_price": round(sum(prices) / len(prices), 6),
                "price_band": _price_band(sum(prices) / len(prices)),
                "mean_source_size_usd": round(sum(source_sizes) / len(source_sizes), 6),
                "source_size_band": _source_size_band(sum(source_sizes) / len(source_sizes)),
                "fills": len(items),
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
            }
        )
    return rows


def build_packets(
    live_state: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    reject_report: dict[str, Any],
    registrations: dict[str, str],
    *,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    cutoff_candidates = [
        value
        for value in (
            _parse_ts(registrations.get(WINDOW_EXPERIMENT)),
            _parse_ts(registrations.get(HOUR_EXPERIMENT)),
        )
        if value is not None
    ]
    discovery_cutoff_ts = min(cutoff_candidates) if cutoff_candidates else (_parse_ts(generated_at) or 0.0)
    direction_day = datetime.fromtimestamp(discovery_cutoff_ts, UTC).date().isoformat()
    rows = _window_rows(
        [row for row in live_state.get("orders", []) if isinstance(row, dict)],
        resolutions,
        discovery_cutoff_ts=discovery_cutoff_ts,
        discovery_day_utc=direction_day,
    )
    cell_rows = []
    for row in rows:
        item = dict(row)
        item["cell"] = "|".join(
            [row["entry_offset_band"], row["price_band"], "late" if row["late_window"] else "timely", row["source_size_band"]]
        )
        cell_rows.append(item)
    cells = _group(cell_rows, "cell")
    discovery_cells = [row for row in cells if row["windows"] >= 10 and _num(row.get("pnl_usd")) < 0]
    direction_day_rows = [row for row in cell_rows if str(row.get("window_start_utc") or "").startswith(direction_day)]
    common = {
        "schema_version": 1,
        "flow_stage": "HUNT/PROMOTE_PREP",
        "generated_at": generated_at,
        "paper_only": True,
        "live_path_mutated": False,
        "copyintent_parity": "UNCHANGED_READ_ONLY",
    }
    window_packet = {
        **common,
        "kind": "paper_copy_weekend_window_sign_skew",
        "experiment_id": WINDOW_EXPERIMENT,
        "registered_at": registrations.get(WINDOW_EXPERIMENT),
        "fixed_schema": {
            "entry_offset_bands_s": [[0, 60], [60, 120], [120, 180], [180, 300]],
            "price_bands": [[0, 0.25], [0.25, 0.50], [0.50, 0.70], [0.70, 1.0]],
            "source_size_bands_usd": [[0, 5], [5, 20], [20, None]],
            "late_window_threshold_s": 180,
        },
        "summary": {
            **_summary(cell_rows),
            "direction_day_utc": direction_day,
            "direction_day": _summary(direction_day_rows),
            "cells": len(cells),
            "negative_discovery_cells_n10": len(discovery_cells),
            "gate": "HOLDOUT_PENDING",
        },
        "cells": cells,
        "candidate_discovery_cells": discovery_cells,
        "rows": cell_rows,
    }
    hour_packet = {
        **common,
        "kind": "paper_copy_weekend_hour_of_day_skew",
        "experiment_id": HOUR_EXPERIMENT,
        "registered_at": registrations.get(HOUR_EXPERIMENT),
        "fixed_schema": {"utc_hours": list(range(24)), "utc_3h_blocks": [f"{hour:02d}_{hour + 2:02d}" for hour in range(0, 24, 3)]},
        "summary": {
            **_summary(rows),
            "direction_day_utc": direction_day,
            "direction_day": _summary(direction_day_rows),
            "candidate_hours_n20": sum(item["windows"] >= 20 and _num(item.get("pnl_usd")) < 0 for item in _group(rows, "utc_hour")),
            "gate": "HOLDOUT_PENDING",
        },
        "by_utc_hour": _group(rows, "utc_hour"),
        "by_utc_3h_block": _group(rows, "utc_3h_block"),
    }

    registration_ts = _parse_ts(registrations.get(FAK_EXPERIMENT)) or float("inf")
    fak_rows = ((reject_report.get("fak_no_match_analysis") or {}).get("rows") or [])
    baseline: list[dict[str, Any]] = []
    prospective: list[dict[str, Any]] = []
    for row in fak_rows:
        same_window = row.get("same_window_outcome") if isinstance(row.get("same_window_outcome"), dict) else {}
        book = row.get("book_state_at_submit") if isinstance(row.get("book_state_at_submit"), dict) else {}
        item = {
            "submitted_at": row.get("submitted_at"),
            "market_slug": row.get("market_slug"),
            "outcome": row.get("outcome"),
            "submit_to_window_close_s": row.get("submit_to_window_close_s"),
            "book_state_verdict": book.get("verdict"),
            "same_window_eventual_fill": bool(same_window.get("same_window_eventual_fill")),
        }
        item["mechanically_requote_eligible"] = bool(
            _num(item["submit_to_window_close_s"], -1) >= 10
            and item["book_state_verdict"] == "ask_at_or_inside_limit"
            and not item["same_window_eventual_fill"]
        )
        target = prospective if (_parse_ts(item["submitted_at"]) or 0) > registration_ts else baseline
        target.append(item)
    fak_packet = {
        **common,
        "kind": "paper_copy_fak_nomatch_requote",
        "experiment_id": FAK_EXPERIMENT,
        "registered_at": registrations.get(FAK_EXPERIMENT),
        "fixed_rule": "one same-intent re-quote; remaining_horizon>=10s; ask<=original limit; no prior same-window recovery; same size/outcome/cap",
        "summary": {
            "historical_baseline_observations": len(baseline),
            "historical_baseline_requote_eligible": sum(bool(row["mechanically_requote_eligible"]) for row in baseline),
            "prospective_observations": len(prospective),
            "prospective_requote_eligible": sum(bool(row["mechanically_requote_eligible"]) for row in prospective),
            "target_observations": 30,
            "gate": "PROSPECTIVE_ACCUMULATING",
        },
        "prospective_rows": prospective,
    }
    return window_packet, hour_packet, fak_packet


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--reject-report", default=DEFAULT_REJECT_REPORT)
    parser.add_argument("--resolutions", default=_default_resolutions_path())
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--window-output", default=WINDOW_OUTPUT)
    parser.add_argument("--hour-output", default=HOUR_OUTPUT)
    parser.add_argument("--fak-output", default=FAK_OUTPUT)
    args = parser.parse_args()
    packets = build_packets(
        load_json(_rooted(args.live_state), default={}) or {},
        load_resolutions(_rooted(args.resolutions)),
        load_json(_rooted(args.reject_report), default={}) or {},
        _registration_times(_rooted(args.registry)),
        generated_at=_iso_now(),
    )
    for output, packet in zip((args.window_output, args.hour_output, args.fak_output), packets):
        atomic_write_json(_rooted(output), packet)
    print({"window": packets[0]["summary"], "hour": packets[1]["summary"], "fak": packets[2]["summary"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
