#!/usr/bin/env python3
"""Build the preregistered offline fee-edge slice decomposition."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

EXPERIMENT_ID = "fee-edge-decomposition-20260720"
DEFAULT_SOURCE = "data/research/routing_shadow_validation_latest.json"
DEFAULT_REGISTRY = "data/research/experiment_preregistry.jsonl"
DEFAULT_OUTPUT = "data/research/fee_edge_decomposition_latest.json"
PRICE_BINS = ((0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01))
SECONDS_BINS = ((0.0, 30.0), (30.0, 60.0), (60.0, 120.0), (120.0, 180.0), (180.0, 301.0))
NOTIONAL_BINS = ((0.0, 1.0), (1.0, 2.0), (2.0, 4.0), (4.0, math.inf))


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _load_preregistration(path: Path) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if isinstance(row, dict) and row.get("experiment_id") == EXPERIMENT_ID:
                matches.append(row)
    if not matches or matches[-1].get("status") != "PREREGISTERED":
        raise ValueError(f"missing active preregistration: {EXPERIMENT_ID}")
    return matches[-1]


def _bin_label(value: float, bins: tuple[tuple[float, float], ...]) -> str | None:
    for low, high in bins:
        if low <= value < high:
            high_label = "inf" if math.isinf(high) else f"{high:g}"
            return f"[{low:g},{high_label})"
    return None


def _normalize(row: dict[str, Any], cohort: str) -> tuple[dict[str, Any] | None, str | None]:
    outcome = row.get("realized_paper_outcome")
    if not isinstance(outcome, dict) or str(outcome.get("status") or "").upper() != "RESOLVED":
        return None, "unresolved"
    values = (outcome.get("paper_pnl_usd"), row.get("expected_fee_usd"), row.get("limit_price"), row.get("shares"))
    if any(value is None for value in values):
        return None, "missing_measurement"
    try:
        pre_fee, fee, price, shares = (float(value) for value in values)
        window_start = float(row.get("window_start_s"))
        observed = float(row.get("observed_ts") if cohort == "fee_cal_measured" else row.get("winning_observed_ts"))
    except (TypeError, ValueError):
        return None, "invalid_numeric"
    seconds_remaining = window_start + 300.0 - observed
    if not 0.0 <= seconds_remaining < 301.0:
        return None, "invalid_seconds_remaining"
    wallet = str(row.get("source_wallet") if cohort == "fee_cal_measured" else row.get("winning_source_wallet") or "")
    market = str(row.get("market_slug") or "")
    if not wallet or not market:
        return None, "missing_identity"
    return {
        "window": market,
        "wallet": wallet.lower(),
        "observed_ts": observed,
        "intent_id": str(row.get("intent_id") if cohort == "fee_cal_measured" else row.get("winning_intent_id") or ""),
        "entry_price": price,
        "seconds_remaining": seconds_remaining,
        "tranche_notional_usd": price * shares,
        "pre_fee_pnl_usd": pre_fee,
        "expected_fee_usd": fee,
        "post_fee_pnl_usd": pre_fee - fee,
    }, None


def _slice_label(axis: str, row: dict[str, Any]) -> str | None:
    if axis == "entry_price_band":
        return _bin_label(row["entry_price"], PRICE_BINS)
    if axis == "seconds_remaining_at_fill":
        return _bin_label(row["seconds_remaining"], SECONDS_BINS)
    if axis == "tranche_notional_usd":
        return _bin_label(row["tranche_notional_usd"], NOTIONAL_BINS)
    return str(row["wallet"])


def _decompose(rows: list[dict[str, Any]], cohort: str) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    excluded: dict[str, int] = {}
    for row in rows:
        item, reason = _normalize(row, cohort)
        if item is None:
            excluded[reason or "unknown"] = excluded.get(reason or "unknown", 0) + 1
        else:
            normalized.append(item)

    axes: dict[str, list[dict[str, Any]]] = {}
    for axis in ("entry_price_band", "seconds_remaining_at_fill", "tranche_notional_usd", "source_wallet"):
        selected: dict[tuple[str, str], dict[str, Any]] = {}
        for row in normalized:
            label = _slice_label(axis, row)
            if label is None:
                continue
            key = (label, row["window"])
            order = (row["observed_ts"], row["wallet"], row["intent_id"])
            incumbent = selected.get(key)
            if incumbent is None or order < incumbent["_order"]:
                selected[key] = {**row, "_order": order}
        grouped: dict[str, list[dict[str, Any]]] = {}
        for (label, _window), row in selected.items():
            grouped.setdefault(label, []).append(row)
        slices = []
        for label, members in sorted(grouped.items()):
            pre = sum(row["pre_fee_pnl_usd"] for row in members)
            fee = sum(row["expected_fee_usd"] for row in members)
            post = sum(row["post_fee_pnl_usd"] for row in members)
            slices.append(
                {
                    "slice": label,
                    "n_resolved_windows": len(members),
                    "pre_fee_pnl_usd": round(pre, 6),
                    "expected_fee_usd": round(fee, 6),
                    "post_fee_pnl_usd": round(post, 6),
                    "success": len(members) >= 100 and post > 0.0,
                }
            )
        axes[axis] = slices
    return {
        "input_rows": len(rows),
        "normalized_resolved_rows": len(normalized),
        "excluded_rows": dict(sorted(excluded.items())),
        "axes": axes,
    }


def build_report(source: dict[str, Any], preregistration: dict[str, Any]) -> dict[str, Any]:
    fee_rows = [row for row in source.get("fee_gated_measurement_rows", []) if isinstance(row, dict)]
    would_rows = [
        row
        for row in source.get("rows", [])
        if isinstance(row, dict) and row.get("extra_would_submit_window") is True
    ]
    cohorts = {
        "fee_cal_measured": _decompose(fee_rows, "fee_cal_measured"),
        "routing_shadow_extra_would": _decompose(would_rows, "routing_shadow_extra_would"),
    }
    winners = []
    for cohort, detail in cohorts.items():
        for axis, slices in detail["axes"].items():
            for item in slices:
                if item["success"]:
                    winners.append({"cohort": cohort, "axis": axis, **item})
    return {
        "schema_version": 1,
        "kind": "fee_edge_decomposition",
        "flow_stage": "LEARN",
        "generated_at": _utc_now(),
        "experiment_id": EXPERIMENT_ID,
        "preregistered_at": preregistration.get("registered_at"),
        "source_generated_at": source.get("generated_at"),
        "measurement_only": True,
        "live_mutation": False,
        "success_rule": "post_fee_pnl_usd > 0 and n_resolved_windows >= 100",
        "dedupe_rule": "earliest observed timestamp, then wallet, then intent per cohort/axis/slice/window",
        "bins": {
            "entry_price_band": ["[0,0.2)", "[0.2,0.4)", "[0.4,0.6)", "[0.6,0.8)", "[0.8,1.01)"],
            "seconds_remaining_at_fill": ["[0,30)", "[30,60)", "[60,120)", "[120,180)", "[180,301)"],
            "tranche_notional_usd": ["[0,1)", "[1,2)", "[2,4)", "[4,inf)"],
            "source_wallet": "categorical",
        },
        "cohorts": cohorts,
        "winner_count": len(winners),
        "verdict": "WINNER" if winners else "NONE",
        "winners": winners,
        "guardrail": "No live gate, threshold, roster, policy, or size change; candidates require explicit Fable direction.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    preregistration = _load_preregistration(_rooted(args.registry))
    source = _load_json(_rooted(args.source))
    report = build_report(source, preregistration)
    atomic_write_json(_rooted(args.output), report)
    print(json.dumps({"verdict": report["verdict"], "winner_count": report["winner_count"], "output": args.output}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
