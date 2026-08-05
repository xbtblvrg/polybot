#!/usr/bin/env python3
"""Build the E7 real-book ask distribution packet for Fable review."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _distribution(values: list[float]) -> dict[str, Any]:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return {"count": 0, "min": 0.0, "p50": 0.0, "max": 0.0}

    def percentile(pct: float) -> float:
        if len(clean) == 1:
            return clean[0]
        idx = (len(clean) - 1) * pct
        lower = math.floor(idx)
        upper = math.ceil(idx)
        if lower == upper:
            return clean[int(idx)]
        return clean[lower] + (clean[upper] - clean[lower]) * (idx - lower)

    return {
        "count": len(clean),
        "min": round(clean[0], 6),
        "p50": round(percentile(0.50), 6),
        "max": round(clean[-1], 6),
    }


def _sample_outcomes(sample: dict[str, Any]) -> dict[str, dict[str, Any]]:
    ask_sample = sample.get("ask_sample") if isinstance(sample.get("ask_sample"), dict) else {}
    outcomes = ask_sample.get("outcomes") if isinstance(ask_sample.get("outcomes"), dict) else {}
    return {str(k): v for k, v in outcomes.items() if isinstance(v, dict)}


def _sample_class(sample: dict[str, Any]) -> str:
    outcomes = _sample_outcomes(sample)
    statuses = [str(row.get("status") or "") for row in outcomes.values()]
    if not outcomes or any(status and status != "OK" for status in statuses):
        return "route_error"
    ask_count = sum(1 for row in outcomes.values() if row.get("has_ask") and _num(row.get("best_ask")) > 0.0)
    if ask_count == 0:
        return "empty"
    if ask_count == len(outcomes):
        return "populated"
    return "partial"


def _offset_key(sample: dict[str, Any]) -> str:
    offset = _num(sample.get("target_offset_s"))
    return str(int(offset)) if float(offset).is_integer() else str(round(offset, 6))


def _window_reason(window: dict[str, Any], sample_by_window: dict[int, list[dict[str, Any]]]) -> str:
    window_start = int(_num(window.get("window_start_s")))
    samples = sample_by_window.get(window_start, [])
    if samples:
        if any(_sample_class(sample) in {"empty", "partial", "populated"} for sample in samples):
            return "real_book_sampled"
        return "route_error"
    if str(window.get("best_spot_status") or "") == "SIGNAL":
        return "sampler_idle"
    return "no_signal"


def build_report(state_path: Path) -> dict[str, Any]:
    state = load_json(state_path, default={})
    if not isinstance(state, dict):
        state = {}
    samples = [row for row in state.get("ask_samples", []) if isinstance(row, dict)]
    delta_windows = [row for row in state.get("delta_windows", []) if isinstance(row, dict)]

    offsets: dict[str, dict[str, Any]] = {}
    sample_by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    unique_real_windows: set[int] = set()
    unique_sample_windows: set[int] = set()
    class_counts: Counter[str] = Counter()

    for sample in samples:
        window_start = int(_num(sample.get("window_start_s")))
        if window_start > 0:
            unique_sample_windows.add(window_start)
            sample_by_window[window_start].append(sample)
        sample_class = _sample_class(sample)
        class_counts[sample_class] += 1
        if sample_class in {"empty", "partial", "populated"} and window_start > 0:
            unique_real_windows.add(window_start)

        offset = _offset_key(sample)
        bucket = offsets.setdefault(
            offset,
            {
                "sample_events": 0,
                "unique_windows": set(),
                "real_book_unique_windows": set(),
                "empty_samples": 0,
                "partial_samples": 0,
                "populated_samples": 0,
                "route_error_samples": 0,
                "outcomes": {
                    "Up": {"real_book_samples": 0, "ask_present": 0, "best_asks": []},
                    "Down": {"real_book_samples": 0, "ask_present": 0, "best_asks": []},
                },
            },
        )
        bucket["sample_events"] += 1
        if window_start > 0:
            bucket["unique_windows"].add(window_start)
        if sample_class in {"empty", "partial", "populated"}:
            if window_start > 0:
                bucket["real_book_unique_windows"].add(window_start)
            bucket[f"{sample_class}_samples"] += 1
        else:
            bucket["route_error_samples"] += 1

        for outcome, row in _sample_outcomes(sample).items():
            if outcome not in bucket["outcomes"]:
                bucket["outcomes"][outcome] = {"real_book_samples": 0, "ask_present": 0, "best_asks": []}
            outcome_bucket = bucket["outcomes"][outcome]
            if str(row.get("status") or "") == "OK":
                outcome_bucket["real_book_samples"] += 1
            best_ask = _num(row.get("best_ask"))
            if row.get("has_ask") and best_ask > 0.0:
                outcome_bucket["ask_present"] += 1
                outcome_bucket["best_asks"].append(best_ask)

    normalized_offsets: dict[str, Any] = {}
    for offset, bucket in sorted(offsets.items(), key=lambda item: _num(item[0])):
        real_samples = int(bucket["empty_samples"] + bucket["partial_samples"] + bucket["populated_samples"])
        outcomes = {}
        for outcome, row in sorted(bucket["outcomes"].items()):
            real_count = int(row["real_book_samples"])
            ask_present = int(row["ask_present"])
            outcomes[outcome] = {
                "real_book_samples": real_count,
                "ask_present": ask_present,
                "ask_present_pct_of_real_books": round((ask_present / real_count) * 100.0, 6) if real_count else 0.0,
                "best_ask_distribution": _distribution([_num(value) for value in row["best_asks"]]),
            }
        normalized_offsets[offset] = {
            "sample_events": int(bucket["sample_events"]),
            "unique_windows": len(bucket["unique_windows"]),
            "real_book_sample_events": real_samples,
            "real_book_unique_windows": len(bucket["real_book_unique_windows"]),
            "empty_samples": int(bucket["empty_samples"]),
            "partial_samples": int(bucket["partial_samples"]),
            "populated_samples": int(bucket["populated_samples"]),
            "route_error_samples": int(bucket["route_error_samples"]),
            "populated_pct_of_real_books": round((int(bucket["populated_samples"]) / real_samples) * 100.0, 6)
            if real_samples
            else 0.0,
            "outcomes": outcomes,
        }

    window_reason_counts: Counter[str] = Counter()
    sampled_windows = {int(_num(sample.get("window_start_s"))) for sample in samples if _num(sample.get("window_start_s")) > 0}
    for window in delta_windows:
        window_reason_counts[_window_reason(window, sample_by_window)] += 1

    unsampled_windows = [
        {
            "window_start_s": int(_num(window.get("window_start_s"))),
            "market_slug": str(window.get("market_slug") or ""),
            "reason": _window_reason(window, sample_by_window),
            "best_spot_status": str(window.get("best_spot_status") or ""),
            "best_abs_delta_bps": round(_num(window.get("best_abs_delta_bps")), 6),
        }
        for window in delta_windows
        if int(_num(window.get("window_start_s"))) not in sampled_windows
    ]

    summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    ask_summary = summary.get("ask_sample_summary") if isinstance(summary.get("ask_sample_summary"), dict) else {}
    return {
        "schema_version": 1,
        "kind": "e7_ask_distribution_report",
        "generated_at": utc_now_iso(),
        "state_path": str(state_path),
        "state_updated_at": state.get("updated_at"),
        "paper_only": bool(state.get("paper_only", True)),
        "live_orders_allowed": bool(state.get("live_orders_allowed", False)),
        "zero_live_assertion": state.get("zero_live_assertion") if isinstance(state.get("zero_live_assertion"), dict) else {},
        "gate": {
            "required_unique_real_book_windows": 24,
            "current_unique_real_book_windows": len(unique_real_windows),
            "remaining_unique_real_book_windows": max(24 - len(unique_real_windows), 0),
            "ready_for_fable_ruling": len(unique_real_windows) >= 24,
        },
        "sample_totals": {
            "sample_events": len(samples),
            "unique_sample_windows": len(unique_sample_windows),
            "real_book_sample_events": sum(class_counts[k] for k in ("empty", "partial", "populated")),
            "real_book_unique_windows": len(unique_real_windows),
            "classes": dict(sorted(class_counts.items())),
            "state_summary_real_book_unique_windows": ask_summary.get("real_book_unique_windows"),
        },
        "offsets": normalized_offsets,
        "accrual_diagnosis": {
            "delta_windows": len(delta_windows),
            "window_reason_counts": dict(sorted(window_reason_counts.items())),
            "unsampled_windows": unsampled_windows[-12:],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="data/research/e7_spot_open_paper_lane_state.json")
    parser.add_argument("--out", default="data/research/e7_ask_distribution_report.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(Path(args.state))
    atomic_write_json(args.out, report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
