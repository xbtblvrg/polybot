#!/usr/bin/env python3
"""Write the R8 scheduler paper-lane closure packet.

Flow stage: LEARN/MEASURE. This is a paper-only post-mortem for the
copy-event-triggered cycle scheduler lane. It does not alter live eligibility,
caps, thresholds, rotation, or the live guard.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_STATE = "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
DEFAULT_OUTPUT = "data/research/copy_event_triggered_cycle_scheduler_closure_latest.json"
DEFAULT_PRICE_HAIRCUT_TICKS = (0.01, 0.02, 0.05)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price <= 0.50:
        return "01_25_50"
    if price <= 0.70:
        return "02_50_70"
    return "03_70_100"


def _pct(part: float, whole: float) -> float:
    return round((part / whole) * 100.0, 6) if whole else 0.0


def _summary_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cost = sum(_num(row.get("cost_usd"), 1.0) for row in rows)
    pnl = sum(_num(row.get("pnl_usd")) for row in rows)
    wins = sum(1 for row in rows if _num(row.get("pnl_usd")) > 0.0)
    losses = sum(1 for row in rows if _num(row.get("pnl_usd")) < 0.0)
    windows = {str(row.get("market_slug") or "") for row in rows if row.get("market_slug")}
    return {
        "rows": len(rows),
        "windows": len(windows),
        "wins": wins,
        "losses": losses,
        "cost_usd": round(cost, 6),
        "would_pnl_usd": round(pnl, 6),
        "roi_pct": _pct(pnl, cost),
        "win_rate_pct": _pct(float(wins), float(len(rows))),
    }


def _resolved_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    accumulator = state.get("paper_clock_accumulator")
    if not isinstance(accumulator, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key, entry in accumulator.items():
        if not isinstance(entry, dict):
            continue
        if entry.get("post_fee_would_pnl_status") != "RESOLVED_POST_FEE_MEASURED":
            continue
        cost = _num(entry.get("paper_order_size_usd"), 1.0)
        rows.append(
            {
                "event_id": entry.get("event_id") or key,
                "candidate_id": entry.get("candidate_id"),
                "source_wallet": str(entry.get("source_wallet") or "").lower(),
                "market_slug": entry.get("market_slug"),
                "outcome": entry.get("outcome"),
                "price": _num(entry.get("price")),
                "cost_usd": cost,
                "pnl_usd": _num(entry.get("post_fee_would_pnl_usd")),
                "top_of_book": _top_of_book(entry),
            }
        )
    return rows


def _top_of_book(entry: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("top_of_book", "book", "clob_book"):
        value = entry.get(key)
        if isinstance(value, dict):
            nested = value.get("top_of_book") if isinstance(value.get("top_of_book"), dict) else value
            return nested if isinstance(nested, dict) else None
    paper_order = entry.get("paper_order") if isinstance(entry.get("paper_order"), dict) else {}
    maker_quote = paper_order.get("maker_quote") if isinstance(paper_order.get("maker_quote"), dict) else {}
    top = maker_quote.get("top_of_book") if isinstance(maker_quote.get("top_of_book"), dict) else None
    return top


def _adverse_price_pnl(row: dict[str, Any], tick: float) -> float:
    cost = _num(row.get("cost_usd"), 1.0)
    raw_pnl = _num(row.get("pnl_usd"))
    price = _num(row.get("price"))
    if raw_pnl <= 0.0 or price <= 0.0:
        return raw_pnl
    adverse_price = min(0.999999, price + float(tick))
    return (cost / adverse_price) - cost


def _haircut_summaries(rows: list[dict[str, Any]], ticks: tuple[float, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    raw_pnl = sum(_num(row.get("pnl_usd")) for row in rows)
    for tick in ticks:
        pnl = sum(_adverse_price_pnl(row, tick) for row in rows)
        cost = sum(_num(row.get("cost_usd"), 1.0) for row in rows)
        out.append(
            {
                "model": "adverse_entry_price_tick",
                "tick_size_usd": round(float(tick), 6),
                "rows": len(rows),
                "cost_usd": round(cost, 6),
                "would_pnl_usd": round(pnl, 6),
                "pnl_delta_vs_raw_usd": round(pnl - raw_pnl, 6),
                "roi_pct": _pct(pnl, cost),
                "rule": "winning rows are repriced at entry_price+tick; losing rows keep full cost loss",
            }
        )
    return out


def _book_depth_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    book_rows = [row for row in rows if isinstance(row.get("top_of_book"), dict)]
    ok_rows = [
        row
        for row in book_rows
        if str((row.get("top_of_book") or {}).get("status") or "") == "OK"
    ]
    fillable_rows = []
    fillable_usd_values: list[float] = []
    for row in ok_rows:
        top = row.get("top_of_book") or {}
        fillable_usd = _num(top.get("fillable_usd"))
        fillable_usd_values.append(fillable_usd)
        if fillable_usd + 1e-9 >= _num(row.get("cost_usd"), 1.0):
            fillable_rows.append(row)
    status = "DEPTH_EVIDENCE_PRESENT" if ok_rows else "NOT_PERSISTED"
    return {
        "status": status,
        "resolved_rows": len(rows),
        "book_evidence_rows": len(book_rows),
        "top_of_book_ok_rows": len(ok_rows),
        "missing_book_evidence_rows": len(rows) - len(book_rows),
        "fillable_at_requested_size_rows": len(fillable_rows),
        "fillable_at_requested_size_pct": _pct(float(len(fillable_rows)), float(len(rows))),
        "median_fillable_usd": (
            round(float(statistics.median(fillable_usd_values)), 6) if fillable_usd_values else None
        ),
        "depth_backed_raw_would_pnl_usd": (
            round(sum(_num(row.get("pnl_usd")) for row in fillable_rows), 6) if fillable_rows else 0.0
        ),
        "evidence_rule": "top_of_book.status == OK and fillable_usd >= requested paper order size",
        "limitation": (
            "scheduler hot-history rows did not persist top_of_book depth; this packet does not infer "
            "historical depth from current books"
        ),
    }


def _group(rows: list[dict[str, Any]], key: str, *, limit: int = 12) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if key == "price_bucket":
            group_key = _price_bucket(_num(row.get("price")))
        else:
            group_key = str(row.get(key) or "")
        grouped[group_key].append(row)
    summaries = [{"key": group_key, **_summary_row(group_rows)} for group_key, group_rows in grouped.items()]
    return sorted(summaries, key=lambda item: item["would_pnl_usd"])[:limit]


def build_closure_packet(
    state: dict[str, Any],
    *,
    generated_at: str,
    price_haircut_ticks: tuple[float, ...] = DEFAULT_PRICE_HAIRCUT_TICKS,
) -> dict[str, Any]:
    rows = _resolved_rows(state)
    raw = _summary_row(rows)
    book_depth = _book_depth_summary(rows)
    state_summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    raw_gate_metric = _num(state_summary.get("paper_clock_post_fee_would_pnl_usd"), raw["would_pnl_usd"])
    verdict = (
        "R18_FAIL_CONFIRMED_RAW_NEGATIVE"
        if raw_gate_metric <= 0.0
        else "R18_FAIL_NOT_FLIPPED_BOOK_EVIDENCE_MISSING"
    )
    return {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_closure",
        "flow_stage": "LEARN/MEASURE",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintent_parity_change": False,
        "single_submitter_change": False,
        "guard_code_touched": False,
        "source_state": {
            "kind": state.get("kind"),
            "generated_at": state.get("generated_at"),
            "status": state.get("status"),
            "clock_start_utc": state.get("clock_start_utc"),
            "clock_end_utc": state.get("clock_end_utc"),
            "paper_clock_rows_landed": state_summary.get("paper_clock_rows_landed"),
            "paper_clock_rows_resolved": state_summary.get("paper_clock_rows_resolved"),
            "paper_clock_post_fee_would_pnl_usd": state_summary.get("paper_clock_post_fee_would_pnl_usd"),
        },
        "closure_verdict": verdict,
        "ruling18_flip_allowed": False,
        "decision_use": "post_mortem_learning_only",
        "raw_vs_haircut": {
            "raw_gate_metric": raw,
            "adverse_price_haircuts": _haircut_summaries(rows, price_haircut_ticks),
        },
        "top_of_book_depth_evidence": book_depth,
        "decomposition": {
            "by_price_bucket_worst_first": _group(rows, "price_bucket"),
            "by_source_wallet_worst_first": _group(rows, "source_wallet"),
        },
        "conclusion": (
            "R18 fail stands: the preregistered raw paper-clock metric is negative; "
            "top-of-book depth was not persisted for this lane, so no positive fill-realism claim is citable."
        ),
        "next_hypothesis_constraints": [
            "do not reseed identical R8 parameters",
            "any new scheduler hypothesis must persist top_of_book depth at trigger time before promotion claims",
            "state changed trigger set, fee model, or member filter before opening a new 48h clock",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--price-haircut-ticks",
        default=",".join(str(item) for item in DEFAULT_PRICE_HAIRCUT_TICKS),
        help="Comma-separated adverse entry price ticks to summarize.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ticks = tuple(float(item) for item in str(args.price_haircut_ticks).split(",") if item.strip())
    state_path = Path(args.state)
    output_path = Path(args.output)
    payload = build_closure_packet(load_json(state_path, default={}) or {}, generated_at=_utc_now_iso(), price_haircut_ticks=ticks)
    payload["inputs"] = {
        "state": str(state_path),
        "output": str(output_path),
        "price_haircut_ticks": list(ticks),
    }
    atomic_write_json(output_path, payload)
    print(
        json.dumps(
            {
                "status": payload["closure_verdict"],
                "raw_would_pnl_usd": payload["raw_vs_haircut"]["raw_gate_metric"]["would_pnl_usd"],
                "book_evidence_rows": payload["top_of_book_depth_evidence"]["book_evidence_rows"],
                "missing_book_evidence_rows": payload["top_of_book_depth_evidence"]["missing_book_evidence_rows"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
