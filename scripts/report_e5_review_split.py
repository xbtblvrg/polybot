#!/usr/bin/env python3
"""Report the E5 aggregate/book-aware/non-fallback split for review packets."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_maker_first_btc5m_paper_lane import (  # noqa: E402
    _book_evidence,
    _route_uses_direct_fallback,
    _summarize_scored_orders,
)
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def _load_dict(path: Path) -> dict[str, Any]:
    loaded = load_json(path, default={})
    return loaded if isinstance(loaded, dict) else {}


def _copy_snapshot(
    *,
    paper_state: Path,
    book_aware_state: Path,
    snapshot_dir: Path,
    refresh_snapshot: bool = False,
) -> dict[str, Any]:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    paper_target = snapshot_dir / paper_state.name
    book_target = snapshot_dir / book_aware_state.name
    reused_existing = paper_target.exists() and book_target.exists() and not refresh_snapshot
    if not reused_existing:
        shutil.copy2(paper_state, paper_target)
        shutil.copy2(book_aware_state, book_target)
    return {
        "paper_state": str(paper_target),
        "book_aware_state": str(book_target),
        "reused_existing": reused_existing,
        "refresh_snapshot": refresh_snapshot,
    }


def build_report(
    *,
    paper_state_path: Path,
    book_aware_state_path: Path,
    snapshot_dir: Path | None = None,
    refresh_snapshot: bool = False,
) -> dict[str, Any]:
    snapshot_paths: dict[str, Any] = {}
    if snapshot_dir is not None:
        snapshot_paths = _copy_snapshot(
            paper_state=paper_state_path,
            book_aware_state=book_aware_state_path,
            snapshot_dir=snapshot_dir,
            refresh_snapshot=refresh_snapshot,
        )
        paper_state_path = Path(snapshot_paths["paper_state"])
        book_aware_state_path = Path(snapshot_paths["book_aware_state"])

    paper_state = _load_dict(paper_state_path)
    book_state = _load_dict(book_aware_state_path)
    paper_orders = paper_state.get("orders") if isinstance(paper_state.get("orders"), list) else []
    scored_orders = book_state.get("scored_orders") if isinstance(book_state.get("scored_orders"), list) else []
    orders_by_id = {str(order.get("order_id") or ""): order for order in paper_orders if isinstance(order, dict)}
    non_fallback_scored: list[dict[str, Any]] = []
    fallback_reason_counts: dict[str, int] = {}
    for order in orders_by_id.values():
        book = _book_evidence(order)
        if not isinstance(book, dict):
            continue
        if _route_uses_direct_fallback(book):
            route = book.get("route_report") if isinstance(book.get("route_report"), dict) else {}
            reason = str(route.get("fallback_source") or route.get("route_class") or "direct_fallback")
            fallback_reason_counts[reason] = fallback_reason_counts.get(reason, 0) + 1
    for scored in scored_orders:
        if not isinstance(scored, dict):
            continue
        order = orders_by_id.get(str(scored.get("order_id") or ""))
        if not isinstance(order, dict):
            continue
        book = _book_evidence(order)
        if not isinstance(book, dict):
            continue
        if _route_uses_direct_fallback(book):
            continue
        non_fallback_scored.append(scored)

    state_non_fallback_summary = (
        book_state.get("non_fallback_summary") if isinstance(book_state.get("non_fallback_summary"), dict) else None
    )
    if state_non_fallback_summary is not None:
        non_fallback_summary = dict(state_non_fallback_summary)
    else:
        non_fallback_summary = _summarize_scored_orders(non_fallback_scored, non_fallback_scored, non_fallback_scored)
    required = 50
    non_fallback_summary["non_fallback_book_evidence_resolved_required"] = required
    non_fallback_summary["non_fallback_book_evidence_positive"] = bool(
        non_fallback_summary["resolved_paper_fills"] >= required
        and float(non_fallback_summary["resolved_paper_pnl_usd"]) >= 0.0
    )
    prospective_summary = (
        book_state.get("prospective_no_fallback_summary")
        if isinstance(book_state.get("prospective_no_fallback_summary"), dict)
        else {}
    )
    promotion_gate = book_state.get("promotion_gate") if isinstance(book_state.get("promotion_gate"), dict) else {}
    return {
        "schema_version": 1,
        "kind": "e5_review_split",
        "generated_at": utc_now_iso(),
        "paper_state": str(paper_state_path),
        "book_aware_state": str(book_aware_state_path),
        "snapshot_paths": snapshot_paths,
        "aggregate_summary": paper_state.get("summary") if isinstance(paper_state.get("summary"), dict) else {},
        "book_aware_full_summary": book_state.get("summary") if isinstance(book_state.get("summary"), dict) else {},
        "book_aware_non_fallback_summary": non_fallback_summary,
        "book_aware_prospective_no_fallback_summary": prospective_summary,
        "promotion_gate": promotion_gate,
        "fallback_reason_histogram": dict(sorted(fallback_reason_counts.items())),
        "review_rule": (
            "Fable 2026-07-07T08:28Z active gate: promote only after >=150 prospectively-resolved "
            "enforced-no-fallback fills with PnL > 0 and maker_fill_rate >= 90%; fail after 300 resolved."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-state", default="data/research/maker_first_btc5m_paper_state.json")
    parser.add_argument("--book-aware-state", default="data/research/maker_first_btc5m_book_aware_state.json")
    parser.add_argument("--snapshot-dir", default="")
    parser.add_argument(
        "--refresh-snapshot",
        action="store_true",
        help="Overwrite snapshot files instead of reusing an existing frozen copy.",
    )
    parser.add_argument("--out", default="data/research/e5_review_split_latest.json")
    parser.add_argument(
        "--latest-out",
        default="data/research/e5_review_split_latest.json",
        help="Canonical latest pointer to refresh in addition to --out.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(
        paper_state_path=Path(args.paper_state),
        book_aware_state_path=Path(args.book_aware_state),
        snapshot_dir=Path(args.snapshot_dir) if args.snapshot_dir else None,
        refresh_snapshot=bool(args.refresh_snapshot),
    )
    atomic_write_json(args.out, report)
    if str(args.latest_out) and Path(args.latest_out) != Path(args.out):
        atomic_write_json(args.latest_out, report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
