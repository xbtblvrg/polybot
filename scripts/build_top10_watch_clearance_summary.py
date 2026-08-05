#!/usr/bin/env python3
"""Summarize the top-10 watch clearance rerun with evidence-graded statuses."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_CANDIDATES = "data/research/wallet_copy_top10_watch_clearance_candidates.json"
DEFAULT_LANE = "data/research/wallet_copy_top10_watch_clearance_lane_state.json"
DEFAULT_REPLAY = "data/research/wallet_copy_top10_watch_clearance_replay.json"
DEFAULT_MEASUREMENT = "data/research/wallet_copy_top10_watch_clearance_measurement_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_RESOLUTION_SUMMARY = "data/research/wallet_copy_top10_watch_clearance_resolution_summary.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_top10_watch_clearance_summary.json"

LANE_COVERAGE_REJECTS = {
    "book_unavailable_or_market_closed",
    "no_ask_liquidity",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES)
    parser.add_argument("--lane-state", default=DEFAULT_LANE)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--measurement", default=DEFAULT_MEASUREMENT)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--resolution-summary", default=DEFAULT_RESOLUTION_SUMMARY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _slug_start(slug: str) -> int | None:
    marker = str(slug or "").rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def _iso_from_start(start: int | None) -> str | None:
    if start is None:
        return None
    import datetime as dt

    return dt.datetime.fromtimestamp(int(start), dt.UTC).replace(microsecond=0).isoformat()


def _load_resolution_rows(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in target.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _resolution_slugs(rows: list[dict[str, Any]]) -> set[str]:
    slugs: set[str] = set()
    for row in rows:
        slug = str(row.get("market_slug") or row.get("slug") or "")
        if slug:
            slugs.add(slug)
            continue
        expiry = int(num(row.get("expiry_unix_ts"), 0))
        if expiry > 0 and str(row.get("window_type") or "").lower() in {"5m", "5min", "5_minute", ""}:
            slugs.add(f"btc-updown-5m-{expiry - 300}")
    return slugs


def _wallet_index(payload: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    rows = payload.get(key) if isinstance(payload.get(key), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet:
            out[wallet] = row
    return out


def _measurement_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    wallets = payload.get("wallets") if isinstance(payload.get("wallets"), dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for key, row in wallets.items():
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or key)
        if wallet:
            out[wallet] = row
    ranked = payload.get("ranked_wallets") if isinstance(payload.get("ranked_wallets"), list) else []
    for row in ranked:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet:
            out[wallet] = row
    return out


def _replay_orders(replay: dict[str, Any]) -> list[dict[str, Any]]:
    rows = replay.get("replay_orders") if isinstance(replay.get("replay_orders"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _order_slug(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return str(order.get("market_slug") or source_intent.get("market_slug") or "")


def _order_status(order: dict[str, Any]) -> str:
    return str(order.get("final_status") or order.get("status") or "").upper()


def _coverage_log(
    *,
    orders: list[dict[str, Any]],
    available_resolution_slugs: set[str],
) -> dict[str, Any]:
    replay_slugs = sorted({slug for slug in (_order_slug(order) for order in orders) if slug})
    filled_slugs = sorted({_order_slug(order) for order in orders if _order_status(order) == "FILLED" and _order_slug(order)})
    all_starts = [_slug_start(slug) for slug in replay_slugs]
    all_starts = [start for start in all_starts if start is not None]
    filled_starts = [_slug_start(slug) for slug in filled_slugs]
    filled_starts = [start for start in filled_starts if start is not None]
    covered = sorted(slug for slug in replay_slugs if slug in available_resolution_slugs)
    missing = sorted(slug for slug in replay_slugs if slug not in available_resolution_slugs)
    covered_filled = sorted(slug for slug in filled_slugs if slug in available_resolution_slugs)
    missing_filled = sorted(slug for slug in filled_slugs if slug not in available_resolution_slugs)
    if not replay_slugs:
        status = "NO_REPLAY_ORDERS"
    elif not missing:
        status = "COVERED"
    elif covered:
        status = "PARTIAL"
    else:
        status = "MISSING"
    return {
        "status": status,
        "replay_order_windows": len(replay_slugs),
        "filled_order_windows": len(filled_slugs),
        "covered_replay_windows": len(covered),
        "covered_filled_windows": len(covered_filled),
        "missing_replay_windows": len(missing),
        "missing_filled_windows": len(missing_filled),
        "replay_window_start_min": min(all_starts) if all_starts else None,
        "replay_window_start_max": max(all_starts) if all_starts else None,
        "replay_window_start_min_iso": _iso_from_start(min(all_starts) if all_starts else None),
        "replay_window_start_max_iso": _iso_from_start(max(all_starts) if all_starts else None),
        "filled_window_start_min": min(filled_starts) if filled_starts else None,
        "filled_window_start_max": max(filled_starts) if filled_starts else None,
        "filled_window_start_min_iso": _iso_from_start(min(filled_starts) if filled_starts else None),
        "filled_window_start_max_iso": _iso_from_start(max(filled_starts) if filled_starts else None),
        "covered_replay_market_slugs": covered[:100],
        "missing_replay_market_slugs": missing[:100],
        "missing_filled_market_slugs": missing_filled[:100],
    }


def _reject_reasons(row: dict[str, Any]) -> dict[str, int]:
    raw = row.get("reject_reasons") if isinstance(row.get("reject_reasons"), dict) else {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        count = int(num(value, 0))
        if count > 0:
            out[str(key)] = count
    return dict(sorted(out.items()))


def classify_clearance(
    *,
    replay: dict[str, Any],
    measurement: dict[str, Any],
) -> tuple[str, str]:
    """Return an evidence-graded clearance status and reason."""

    replay_status = str(replay.get("eligibility_status") or replay.get("status") or "").upper()
    replay_pnl = num(replay.get("paper_pnl_usd"), 0.0)
    resolved_orders = int(replay.get("resolved_orders") or 0)
    replay_copyable = int(replay.get("copyable_buy_events") or 0)
    measurement_copyable = int(measurement.get("copyable_buy_events") or 0)
    copyable_sample = max(replay_copyable, measurement_copyable)
    buy_events = int(measurement.get("buy_events") or 0)
    unresolved_ratio = num(replay.get("unresolved_ratio"), 0.0)
    rejects = _reject_reasons(measurement)
    reject_keys = set(rejects)

    if replay_status == "PASS" and replay_pnl > 0.0:
        return "DEFINITIVE_PASS_PAPER_ONLY", "positive_resolved_replay"
    if copyable_sample >= 5 and resolved_orders > 0 and replay_pnl < 0.0:
        return "DEFINITIVE_FAIL_NEGATIVE_EDGE", "negative_resolved_replay"
    if buy_events <= 0:
        return "ANALYZE_NO_RECENT_BUY_SAMPLE", "no_recent_realtime_buy_sample"
    if resolved_orders <= 0 and unresolved_ratio >= 1.0:
        return "UNMEASURABLE_RESOLUTION_BLIND_SPOT", "all_replay_orders_unresolved"
    if reject_keys and reject_keys <= LANE_COVERAGE_REJECTS and measurement_copyable <= 0:
        if resolved_orders > 0:
            return "FAIL_LANE_COVERAGE", "all_recent_buys_unfillable_in_current_lane"
        return "UNMEASURABLE_LANE_COVERAGE", "lane_coverage_rejects_without_resolved_edge"
    if resolved_orders <= 0:
        return "UNMEASURABLE_RESOLUTION_BLIND_SPOT", "no_resolved_replay_orders"
    return "ANALYZE_PARTIAL_SAMPLE", "sample_not_decisive"


def _row_for_wallet(
    *,
    wallet: str,
    candidate: dict[str, Any],
    lane_row: dict[str, Any],
    replay_candidate: dict[str, Any],
    measurement: dict[str, Any],
    available_resolution_slugs: set[str],
) -> dict[str, Any]:
    replay = replay_candidate.get("paper_replay") if isinstance(replay_candidate.get("paper_replay"), dict) else {}
    orders = _replay_orders(replay)
    clearance_status, reason = classify_clearance(replay=replay, measurement=measurement)
    return {
        "wallet": wallet,
        "candidate_id": candidate.get("candidate_id") or replay_candidate.get("candidate_id") or lane_row.get("name") or "",
        "source_queue_rank": candidate.get("source_queue_rank") or lane_row.get("queue_rank"),
        "clearance_status": clearance_status,
        "clearance_reason": reason,
        "sample_status": measurement.get("sample_status") or "",
        "buy_events": int(measurement.get("buy_events") or 0),
        "copyable_buy_events": int(measurement.get("copyable_buy_events") or 0),
        "copyable_rate_pct": measurement.get("copyable_rate_pct"),
        "measurement_paper_pnl_usd": num(measurement.get("paper_pnl_usd"), 0.0),
        "reject_reasons": _reject_reasons(measurement),
        "seed_replay": candidate.get("paper_replay_seed") if isinstance(candidate.get("paper_replay_seed"), dict) else {},
        "replay": {
            "candidate_id": replay_candidate.get("candidate_id") or "",
            "replay_status": replay.get("eligibility_status") or replay.get("status") or "",
            "failure_reasons": [
                str(reason)
                for reason in (replay.get("failure_reasons") or [])
                if str(reason)
            ],
            "paper_orders": int(replay.get("paper_orders") or 0),
            "resolved_orders": int(replay.get("resolved_orders") or 0),
            "paper_pnl_usd": num(replay.get("paper_pnl_usd"), 0.0),
            "copyable_buy_events": int(replay.get("copyable_buy_events") or 0),
            "candidate_clob_backed_orders": int(replay.get("candidate_clob_backed_orders") or 0),
            "unresolved_ratio": replay.get("unresolved_ratio"),
            "candidate_rejected_fill_count": int(replay.get("candidate_rejected_fill_count") or 0),
            "candidate_rejected_fill_ratio": replay.get("candidate_rejected_fill_ratio"),
        },
        "resolution_window_coverage": _coverage_log(
            orders=orders,
            available_resolution_slugs=available_resolution_slugs,
        ),
    }


def build_summary(
    *,
    candidates_payload: dict[str, Any],
    lane_state: dict[str, Any],
    replay_payload: dict[str, Any],
    measurement_state: dict[str, Any],
    resolution_rows: list[dict[str, Any]],
    resolution_summary: dict[str, Any],
    resolutions_path: str,
) -> dict[str, Any]:
    candidate_index = _wallet_index(candidates_payload, "candidates")
    lane_index = _wallet_index(lane_state, "ranked_wallets")
    replay_index = _wallet_index(replay_payload, "candidates")
    measurement_index = _measurement_index(measurement_state)
    ordered_wallets = [_norm_wallet(row.get("wallet")) for row in candidates_payload.get("candidates") or [] if isinstance(row, dict)]
    ordered_wallets = [wallet for wallet in ordered_wallets if wallet]
    available_resolution_slugs = _resolution_slugs(resolution_rows)
    rows = [
        _row_for_wallet(
            wallet=wallet,
            candidate=candidate_index.get(wallet, {}),
            lane_row=lane_index.get(wallet, {}),
            replay_candidate=replay_index.get(wallet, {}),
            measurement=measurement_index.get(wallet, {}),
            available_resolution_slugs=available_resolution_slugs,
        )
        for wallet in ordered_wallets
    ]
    status_counts = Counter(str(row.get("clearance_status") or "") for row in rows)
    definitive_pass_count = int(status_counts.get("DEFINITIVE_PASS_PAPER_ONLY") or 0)
    definitive_fail_count = int(status_counts.get("DEFINITIVE_FAIL_NEGATIVE_EDGE") or 0)
    requested_slugs = sorted(
        {
            slug
            for row in rows
            for slug in (row.get("resolution_window_coverage") or {}).get("covered_replay_market_slugs", [])
            + (row.get("resolution_window_coverage") or {}).get("missing_replay_market_slugs", [])
        }
    )
    covered_requested = sorted(slug for slug in requested_slugs if slug in available_resolution_slugs)
    missing_requested = sorted(slug for slug in requested_slugs if slug not in available_resolution_slugs)
    return {
        "schema_version": 2,
        "kind": "wallet_copy_top10_watch_clearance_summary",
        "flow_stage": "PROMOTE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "status": "PASS",
        "taxonomy": {
            "definitive_fail_rule": (
                "copyable_sample>=5 and resolved_orders>0 and resolved replay PnL negative"
            ),
            "missing_data_rule": (
                "book_unavailable/no_ask_liquidity/unresolved_ratio=1.0 is unmeasurable unless resolved edge exists"
            ),
        },
        "summary": {
            "candidate_count": len(rows),
            "definitive_count": definitive_pass_count + definitive_fail_count,
            "definitive_pass_count": definitive_pass_count,
            "definitive_fail_count": definitive_fail_count,
            "fail_lane_coverage_count": int(status_counts.get("FAIL_LANE_COVERAGE") or 0),
            "unmeasurable_count": sum(
                count for status, count in status_counts.items() if status.startswith("UNMEASURABLE_")
            ),
            "analyze_count": sum(
                count for status, count in status_counts.items() if status.startswith("ANALYZE_")
            ),
            "clearance_status_counts": dict(sorted(status_counts.items())),
            "candidates_with_resolved_orders": sum(
                1 for row in rows if int(((row.get("replay") or {}).get("resolved_orders") or 0)) > 0
            ),
            "resolution_requested_replay_windows": len(requested_slugs),
            "resolution_covered_replay_windows": len(covered_requested),
            "resolution_missing_replay_windows": len(missing_requested),
        },
        "resolution_window_diagnosis": {
            "resolutions_path": resolutions_path,
            "resolution_rows_loaded": len(resolution_rows),
            "available_btc5m_resolution_windows": len(available_resolution_slugs),
            "requested_replay_market_slugs": requested_slugs,
            "covered_replay_market_slugs": covered_requested,
            "missing_replay_market_slugs": missing_requested,
            "resolution_summary": resolution_summary,
        },
        "measurement_summary": measurement_state.get("summary") if isinstance(measurement_state.get("summary"), dict) else {},
        "replay_summary": replay_payload.get("replay_summary") if isinstance(replay_payload.get("replay_summary"), dict) else {},
        "rows": rows,
        "next": (
            "promote only definitive passes; keep unmeasurable wallets in watch with their named missing-data cause"
        ),
    }


def main() -> int:
    args = parse_args()
    payload = build_summary(
        candidates_payload=load_json(args.candidates, default={}),
        lane_state=load_json(args.lane_state, default={}),
        replay_payload=load_json(args.replay, default={}),
        measurement_state=load_json(args.measurement, default={}),
        resolution_rows=_load_resolution_rows(args.resolutions),
        resolution_summary=load_json(args.resolution_summary, default={}),
        resolutions_path=str(args.resolutions),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
