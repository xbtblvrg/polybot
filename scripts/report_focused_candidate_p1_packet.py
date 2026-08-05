#!/usr/bin/env python3
"""Build a bounded, paper-only P1 packet for one market-cohort candidate."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import hashlib
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_strategy_decompiler_intake import (  # noqa: E402
    _default_resolutions_path,
    _float,
    _load_resolutions,
    _norm_outcome,
    _parse_ts,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WALLET = "0x8a47951a3cefcc98dc8b41eb438d1c49249872ef"
DEFAULT_PACKETS = "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_HISTORY = "data/research/source_active_policy_history_8a47951a3c_focused_8a47_20260720T0536Z.json"
DEFAULT_TEMPORAL = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_LIVE_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = "data/research/focused_candidate_p1_8a47951a3c_latest.json"
COMPLETE_STOPS = {"empty_page", "short_page", "lookback_cutoff_reached"}


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _packet_row(payload: dict[str, Any], wallet: str) -> dict[str, Any]:
    return next(
        (
            row
            for row in payload.get("packets") or []
            if isinstance(row, dict) and _norm_wallet(row.get("wallet")) == wallet
        ),
        {},
    )


def _temporal_row(payload: dict[str, Any], wallet: str) -> dict[str, Any]:
    return next(
        (
            row
            for row in payload.get("wallets") or []
            if isinstance(row, dict) and _norm_wallet(row.get("wallet")) == wallet
        ),
        {},
    )


def _event_key(row: dict[str, Any]) -> tuple[str, str, str, float]:
    return (
        str(row.get("transaction_hash") or row.get("transactionHash") or ""),
        str(row.get("market_slug") or row.get("event_slug") or ""),
        _norm_outcome(row.get("outcome")),
        round(_parse_ts(row.get("event_ts") or row.get("timestamp")), 3),
    )


def _resolved_events(history: dict[str, Any], winners: dict[str, str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, float]] = set()
    for row in history.get("events") or []:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        key = _event_key(row)
        if key in seen:
            continue
        seen.add(key)
        slug = str(row.get("market_slug") or row.get("event_slug") or "")
        condition = str(row.get("condition_id") or "")
        winner = winners.get(slug) or winners.get(condition)
        outcome = _norm_outcome(row.get("outcome"))
        price = _float(row.get("price"), 0.0)
        size = _float(row.get("size"), 0.0)
        if not winner or not outcome or price <= 0.0 or price >= 1.0 or size <= 0.0:
            continue
        stake = price * size
        pnl = (1.0 - price) * size if outcome == winner else -stake
        out.append({"market_slug": slug or condition, "stake_usd": stake, "pnl_usd": pnl})
    return out


def _concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_market: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"resolved_trades": 0, "stake_usd": 0.0, "pnl_usd": 0.0}
    )
    for row in rows:
        market = str(row.get("market_slug") or "")
        agg = by_market[market]
        agg["resolved_trades"] = int(agg["resolved_trades"]) + 1
        agg["stake_usd"] = float(agg["stake_usd"]) + _float(row.get("stake_usd"), 0.0)
        agg["pnl_usd"] = float(agg["pnl_usd"]) + _float(row.get("pnl_usd"), 0.0)
    ranked = sorted(
        ({"market_slug": market, **values} for market, values in by_market.items()),
        key=lambda row: (-float(row["pnl_usd"]), str(row["market_slug"])),
    )
    positive_total = sum(max(0.0, float(row["pnl_usd"])) for row in ranked)
    top1 = sum(max(0.0, float(row["pnl_usd"])) for row in ranked[:1])
    top3 = sum(max(0.0, float(row["pnl_usd"])) for row in ranked[:3])
    top1_share = (top1 / positive_total * 100.0) if positive_total > 0.0 else None
    top3_share = (top3 / positive_total * 100.0) if positive_total > 0.0 else None
    discounted = top1_share is not None and top1_share > 50.0
    return {
        "basis": "share_of_gross_positive_pnl; defined even when net PnL is negative",
        "unique_markets": len(ranked),
        "gross_positive_pnl_usd": round(positive_total, 6),
        "top1_positive_pnl_share_pct": round(top1_share, 6) if top1_share is not None else None,
        "top3_positive_pnl_share_pct": round(top3_share, 6) if top3_share is not None else None,
        "concentration_discounted": discounted,
        "threshold_rule": "top1_positive_pnl_share_pct > 50",
        "top_markets": [
            {
                **row,
                "stake_usd": round(float(row["stake_usd"]), 6),
                "pnl_usd": round(float(row["pnl_usd"]), 6),
            }
            for row in ranked[:10]
        ],
    }


def _runtime_lane_hours(live_ledger: dict[str, Any]) -> dict[str, Any]:
    orders = [row for row in live_ledger.get("orders") or [] if isinstance(row, dict)]
    filled = [row for row in orders if str(row.get("final_status") or "").upper() == "FILLED"]
    latest = max(filled, key=lambda row: _parse_ts(row.get("submitted_at") or row.get("updated_at")), default={})
    wallet = _norm_wallet(latest.get("source_wallet") or (latest.get("source_intent") or {}).get("source_wallet"))
    policy = str((latest.get("source_intent") or {}).get("policy_id") or latest.get("policy_id") or "")
    counts: dict[str, int] = defaultdict(int)
    for row in filled:
        row_wallet = _norm_wallet(row.get("source_wallet") or (row.get("source_intent") or {}).get("source_wallet"))
        if not wallet or row_wallet != wallet:
            continue
        ts = _parse_ts(row.get("submitted_at") or row.get("updated_at"))
        if ts <= 0.0:
            continue
        dt = datetime.fromtimestamp(ts, tz=UTC)
        regime = "weekend" if dt.weekday() >= 5 else "weekday"
        counts[f"{regime}:{dt.hour:02d}"] += 1
    return {"wallet": wallet, "policy_id": policy, "filled_orders": sum(counts.values()), "cells": dict(sorted(counts.items()))}


def build_report(
    *,
    wallet: str,
    packets: dict[str, Any],
    history: dict[str, Any],
    temporal: dict[str, Any],
    live_ledger: dict[str, Any],
    winners: dict[str, str],
) -> dict[str, Any]:
    packet = _packet_row(packets, wallet)
    temporal_profile = _temporal_row(temporal, wallet)
    resolved = _resolved_events(history, winners)
    new_pnl = sum(_float(row.get("pnl_usd"), 0.0) for row in resolved)
    new_stake = sum(_float(row.get("stake_usd"), 0.0) for row in resolved)
    old_pnl = _float(packet.get("paper_pnl_usd"), 0.0)
    replay = history.get("replay") if isinstance(history.get("replay"), dict) else {}
    stop_reason = str(replay.get("stop_reason") or "")
    runtime = _runtime_lane_hours(live_ledger)
    candidate_cells = temporal_profile.get("regime_hour_profiles") if isinstance(temporal_profile.get("regime_hour_profiles"), dict) else {}
    overlaps: list[dict[str, Any]] = []
    for cell, live_fills in runtime.get("cells", {}).items():
        regime, hour = cell.split(":", 1)
        profile = (candidate_cells.get(regime) or {}).get(hour) if isinstance(candidate_cells.get(regime), dict) else None
        if isinstance(profile, dict):
            overlaps.append({"cell": cell, "runtime_fills": live_fills, "candidate": profile})
    all_slice = (temporal_profile.get("slice_labels") or {}).get("all") if isinstance(temporal_profile.get("slice_labels"), dict) else {}
    concentration = _concentration(resolved)
    history_complete = stop_reason in COMPLETE_STOPS
    temporal_pass = isinstance(all_slice, dict) and all_slice.get("label") == "PROVEN-POSITIVE" and any(
        _float(row.get("candidate", {}).get("roi_pct"), 0.0) > 0.0 for row in overlaps
    )
    rotation_eligible = bool(history_complete and new_pnl > 0.0 and temporal_pass and not concentration["concentration_discounted"])
    return {
        "schema_version": 1,
        "kind": "focused_candidate_p1_packet",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": _utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": wallet,
        "history_depth": {
            "status": "COMPLETE_TO_PREREGISTERED_LOOKBACK" if history_complete else "TRUNCATED_AT_PREREGISTERED_CAP",
            "stop_reason": stop_reason,
            "lookback_cutoff_iso": replay.get("lookback_cutoff_iso"),
            "oldest_remote_event_ts": replay.get("oldest_remote_event_ts"),
            "latest_remote_event_ts": replay.get("latest_remote_event_ts"),
            "raw_rows_seen": replay.get("raw_rows_seen"),
            "normalized_btc5m_buy_events": replay.get("normalized_btc5m_buy_events"),
            "resolved_events": len(resolved),
        },
        "old_vs_new": {
            "old_packet_pnl_usd": round(old_pnl, 6),
            "old_packet_resolved": int(packet.get("resolved_copyable_events") or 0),
            "new_deep_pnl_usd": round(new_pnl, 6),
            "new_deep_stake_usd": round(new_stake, 6),
            "new_deep_resolved": len(resolved),
            "pnl_delta_usd": round(new_pnl - old_pnl, 6),
            "sign_flipped": old_pnl > 0.0 and new_pnl <= 0.0,
        },
        "temporal_hour_match": {
            "status": "PASS" if temporal_pass else "FAIL",
            "candidate_classification": temporal_profile.get("classification"),
            "candidate_all_slice": all_slice,
            "candidate_profitable_hour_bands": temporal_profile.get("profitable_hour_bands") or [],
            "runtime_lane": runtime,
            "overlap_cells": overlaps,
            "rule": "candidate aggregate must be PROVEN-POSITIVE and at least one profitable regime/hour cell must overlap a filled runtime-lane cell",
        },
        "concentration": concentration,
        "decision": {
            "p1_pass": rotation_eligible,
            "rotation_eligible": rotation_eligible,
            "verdict": "PROBE_READY_PENDING_FABLE_AUDIT" if rotation_eligible else "P1_FAIL_NO_ROTATION",
            "reasons": [
                reason
                for condition, reason in (
                    (not history_complete, "history_not_complete_to_preregistered_bound"),
                    (new_pnl <= 0.0, "deepened_paper_pnl_nonpositive"),
                    (not temporal_pass, "temporal_hour_match_fail"),
                    (bool(concentration["concentration_discounted"]), "top1_positive_pnl_concentration_gt_50pct"),
                )
                if condition
            ],
            "live_mutation": False,
            "next_action": "Fable audit; no live or shadow admission" if not rotation_eligible else "Fable promotion audit required before any mutation",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", default=DEFAULT_WALLET)
    parser.add_argument("--admission-packets", default=DEFAULT_PACKETS)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--temporal", default=DEFAULT_TEMPORAL)
    parser.add_argument("--live-ledger", default=DEFAULT_LIVE_LEDGER)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    wallet = _norm_wallet(args.wallet)
    resolutions_path = ROOT / args.resolutions if args.resolutions else _default_resolutions_path(ROOT)
    history_path = ROOT / args.history
    report = build_report(
        wallet=wallet,
        packets=load_json(ROOT / args.admission_packets, default={}),
        history=load_json(history_path, default={}),
        temporal=load_json(ROOT / args.temporal, default={}),
        live_ledger=load_json(ROOT / args.live_ledger, default={}),
        winners=_load_resolutions(resolutions_path),
    )
    history_bytes = history_path.read_bytes()
    report["inputs"] = {
        "admission_packets": args.admission_packets,
        "history": args.history,
        "history_sha256": hashlib.sha256(history_bytes).hexdigest(),
        "history_byte_count": len(history_bytes),
        "history_event_count": len(load_json(history_path, default={}).get("events") or []),
        "temporal": args.temporal,
        "live_ledger": args.live_ledger,
        "resolutions": str(resolutions_path.relative_to(ROOT)) if resolutions_path.is_relative_to(ROOT) else str(resolutions_path),
    }
    atomic_write_json(ROOT / args.output, report)
    print(report["decision"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
