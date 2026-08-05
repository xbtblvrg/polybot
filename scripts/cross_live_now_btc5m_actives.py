#!/usr/bin/env python3
"""Cross right-now BTC-5m active wallets against promotion evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LIVE_NOW = ROOT / "data/research/live_now_btc5m_actives.json"
DEFAULT_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_UNIVERSE = ROOT / "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_UNIVERSE_RESEARCH = ROOT / "data/research/wallet_copy_full_universe_copyability_research_latest.json"
DEFAULT_REPLAY = ROOT / "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_SHORTLIST = ROOT / "data/research/active_set_expansion_full_pool_shortlist.json"
DEFAULT_FLEET = ROOT / "data/research/btc5m_live_paper_fleet_latest.json"
DEFAULT_MORNING = ROOT / "data/research/btc5m_morning_ranked_table_latest.json"
DEFAULT_LIVE_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_WATCH_CONFIG = ROOT / "configs/wallet_copy/watch_tier_wallets.json"
DEFAULT_OUTPUT = ROOT / "data/research/live_now_btc5m_actives_cross_latest.json"

ADMISSION_STATUSES = {"READY_QUEUE", "READY_QUEUE_WASH_REVIEW"}
NON_LIVE_BREADTH_STATUSES = {
    "DENIED_READMISSION_TODAY",
    "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE",
    "DEFERRED_POST_ADDRESS_FORM_MAP_REPROBE",
    "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
}


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _rows(payload: Any, key: str) -> list[dict[str, Any]]:
    value = payload.get(key) if isinstance(payload, dict) else []
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _add(index: dict[str, dict[str, Any]], wallet: Any, source: str, evidence: dict[str, Any]) -> None:
    normalized = _norm_wallet(wallet)
    if not normalized:
        return
    row = index.setdefault(normalized, {"wallet": normalized, "sources": [], "evidence": {}})
    if source not in row["sources"]:
        row["sources"].append(source)
    row["evidence"][source] = evidence


def _active_wallets(payload: Any) -> set[str]:
    active_set = payload.get("active_set") if isinstance(payload, dict) else {}
    return {
        wallet
        for wallet in (
            _norm_wallet(row.get("source_wallet") or row.get("wallet")) for row in _rows(active_set, "members")
        )
        if wallet
    }


def _index_queue(index: dict[str, dict[str, Any]], queue: dict[str, Any]) -> None:
    for row in _rows(queue, "ranked_members"):
        disposition = row.get("breadth_disposition") if isinstance(row.get("breadth_disposition"), dict) else {}
        replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
        clearance = row.get("clearance") if isinstance(row.get("clearance"), dict) else {}
        fresh = row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}
        _add(
            index,
            row.get("wallet"),
            "member_queue",
            {
                "candidate_id": row.get("name"),
                "queue_rank": row.get("queue_rank"),
                "ready_for_live": bool(row.get("ready_for_live")),
                "ready_for_live_before_breadth_disposition": bool(
                    row.get("ready_for_live_before_breadth_disposition")
                ),
                "breadth_disposition_status": str(disposition.get("status") or "").upper(),
                "clearance_ready": bool(row.get("clearance_ready")),
                "paper_pnl_usd": row.get("resolved_pnl"),
                "copyable_buy_events": replay.get("copyable_buy_events") or clearance.get("copyable_buy_events"),
                "candidate_clob_backed_orders": replay.get("candidate_clob_backed_orders")
                or clearance.get("candidate_clob_backed_orders"),
                "policy_compatible_fresh_buy_rows_le_30s": fresh.get("policy_compatible_fresh_buy_rows_le_30s"),
                "remote_dataapi_btc5m_buys_24h": fresh.get("remote_dataapi_btc5m_buys_24h"),
                "latest_trade_age_h": fresh.get("latest_trade_age_h"),
            },
        )


def _index_universe(index: dict[str, dict[str, Any]], payload: dict[str, Any], source: str) -> None:
    for key in ("top_wallets", "ranked_queue", "leaderboard"):
        for row in _rows(payload, key):
            replay = row.get("copy_replay") if isinstance(row.get("copy_replay"), dict) else {}
            history = row.get("source_history") if isinstance(row.get("source_history"), dict) else {}
            _add(
                index,
                row.get("wallet"),
                source,
                {
                    "admission_status": str(row.get("admission_status") or "").upper(),
                    "universe_rank": row.get("universe_rank"),
                    "queue_rank": row.get("queue_rank"),
                    "copyability_score": row.get("copyability_score"),
                    "paper_pnl_usd": replay.get("paper_pnl_usd"),
                    "copyable_buy_events": replay.get("copyable_buy_events"),
                    "candidate_clob_backed_orders": replay.get("candidate_clob_backed_orders"),
                    "resolved_orders": replay.get("resolved_orders"),
                    "unique_windows": replay.get("unique_windows"),
                    "source_history_pnl_usd": history.get("pnl_usd"),
                    "source_history_roi_pct": history.get("roi_pct"),
                    "evidence_sources": row.get("evidence_sources") or [],
                },
            )


def _index_replay(index: dict[str, dict[str, Any]], replay_payload: dict[str, Any]) -> None:
    for row in _rows(replay_payload, "candidates"):
        replay = row.get("paper_replay") if isinstance(row.get("paper_replay"), dict) else row
        _add(
            index,
            row.get("wallet"),
            "full_pool_replay",
            {
                "candidate_id": row.get("candidate_id") or replay.get("candidate_id"),
                "status": str(replay.get("eligibility_status") or replay.get("status") or "").upper(),
                "paper_pnl_usd": replay.get("paper_pnl_usd"),
                "copyable_buy_events": replay.get("copyable_buy_events"),
                "candidate_clob_backed_orders": replay.get("candidate_clob_backed_orders"),
                "resolved_orders": replay.get("resolved_orders"),
                "failure_reasons": replay.get("failure_reasons") or [],
            },
        )


def _index_shortlist(index: dict[str, dict[str, Any]], shortlist: dict[str, Any]) -> None:
    for row in _rows(shortlist, "top_candidates"):
        profile = row.get("eligible_profile") if isinstance(row.get("eligible_profile"), dict) else {}
        band = profile.get("best_eligible_move_slice") if isinstance(profile.get("best_eligible_move_slice"), dict) else {}
        _add(
            index,
            row.get("wallet"),
            "profile_shortlist",
            {
                "resolved_pnl": row.get("resolved_pnl"),
                "profile_status": profile.get("status"),
                "copyable_rate_pct": band.get("copyable_rate_pct"),
                "fill_sample": band.get("fill_sample"),
                "entry_price_band": band.get("entry_price_band"),
            },
        )


def _index_fleet(index: dict[str, dict[str, Any]], fleet: dict[str, Any]) -> None:
    for row in _rows(fleet, "fleet"):
        _add(
            index,
            row.get("wallet"),
            "btc5m_fleet",
            {
                "fleet_rank": row.get("fleet_rank"),
                "admission_status": str(row.get("admission_status") or "").upper(),
                "paper_pnl_usd": row.get("paper_pnl_usd"),
                "copyable_buy_events": row.get("copyable_buy_events"),
                "resolved_orders": row.get("resolved_orders"),
            },
        )


def _index_morning(index: dict[str, dict[str, Any]], morning: dict[str, Any]) -> None:
    for row in _rows(morning, "ranked_rows"):
        wallet = row.get("wallet") or row.get("candidate_id")
        _add(
            index,
            wallet,
            "btc5m_morning_table",
            {
                "rank": row.get("rank"),
                "status": row.get("status"),
                "oos_pnl_usd": row.get("oos_pnl_usd"),
                "oos_trades": row.get("oos_trades"),
                "holdout_passed": bool(row.get("holdout_passed")),
            },
        )


def build_evidence_index(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    _index_queue(index, load_json(args.queue, default={}) or {})
    _index_universe(index, load_json(args.universe, default={}) or {}, "full_universe_copyability")
    _index_universe(index, load_json(args.universe_research, default={}) or {}, "full_universe_copyability_research")
    _index_replay(index, load_json(args.replay, default={}) or {})
    _index_shortlist(index, load_json(args.shortlist, default={}) or {})
    _index_fleet(index, load_json(args.fleet, default={}) or {})
    _index_morning(index, load_json(args.morning, default={}) or {})
    return index


def _admission_grade(evidence: dict[str, Any]) -> tuple[bool, str]:
    queue = evidence.get("member_queue") if isinstance(evidence.get("member_queue"), dict) else {}
    if queue.get("ready_for_live"):
        status = str(queue.get("breadth_disposition_status") or "").upper()
        if status in NON_LIVE_BREADTH_STATUSES:
            return False, f"queue_ready_removed_by_breadth_disposition:{status}"
        return True, "member_queue_ready_for_live"
    for source in ("full_universe_copyability", "full_universe_copyability_research", "btc5m_fleet"):
        row = evidence.get(source) if isinstance(evidence.get(source), dict) else {}
        if str(row.get("admission_status") or "").upper() in ADMISSION_STATUSES:
            return True, f"{source}:{row.get('admission_status')}"
    replay = evidence.get("full_pool_replay") if isinstance(evidence.get("full_pool_replay"), dict) else {}
    if (
        str(replay.get("status") or "").upper() == "PASS"
        and num(replay.get("paper_pnl_usd"), 0.0) > 0.0
        and int(replay.get("copyable_buy_events") or 0) >= 20
        and int(replay.get("candidate_clob_backed_orders") or 0) > 0
    ):
        return True, "full_pool_replay_pass"
    return False, "no_admission_grade_evidence"


def _positive_evidence(evidence: dict[str, Any]) -> bool:
    for row in evidence.values():
        if not isinstance(row, dict):
            continue
        if num(row.get("paper_pnl_usd"), 0.0) > 0.0 or num(row.get("resolved_pnl"), 0.0) > 0.0:
            return True
        if num(row.get("copyability_score"), 0.0) > 0.0:
            return True
    return False


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    live_now = load_json(args.live_now, default={}) or {}
    index = build_evidence_index(args)
    active = _active_wallets(load_json(args.live_guard, default={}) or {})
    live_rows = _rows(live_now, "wallets")
    generated_at = utc_now_iso()
    crossed: list[dict[str, Any]] = []
    admission_packets: list[dict[str, Any]] = []
    watch_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(live_rows, start=1):
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        evidence_row = index.get(wallet, {"wallet": wallet, "sources": [], "evidence": {}})
        evidence = evidence_row.get("evidence") if isinstance(evidence_row.get("evidence"), dict) else {}
        admission_grade, admission_reason = _admission_grade(evidence)
        already_active = wallet in active
        positive = _positive_evidence(evidence)
        disposition = "ADMISSION_PACKET_READY" if admission_grade and not already_active else "WATCH_TIER_MEASURE"
        crossed_row = {
            "wallet": wallet,
            "live_now_rank": rank,
            "trades_in_sample": row.get("trades_in_sample"),
            "already_active": already_active,
            "scored_universe_match": bool(evidence_row.get("sources")),
            "positive_evidence": positive,
            "admission_grade_evidence": admission_grade,
            "admission_reason": admission_reason,
            "disposition": disposition,
            "sources": evidence_row.get("sources") or [],
            "evidence": evidence,
        }
        crossed.append(crossed_row)
        if disposition == "ADMISSION_PACKET_READY":
            admission_packets.append(
                {
                    "flow_stage": "PROMOTE/LIVE",
                    "wallet": wallet,
                    "candidate_id": (
                        evidence.get("member_queue", {}).get("candidate_id")
                        if isinstance(evidence.get("member_queue"), dict)
                        else None
                    ),
                    "reason": admission_reason,
                    "min_size_policy": {
                        "wallet_fraction": 0.10,
                        "max_order_usd": 0.5,
                        "max_price": 0.45,
                        "live_order_submitter": "scripts/run_wallet_copy_live_guard.py",
                    },
                    "requires_fable_or_existing_breadth_authority": True,
                    "live_orders_allowed": False,
                    "paper_only": True,
                }
            )
        else:
            watch_rows.append(crossed_row)
    return {
        "schema_version": 1,
        "kind": "live_now_btc5m_actives_cross",
        "flow_stage": "DISCOVER/PROMOTE/SOS",
        "generated_at": generated_at,
        "direction_id": "2026-07-13T10:22Z-fable-live-now-actives-cross",
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "live_now": str(args.live_now),
            "queue": str(args.queue),
            "universe": str(args.universe),
            "universe_research": str(args.universe_research),
            "replay": str(args.replay),
        },
        "summary": {
            "live_now_wallets": len(crossed),
            "scored_universe_matches": sum(1 for row in crossed if row["scored_universe_match"]),
            "positive_evidence_matches": sum(1 for row in crossed if row["positive_evidence"]),
            "admission_packets": len(admission_packets),
            "watch_tier_measurement_rows": len(watch_rows),
            "already_active": sum(1 for row in crossed if row["already_active"]),
        },
        "admission_packets": admission_packets,
        "watch_tier_candidates": watch_rows,
        "crossed": crossed,
    }


def write_watch_config(args: argparse.Namespace, report: dict[str, Any]) -> dict[str, Any]:
    existing = load_json(args.watch_config, default={}) or {}
    existing_wallets = _rows(existing, "wallets")
    generated_at = report.get("generated_at") or utc_now_iso()
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in _rows(report, "watch_tier_candidates"):
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet or row.get("already_active"):
            continue
        seen.add(wallet)
        merged.append(
            {
                "source_wallet": wallet,
                "selection_reason": "live_now_btc5m_active_unscored_or_not_admission_grade",
                "trades_in_sample": row.get("trades_in_sample"),
                "live_now_rank": row.get("live_now_rank"),
                "admission_reason": row.get("admission_reason"),
                "scored_universe_match": row.get("scored_universe_match"),
                "positive_evidence": row.get("positive_evidence"),
                "sources": ["live_now_btc5m_actives", *list(row.get("sources") or [])],
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    for row in existing_wallets:
        wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        if not wallet or wallet in seen:
            continue
        merged.append(row)
        seen.add(wallet)
    config = {
        "schema_version": 2,
        "kind": "wallet_copy_watch_tier_wallets",
        "flow_stage": "DISCOVER/LEARN",
        "generated_at": generated_at,
        "pinned_by": "codex_live_now_btc5m_actives_cross_20260713T1022Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "contract": "Measure-only watch-tier Data API polling into separate files; no CopyIntent path, no live membership change, no order submission.",
        "selection_criteria": {
            "direction_id": "2026-07-13T10:22Z-fable-live-now-actives-cross",
            "source_pool": str(args.live_now),
            "ranking": "right-now BTC-5m active wallets first, preserving existing watch-tier rows after them",
            "previous_selection_criteria": existing.get("selection_criteria") if isinstance(existing, dict) else {},
        },
        "ranking_artifact": str(args.output),
        "wallets": merged,
    }
    atomic_write_json(args.watch_config, config)
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-now", type=Path, default=DEFAULT_LIVE_NOW)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--universe-research", type=Path, default=DEFAULT_UNIVERSE_RESEARCH)
    parser.add_argument("--replay", type=Path, default=DEFAULT_REPLAY)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--fleet", type=Path, default=DEFAULT_FLEET)
    parser.add_argument("--morning", type=Path, default=DEFAULT_MORNING)
    parser.add_argument("--live-guard", type=Path, default=DEFAULT_LIVE_GUARD)
    parser.add_argument("--watch-config", type=Path, default=DEFAULT_WATCH_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--write-watch-config", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    config = write_watch_config(args, report) if args.write_watch_config else None
    print(
        json.dumps(
            {
                "summary": report["summary"],
                "watch_config_wallets": len(config["wallets"]) if config else None,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
