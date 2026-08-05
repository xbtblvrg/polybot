#!/usr/bin/env python3
"""Rank leaderboard scan wallets for measure-only watch-tier expansion."""

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

DEFAULT_LEADERBOARD = ROOT / "data/research/wallet_copy_leaderboard_crypto_state.json"
DEFAULT_PROBE = ROOT / "data/research/corrected_copyability_probe_watch_tier_20260709T0630Z.json"
DEFAULT_FLEET = ROOT / "data/research/btc5m_live_paper_fleet_latest.json"
DEFAULT_MORNING = ROOT / "data/research/btc5m_morning_ranked_table_latest.json"
DEFAULT_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_ACTIVE_SET = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_WATCH_CONFIG = ROOT / "configs/wallet_copy/watch_tier_wallets.json"
DEFAULT_OUTPUT = ROOT / "data/research/watch_tier_expansion_ranking_latest.json"
DEFAULT_CAP = 15


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


def _active_wallets(active_set_path: Path) -> set[str]:
    payload = load_json(active_set_path, default={})
    active_set = payload.get("active_set") if isinstance(payload, dict) else {}
    out: set[str] = set()
    for member in _rows(active_set, "members"):
        wallet = _norm_wallet(member.get("source_wallet"))
        if wallet:
            out.add(wallet)
    return out


def _build_index(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    leaderboard = load_json(args.leaderboard, default={})
    for row in _rows(leaderboard, "candidate_wallets"):
        _add(
            index,
            row.get("address"),
            "leaderboard_scan_pool",
            {
                "ranks": row.get("ranks") if isinstance(row.get("ranks"), dict) else {},
                "pnl_by_period": row.get("pnl_by_period") if isinstance(row.get("pnl_by_period"), dict) else {},
                "vol_by_period": row.get("vol_by_period") if isinstance(row.get("vol_by_period"), dict) else {},
                "categories": row.get("categories") or [],
                "tags": row.get("tags") or [],
                "user_name": row.get("user_name") or row.get("name") or "",
            },
        )

    probe = load_json(args.probe, default={})
    for key in ("ranked_candidates", "fresh_local_feed_outside_queue"):
        for row in _rows(probe, key):
            _add(
                index,
                row.get("wallet"),
                "corrected_probe",
                {
                    "fresh_flow": bool(row.get("fresh_flow")),
                    "local_feed_rows": row.get("local_feed_rows"),
                    "btc5m_buys": row.get("btc5m_buys"),
                    "median_entry_offset_s": row.get("median_entry_offset_s"),
                    "median_buy_entry_offset_s": row.get("median_buy_entry_offset_s"),
                    "inband_025_050_buy_share_pct": row.get("inband_025_050_buy_share_pct"),
                    "p1_promotion_eligible": bool(row.get("p1_promotion_eligible")),
                    "p1_reject_reasons": row.get("p1_reject_reasons") or [],
                },
            )

    fleet = load_json(args.fleet, default={})
    for row in _rows(fleet, "fleet"):
        _add(
            index,
            row.get("wallet"),
            "btc5m_fleet",
            {
                "fleet_rank": row.get("fleet_rank"),
                "admission_status": row.get("admission_status"),
                "paper_pnl_usd": row.get("paper_pnl_usd"),
                "copyable_buy_events": row.get("copyable_buy_events"),
                "resolved_orders": row.get("resolved_orders"),
                "holdout_window_evidence": row.get("holdout_window_evidence"),
            },
        )

    morning = load_json(args.morning, default={})
    for row in _rows(morning, "ranked_rows"):
        if str(row.get("family") or "") != "copy":
            continue
        _add(
            index,
            row.get("candidate_id"),
            "btc5m_morning_table",
            {
                "rank": row.get("rank"),
                "status": row.get("status"),
                "oos_pnl_usd": row.get("oos_pnl_usd"),
                "oos_trades": row.get("oos_trades"),
                "holdout_passed": row.get("holdout_passed"),
            },
        )

    queue = load_json(args.queue, default={})
    for row in _rows(queue, "ranked_members"):
        _add(
            index,
            row.get("wallet"),
            "member_queue",
            {
                "queue_rank": row.get("queue_rank"),
                "ready_for_live": bool(row.get("ready_for_live")),
                "resolved_pnl": row.get("resolved_pnl"),
                "candidate_id": row.get("name"),
            },
        )
    return index


def _score(row: dict[str, Any]) -> tuple[Any, ...]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    probe = evidence.get("corrected_probe") if isinstance(evidence.get("corrected_probe"), dict) else {}
    fleet = evidence.get("btc5m_fleet") if isinstance(evidence.get("btc5m_fleet"), dict) else {}
    morning = evidence.get("btc5m_morning_table") if isinstance(evidence.get("btc5m_morning_table"), dict) else {}
    queue = evidence.get("member_queue") if isinstance(evidence.get("member_queue"), dict) else {}
    leader = evidence.get("leaderboard_scan_pool") if isinstance(evidence.get("leaderboard_scan_pool"), dict) else {}
    ranks = leader.get("ranks") if isinstance(leader.get("ranks"), dict) else {}
    best_rank = min([int(v) for v in ranks.values() if isinstance(v, (int, float))] or [999999])
    evidence_tier = 9
    if probe.get("fresh_flow"):
        evidence_tier = 0
    elif queue.get("ready_for_live"):
        evidence_tier = 1
    elif str(fleet.get("admission_status") or "") == "READY_QUEUE":
        evidence_tier = 2
    elif morning.get("holdout_passed"):
        evidence_tier = 3
    elif "leaderboard_scan_pool" in row.get("sources", []):
        evidence_tier = 4
    return (
        evidence_tier,
        num(probe.get("median_entry_offset_s"), 999999.0),
        -num(probe.get("inband_025_050_buy_share_pct"), -1.0),
        -int(num(probe.get("btc5m_buys"), 0)),
        -num(fleet.get("paper_pnl_usd"), num(morning.get("oos_pnl_usd"), num(queue.get("resolved_pnl"), 0.0))),
        best_rank,
    )


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    index = _build_index(args)
    active = _active_wallets(args.active_set)
    rows: list[dict[str, Any]] = []
    for wallet, row in index.items():
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        probe = evidence.get("corrected_probe") if isinstance(evidence.get("corrected_probe"), dict) else {}
        row = dict(row)
        row["already_active"] = wallet in active
        row["selection_reason"] = (
            "fresh_corrected_probe_flow"
            if probe.get("fresh_flow")
            else "positive_copy_evidence_needs_measurement"
            if any(source in row.get("sources", []) for source in ("member_queue", "btc5m_fleet", "btc5m_morning_table"))
            else "leaderboard_scan_pool"
        )
        rows.append(row)
    ranked = sorted(rows, key=_score)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in ranked:
        if row.get("already_active"):
            continue
        wallet = row["wallet"]
        if wallet in seen:
            continue
        selected.append(row)
        seen.add(wallet)
        if len(selected) >= int(args.cap):
            break
    return {
        "schema_version": 1,
        "kind": "watch_tier_expansion_ranking",
        "flow_stage": "LEARN/ROTATE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "criteria": {
            "source_pool": str(args.leaderboard),
            "ranking": "corrected-probe latency evidence first, then positive BTC5m copy evidence, then leaderboard rank",
            "cap": int(args.cap),
            "active_wallets_excluded": True,
        },
        "summary": {
            "candidate_wallets": len(index),
            "ranked_wallets": len(ranked),
            "selected_wallets": len(selected),
            "corrected_probe_rows": sum(1 for row in ranked if "corrected_probe" in row.get("sources", [])),
            "selected_fresh_probe": sum(
                1
                for row in selected
                if isinstance(row.get("evidence"), dict)
                and isinstance(row["evidence"].get("corrected_probe"), dict)
                and row["evidence"]["corrected_probe"].get("fresh_flow")
            ),
        },
        "selected": selected,
        "ranked": ranked[:200],
    }


def write_watch_config(args: argparse.Namespace, report: dict[str, Any]) -> dict[str, Any]:
    selected = report.get("selected") if isinstance(report.get("selected"), list) else []
    wallets = []
    for row in selected:
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        probe = evidence.get("corrected_probe") if isinstance(evidence.get("corrected_probe"), dict) else {}
        fleet = evidence.get("btc5m_fleet") if isinstance(evidence.get("btc5m_fleet"), dict) else {}
        wallets.append(
            {
                "source_wallet": row.get("wallet"),
                "selection_reason": row.get("selection_reason"),
                "median_entry_offset_s": probe.get("median_entry_offset_s"),
                "inband_025_050_buy_share_pct": probe.get("inband_025_050_buy_share_pct"),
                "btc5m_buys": probe.get("btc5m_buys"),
                "paper_pnl_usd": fleet.get("paper_pnl_usd"),
                "sources": row.get("sources", []),
            }
        )
    config = {
        "schema_version": 2,
        "kind": "wallet_copy_watch_tier_wallets",
        "flow_stage": "LEARN/ROTATE",
        "generated_at": report.get("generated_at"),
        "pinned_by": "codex_p4b_watch_tier_expansion_20260709T0800Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "contract": "Measure-only watch-tier Data API polling into separate files; no CopyIntent path, no live membership change, no order submission.",
        "selection_criteria": report.get("criteria"),
        "ranking_artifact": str(args.output),
        "wallets": wallets,
    }
    atomic_write_json(args.watch_config, config)
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leaderboard", type=Path, default=DEFAULT_LEADERBOARD)
    parser.add_argument("--probe", type=Path, default=DEFAULT_PROBE)
    parser.add_argument("--fleet", type=Path, default=DEFAULT_FLEET)
    parser.add_argument("--morning", type=Path, default=DEFAULT_MORNING)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--active-set", type=Path, default=DEFAULT_ACTIVE_SET)
    parser.add_argument("--watch-config", type=Path, default=DEFAULT_WATCH_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cap", type=int, default=DEFAULT_CAP)
    parser.add_argument("--write-config", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    config = write_watch_config(args, report) if args.write_config else None
    print(json.dumps({"summary": report["summary"], "config_wallets": len(config["wallets"]) if config else None}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
