#!/usr/bin/env python3
"""Assemble the BTC-5m 48H verdict packet.

Flow stage: LIVE/PROMOTE/LEARN. The packet compares the live freshness-gated
wallet-copy lane against the named paper alternatives without promoting them.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/btc5m_48h_verdict_packet_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current-scorecard", default="data/research/wallet_copy_daily_scorecard_2026-07-09_current.json")
    parser.add_argument("--closed-scorecard", default="data/research/wallet_copy_daily_scorecard_2026-07-08.json")
    parser.add_argument("--watch-tier-probe", default="data/research/corrected_copyability_probe_watch_tier_20260709T0630Z.json")
    parser.add_argument("--watch-tier-config", default="configs/wallet_copy/watch_tier_wallets.json")
    parser.add_argument("--watch-tier-shadow-ev", default="data/research/watch_tier_shadow_ev_latest.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _summary(path: str | Path) -> dict[str, Any]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return {"path": str(path), "status": "MISSING"}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    gate = payload.get("promotion_gate") if isinstance(payload.get("promotion_gate"), dict) else {}
    book = payload.get("book_aware_summary") if isinstance(payload.get("book_aware_summary"), dict) else {}
    return {
        "path": str(path),
        "kind": payload.get("kind"),
        "updated_at": payload.get("updated_at") or payload.get("generated_at"),
        "paper_only": bool(payload.get("paper_only")),
        "live_orders_allowed": bool(payload.get("live_orders_allowed")),
        "summary": {
            key: summary.get(key)
            for key in (
                "signals",
                "paper_quotes",
                "filled_orders",
                "open_orders",
                "cancelled_orders",
                "maker_fill_rate_pct",
                "resolved_paper_fills",
                "resolved_paper_pnl_usd",
                "resolved_paper_roi_pct",
                "promotion_bar_applies",
                "signal_gated_max_age_s",
                "paper_orders",
                "paper_filled_orders",
                "book_verified_fills",
                "park_watch",
                "ask_at_entry_distribution",
            )
            if key in summary
        },
        "book_aware_summary": {
            key: book.get(key)
            for key in (
                "paper_quotes",
                "filled_orders",
                "maker_fill_rate_pct",
                "copyintent_parity_violations",
                "prospective_no_fallback_book_orders",
                "prospective_no_fallback_filled_orders",
                "resolved_paper_fills",
                "resolved_paper_pnl_usd",
                "resolved_paper_roi_pct",
                "direct_fallback_share_pct",
            )
            if key in book
        },
        "promotion_gate": {
            key: gate.get(key)
            for key in (
                "status",
                "active_gate_decision",
                "promotion_50_resolved_positive",
                "promotion_150_prospective_no_fallback_positive",
                "prospective_no_fallback_resolved_fills_required",
                "maker_fill_rate_required_pct",
                "requires_positive_pnl",
                "requires_positive_prospective_no_fallback_pnl",
                "park_watch",
            )
            if key in gate
        },
    }


def _scorecard_snapshot(scorecard: dict[str, Any]) -> dict[str, Any]:
    total = ((scorecard.get("today") or {}).get("total") or {}) if isinstance(scorecard.get("today"), dict) else {}
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else {}
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    target = scorecard.get("target_ladder") if isinstance(scorecard.get("target_ladder"), dict) else {}
    actual = target.get("actual") if isinstance(target.get("actual"), dict) else {}
    return {
        "day_utc": scorecard.get("day_utc"),
        "generated_at": scorecard.get("generated_at"),
        "pnl_usd": total.get("pnl_usd"),
        "fills": total.get("fills"),
        "resolved_fills": total.get("resolved_fills"),
        "roi_pct": total.get("roi_pct"),
        "windows_filled": canonical.get("windows_filled"),
        "windows_submitted": canonical.get("windows_submitted"),
        "denominator_windows": canonical.get("denominator_windows"),
        "since_topup_canonical_pnl_usd": since.get("canonical_pnl_usd"),
        "since_topup_actual_delta_usd": since.get("actual_delta_vs_baseline_usd"),
        "since_topup_reconciled_delta_usd": since.get("actual_basis_reconciled_delta_vs_baseline_usd"),
        "since_topup_verdict": since.get("primary_verdict"),
        "target_gap_usd": actual.get("north_star_daily_gap_usd"),
    }


def _wallets_from_config(path: str | Path) -> list[str]:
    payload = load_json(path, default={})
    rows = payload.get("wallets") if isinstance(payload, dict) and isinstance(payload.get("wallets"), list) else []
    wallets = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if wallet:
            wallets.append(wallet)
    return wallets


def _probe_rows(path: str | Path, wallets: list[str]) -> list[dict[str, Any]]:
    payload = load_json(path, default={})
    ranked = payload.get("ranked_candidates") if isinstance(payload, dict) and isinstance(payload.get("ranked_candidates"), list) else []
    wanted = set(wallets)
    rows = []
    for idx, row in enumerate(ranked, start=1):
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or "").lower()
        if wallet not in wanted:
            continue
        rows.append(
            {
                "rank": idx,
                "wallet": wallet,
                "already_active": bool(row.get("already_active")),
                "fresh_flow": bool(row.get("fresh_flow")),
                "local_feed_rows": row.get("local_feed_rows"),
                "btc5m_buys": row.get("btc5m_buys"),
                "median_entry_offset_s": row.get("median_entry_offset_s"),
                "inband_025_050_buy_share_pct": row.get("inband_025_050_buy_share_pct"),
                "p1_promotion_eligible": bool(row.get("p1_promotion_eligible")),
                "p1_reject_reasons": row.get("p1_reject_reasons") or [],
            }
        )
    return rows


def _live_primary(current: dict[str, Any]) -> dict[str, Any]:
    snap = _scorecard_snapshot(current)
    pnl = float(snap.get("pnl_usd") or 0.0)
    return {
        "name": "live_60s_freshness_gated_wallet_copy",
        "flow_stage": "LIVE/LEARN",
        "status": "PRIMARY" if pnl > 0 else "PRIMARY_BUT_NOT_PRODUCING",
        "reason": "only lane with fresh positive live PnL evidence after late-window gate tightened to 60s",
        "current_day": snap,
    }


def build_packet(
    *,
    current_scorecard: dict[str, Any],
    closed_scorecard: dict[str, Any],
    watch_tier_probe: str,
    watch_tier_config: str,
    watch_tier_shadow_ev: dict[str, Any],
) -> dict[str, Any]:
    watch_wallets = _wallets_from_config(watch_tier_config)
    maker_rows = [
        {
            "gate": "5s",
            **_summary("data/research/maker_first_btc5m_signal_gated_paper_state.json"),
        },
        {
            "gate": "10s",
            **_summary("data/research/maker_first_btc5m_signal_gated_paper_10s_paper_state.json"),
        },
        {
            "gate": "30s",
            **_summary("data/research/maker_first_btc5m_signal_gated_paper_30s_paper_state.json"),
        },
    ]
    return {
        "schema_version": 1,
        "kind": "btc5m_48h_verdict_packet",
        "flow_stage": "LIVE/PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "source_direction": "Fable DIRECTION 2026-07-09T06:55Z RESUMPTION QUEUE item 2",
        "primary": _live_primary(current_scorecard),
        "scorecards": {
            "current_day": _scorecard_snapshot(current_scorecard),
            "closed_2026_07_08": _scorecard_snapshot(closed_scorecard),
        },
        "green_day_attribution": {
            "status": "ATTRIBUTED_TO_60S_GATE"
            if float(_scorecard_snapshot(current_scorecard).get("pnl_usd") or 0.0) > 0
            else "NOT_GREEN",
            "evidence": "Fable 06:34Z: 4 pre-gate late-window fills were total losses; post-60s-gate fills are net positive.",
            "late_window_cohort": current_scorecard.get("late_window_cohort", {}),
        },
        "signal_gated_maker": {
            "status": "PENDING_NOT_PRIMARY",
            "rows": maker_rows,
            "decision": "no promotion: canonical 5s gate has no resolved positive sample; 10s/30s are measurement-only",
        },
        "e7": {
            "status": "NOT_PRIMARY",
            "lane": _summary("data/research/e7_spot_open_paper_lane_state.json"),
            "packet": _summary("data/research/e7_paper_packet_report.json"),
            "decision": "no promotion: paper lane remains below live-ready promotion bar",
        },
        "selective_tail_watch_tier": {
            "status": "SHADOW_ONLY",
            "probe_path": watch_tier_probe,
            "watch_tier_probe_rows": _probe_rows(watch_tier_probe, watch_wallets),
            "shadow_ev": {
                "summary": watch_tier_shadow_ev.get("summary", {}),
                "wallets": watch_tier_shadow_ev.get("wallets", []),
                "criteria": watch_tier_shadow_ev.get("criteria", {}),
            },
            "decision": "no copy rights; re-admission requires >=30 resolved gated-eligible shadow signals with ROI>0 and Fable ruling",
        },
        "verdict": {
            "primary": "live_60s_freshness_gated_wallet_copy",
            "promote_new_lane_now": False,
            "next_action": "continue live 60s-gated lane, accrue watch-tier shadow EV, and let Fable rule on any gate crossing",
        },
    }


def main() -> int:
    args = parse_args()
    packet = build_packet(
        current_scorecard=load_json(args.current_scorecard, default={}) if Path(args.current_scorecard).exists() else {},
        closed_scorecard=load_json(args.closed_scorecard, default={}) if Path(args.closed_scorecard).exists() else {},
        watch_tier_probe=args.watch_tier_probe,
        watch_tier_config=args.watch_tier_config,
        watch_tier_shadow_ev=load_json(args.watch_tier_shadow_ev, default={})
        if Path(args.watch_tier_shadow_ev).exists()
        else {},
    )
    atomic_write_json(args.output, packet)
    print(json.dumps(packet, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
