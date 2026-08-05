#!/usr/bin/env python3
"""Rank the five operator-directed profit directions from measured evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402
from src.wallet_copy.venue_executability import venue_gate_summary  # noqa: E402


DEFAULT_OUTPUT = ROOT / "data/research/direction_leaderboard_latest.json"


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 6)


def _hours_to_next_utc_day(now: dt.datetime) -> float:
    next_day = dt.datetime.combine(
        now.date() + dt.timedelta(days=1),
        dt.time(),
        tzinfo=dt.timezone.utc,
    )
    return max(0.0, (next_day - now).total_seconds() / 3600.0)


def _t2_projection(policy: dict[str, Any], *, target_windows_per_day: int) -> dict[str, Any]:
    per_fill: list[float] = []
    for cell in policy.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        score = venue_gate_summary(cell)
        if not score.get("f1_pass"):
            continue
        resolved = int(score.get("resolved") or 0)
        if resolved > 0:
            per_fill.append(float(score.get("post_fee_pnl_usd") or 0.0) / resolved)
    median_per_fill = statistics.median(per_fill) if per_fill else None
    return {
        "f1_pass_cells": len(per_fill),
        "median_post_fee_pnl_per_resolved_fill_usd": _round(median_per_fill),
        "target_resolved_windows_per_day": target_windows_per_day,
        "measured_expected_usd_per_day": _round(
            median_per_fill * target_windows_per_day if median_per_fill is not None else None
        ),
        "projection_basis": (
            "median F1-pass exact-cell post-fee PnL/resolved fill × T3 target pace; "
            "portfolio projection, not a live-PnL claim"
        ),
    }


def _t1_projection(admission: dict[str, Any], *, target_windows_per_day: int) -> dict[str, Any]:
    pnl = 0.0
    resolved = 0
    admitted_slices = 0
    for member in admission.get("members") or []:
        if not isinstance(member, dict):
            continue
        scoped = member.get("band_scoped_admission")
        if not isinstance(scoped, dict):
            continue
        for band in scoped.get("admitted_bands") or []:
            score = venue_gate_summary(band) if isinstance(band, dict) else None
            if not isinstance(score, dict) or not score.get("f1_pass"):
                continue
            pnl += float(score.get("post_fee_pnl_usd") or 0.0)
            resolved += int(score.get("resolved") or 0)
            admitted_slices += 1
    per_fill = pnl / resolved if resolved else None
    return {
        "admitted_members": len(admission.get("members") or []),
        "admitted_f1_pass_slices": admitted_slices,
        "resolved_evidence": resolved,
        "post_fee_pnl_usd": _round(pnl),
        "post_fee_pnl_per_resolved_fill_usd": _round(per_fill),
        "target_resolved_windows_per_day": target_windows_per_day,
        "measured_expected_usd_per_day": _round(
            per_fill * target_windows_per_day if per_fill is not None else None
        ),
        "projection_basis": (
            "admitted-band post-fee PnL/resolved fill × T3 target pace; "
            "historical fixed-policy projection, not a live-PnL claim"
        ),
    }


def build_leaderboard(root: Path, *, now: dt.datetime | None = None) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    data = root / "data/research"
    policy = _load(data / "wide_policy_fingerprint_evidence_latest.json")
    band = _load(data / "band_scoped_admission_latest.json")
    t2_admission = _load(data / "t2_82c8_cell_admission_latest.json")
    scalp = _load(data / "btc5m_structural_scalp_paper_lane_state.json")
    multivenue = _load(data / "btc5m_multivenue_ttl_passive_residual_selector.json")
    frontier = _load(data / "wide_direct_admissible_frontier_latest.json")
    volume_standby = _load(data / "13e0_exact_policy_promotion_packet_latest.json")
    digest = _load(data / "state_digest.json")
    target_windows_per_day = 100

    t2_metric = _t2_projection(policy, target_windows_per_day=target_windows_per_day)
    t1_metric = _t1_projection(band, target_windows_per_day=target_windows_per_day)
    scalp_summary = scalp.get("summary") if isinstance(scalp.get("summary"), dict) else {}
    volume_ev = volume_standby.get("ev") if isinstance(volume_standby.get("ev"), dict) else {}
    volume_clock = volume_standby.get("clock") if isinstance(volume_standby.get("clock"), dict) else {}
    elapsed_days = float(volume_clock.get("elapsed_h") or 0.0) / 24.0
    volume_prefee_day = (
        float(volume_ev.get("resolved_paper_pnl_usd") or 0.0) / elapsed_days
        if elapsed_days > 0
        else None
    )
    live_day_pnl = (digest.get("pnl") or {}).get("day_pnl_usd")

    rows = [
        {
            "direction_id": "t2_exact_cell_portfolio",
            "label": "T2 exact-cell portfolio",
            "status": "LIVE_PORTFOLIO_READY_CELL_ROTATION_REQUIRED",
            "measured_expected_usd_per_day": t2_metric["measured_expected_usd_per_day"],
            "time_to_live_readiness_h": 0.0,
            "evidence": {
                **t2_metric,
                "current_admission_status": t2_admission.get("status"),
                "live_portfolio_day_pnl_usd": live_day_pnl,
                "killed_identity_policy": "no manual re-enable; next all-pass exact cell only",
                "source": "data/research/wide_policy_fingerprint_evidence_latest.json",
            },
            "kill_line": "per-cell -$4 and first 3 resolved fills cumulative post-fee <=$0",
            "verdict_clock": "T3 >=50 windows in first 12h of 2026-07-29; then rolling 48h",
        },
        {
            "direction_id": "t1_band_scoped_seats",
            "label": "T1 band-scoped seats",
            "status": "LIVE_READY_MIDNIGHT_RELOAD",
            "measured_expected_usd_per_day": t1_metric["measured_expected_usd_per_day"],
            "time_to_live_readiness_h": _round(_hours_to_next_utc_day(now)),
            "evidence": {
                **t1_metric,
                "admission_status": band.get("status"),
                "live_portfolio_day_pnl_usd": live_day_pnl,
                "source": "data/research/band_scoped_admission_latest.json",
            },
            "kill_line": "per-member -$4 post-fee",
            "verdict_clock": "T3 >=50 windows in first 12h of 2026-07-29; then rolling 48h",
        },
        {
            "direction_id": "frontier_finds",
            "label": "Best frontier finds (9796/c539/13e0 class)",
            "status": "EVIDENCE_WATCH_NO_ALL_PASS_TARGET",
            "measured_expected_usd_per_day": 0.0,
            "time_to_live_readiness_h": None,
            "evidence": {
                "eligible_count": frontier.get("eligible_count"),
                "frontier_candidate_count": frontier.get("candidate_count"),
                "watch_rows": [
                    {
                        "wallet": "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
                        "measured_post_fee_pnl_usd": 310.40,
                        "roi_pct": 14.97,
                        "action": "watch_only_zero_own_source_rows",
                        "evidence_authority": "Fable VERIFIED 2026-07-28T19:56Z",
                    },
                    {
                        "wallet": volume_standby.get("wallet"),
                        "measured_prefee_pnl_usd": volume_ev.get("resolved_paper_pnl_usd"),
                        "prefee_usd_per_day": _round(volume_prefee_day),
                        "terminal_decision": volume_standby.get("terminal_decision"),
                        "action": "evidence_only_terminal_park_preserved",
                        "evidence_authority": "data/research/13e0_exact_policy_promotion_packet_latest.json",
                    },
                ],
                "promotion_grade_expected_usd_per_day": 0.0,
                "caveat": "positive pre-fee/watch evidence is not promotion-grade expected PnL",
                "source": "data/research/wide_direct_admissible_frontier_latest.json",
            },
            "kill_line": "assigned only after exact-cell admission; then -$4",
            "verdict_clock": "48h begins only when an all-pass exact identity enters live",
        },
        {
            "direction_id": "multivenue_passive_residual",
            "label": "Multivenue passive-residual",
            "status": str(multivenue.get("status") or "NO_GATE_COMPLETE_CELL"),
            "measured_expected_usd_per_day": 0.0,
            "time_to_live_readiness_h": 24.0,
            "evidence": {
                "selected": multivenue.get("selected"),
                "cell_count": len(multivenue.get("cells") or []),
                "gate_digits": {
                    "resolved_terminals_gte": 30,
                    "post_fee_pnl_gt": 0,
                    "both_halves_positive": True,
                    "executable_fill_rate_gte_pct": 60,
                    "attempts_gte": 50,
                    "trailing_24h_intents_gte": 1,
                },
                "source": "data/research/btc5m_multivenue_ttl_passive_residual_selector.json",
            },
            "kill_line": "$1 target, $2.50 venue ceiling, per-cell -$4",
            "verdict_clock": "48h from first gate-complete live cell",
        },
        {
            "direction_id": "structural_scalp_new_identity",
            "label": "Structural scalp new-identity re-proof",
            "status": "PARKED_CURRENT_IDENTITY_NEW_IDENTITY_REQUIRED",
            "measured_expected_usd_per_day": _round(scalp_summary.get("study_ev_per_day_usd")),
            "time_to_live_readiness_h": 72.0,
            "evidence": {
                "current_forward_fills": scalp_summary.get("forward_fills"),
                "current_forward_post_fee_pnl_usd": scalp_summary.get("forward_pnl_usd"),
                "unlock": {
                    "new_identity": True,
                    "elapsed_h_gte": 72,
                    "resolved_forward_fills_gte": 300,
                    "post_fee_pnl_gt": 0,
                    "both_halves_positive": True,
                },
                "source": "data/research/btc5m_structural_scalp_paper_lane_state.json",
            },
            "kill_line": "$1 target, $2.50 venue ceiling, per-cell -$4",
            "verdict_clock": "48h starts with the new-identity re-proof clock",
        },
    ]
    rows.sort(
        key=lambda row: (
            -(float(row["measured_expected_usd_per_day"]) if row["measured_expected_usd_per_day"] is not None else -1e12),
            float(row["time_to_live_readiness_h"]) if row["time_to_live_readiness_h"] is not None else 1e12,
        )
    )
    for rank, row in enumerate(rows, 1):
        row["rank"] = rank
    return {
        "schema_version": 1,
        "kind": "direction_leaderboard",
        "flow_stage": "PROMOTE/LIVE/ROTATE",
        "generated_at": now.isoformat().replace("+00:00", "Z"),
        "ranking_rule": (
            "descending measured promotion-grade expected USD/day, then shortest "
            "time-to-live-readiness; the live ledger supersedes projections at each 48h verdict"
        ),
        "daily_scorecard_contract": "embedded in every newly generated wallet_copy_daily_scorecard",
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    payload = build_leaderboard(Path(args.root))
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
