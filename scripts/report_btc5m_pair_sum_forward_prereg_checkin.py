#!/usr/bin/env python3
"""Refresh the BTC5M pair-sum forward pre-registration check-in.

Flow stage: LEARN/PROMOTE. The report is evidence-only: it verifies the
pre-registered forward gate and ranks the structural scalp lane against the
maker-first paper lane without promoting either lane by itself.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


EXPERIMENT_ID = "btc5m-pair-sum-forward-20260707"
DEFAULT_PREREG = "data/research/experiment_preregistration_latest.json"
DEFAULT_STRUCTURAL_STATE = "data/research/btc5m_structural_scalp_paper_lane_state.json"
DEFAULT_MAKER_STATE = "data/research/maker_first_btc5m_paper_state.json"
DEFAULT_OUTPUT = "data/research/btc5m_pair_sum_forward_prereg_checkin_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _rooted(path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _prereg_passed(prereg: dict[str, Any]) -> bool:
    active_ids = {str(item) for item in prereg.get("active_ids") or [] if str(item)}
    missing = [str(item) for item in prereg.get("missing_required_ids") or [] if str(item)]
    latest_id = str(prereg.get("latest_experiment_id") or "")
    return str(prereg.get("status") or "") == "PASS" and not missing and (
        latest_id == EXPERIMENT_ID or EXPERIMENT_ID in active_ids
    )


def _structural_row(state: dict[str, Any], *, preregistration_status: str) -> dict[str, Any]:
    summary = _as_dict(state.get("summary"))
    metrics = _as_dict(state.get("metrics"))
    forward = _as_dict(metrics.get("forward"))
    all_time = _as_dict(metrics.get("all_time"))
    forward_fills = _int(forward.get("fills"), _int(summary.get("forward_fills")))
    forward_pnl = _float(forward.get("pnl_usd"), _float(summary.get("forward_pnl_usd")))
    ev_per_day = _float(forward.get("ev_per_day_usd"), _float(summary.get("study_ev_per_day_usd")))
    criterion_passed = preregistration_status == "PASS" and forward_fills >= 30 and forward_pnl > 0 and ev_per_day > 0
    return {
        "basis": "pre_registered_forward_gate",
        "criterion_passed": criterion_passed,
        "forward_fills": forward_fills,
        "forward_pnl_usd": round(forward_pnl, 6),
        "lane": "btc5m_pair_sum_forward_structural_scalp",
        "note": (
            "pre-registered gate passed: >=30 forward fills, forward PnL > 0, EV/day > 0"
            if criterion_passed
            else "pre-registered gate has not passed"
        ),
        "paper_pnl_usd_all_time": round(_float(all_time.get("pnl_usd"), _float(summary.get("paper_pnl_usd"))), 6),
        "rank_score_usd": round(forward_pnl, 6),
        "study_ev_per_day_usd": round(ev_per_day, 6),
    }


def _maker_row(state: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(state.get("summary"))
    resolved_pnl = _float(summary.get("resolved_paper_pnl_usd"))
    return {
        "basis": "resolved_paper_pnl_usd",
        "lane": "maker_first_btc5m_v1",
        "live_orders_allowed": bool(summary.get("live_orders_allowed")),
        "note": "higher resolved PnL but unresolved-heavy and not promoted by this heartbeat",
        "paper_only": bool(summary.get("paper_only", True)),
        "rank_score_usd": round(resolved_pnl, 6),
        "resolved_paper_fills": _int(summary.get("resolved_paper_fills")),
        "resolved_paper_pnl_usd": round(resolved_pnl, 6),
        "resolved_paper_roi_pct": round(_float(summary.get("resolved_paper_roi_pct")), 6),
        "unresolved_paper_fills": _int(summary.get("unresolved_paper_fills")),
    }


def build_checkin(
    *,
    prereg: dict[str, Any],
    structural_state: dict[str, Any],
    maker_state: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    prereg_status = "PASS" if _prereg_passed(prereg) else str(prereg.get("status") or "MISSING_REQUIRED")
    structural = _structural_row(structural_state, preregistration_status=prereg_status)
    maker = _maker_row(maker_state)
    ranking = sorted(
        [maker, structural],
        key=lambda row: (_float(row.get("rank_score_usd")), 1 if row.get("lane") == "maker_first_btc5m_v1" else 0),
        reverse=True,
    )
    for index, row in enumerate(ranking, start=1):
        row["rank"] = index
    decision = (
        "MAKER_FIRST_HIGHER_RESOLVED_PNL_STRUCTURAL_SCALP_PREREG_PASS_CHECKED_IN"
        if structural["criterion_passed"] and ranking[0].get("lane") == "maker_first_btc5m_v1"
        else "STRUCTURAL_SCALP_PREREG_PASS_CHECKED_IN"
        if structural["criterion_passed"]
        else "STRUCTURAL_SCALP_PREREG_GATE_NOT_PASSED"
    )
    return {
        "schema_version": 1,
        "kind": "btc5m_pair_sum_forward_prereg_checkin",
        "flow_stage": "LEARN",
        "generated_at": generated_at,
        "experiment_id": EXPERIMENT_ID,
        "success_criterion": prereg.get("latest_success_criterion"),
        "preregistration_status": prereg_status,
        "criterion_passed": bool(structural["criterion_passed"]),
        "decision": decision,
        "promotion_status": "CHECKIN_ONLY_NO_LIVE_PROMOTION_THIS_HEARTBEAT",
        "maker_first": maker,
        "structural_scalp": structural,
        "ranking": ranking,
        "source_artifacts": [
            DEFAULT_PREREG,
            DEFAULT_STRUCTURAL_STATE,
            DEFAULT_MAKER_STATE,
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prereg", default=DEFAULT_PREREG)
    parser.add_argument("--structural-state", default=DEFAULT_STRUCTURAL_STATE)
    parser.add_argument("--maker-state", default=DEFAULT_MAKER_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prereg = load_json(_rooted(args.prereg), default={})
    structural_state = load_json(_rooted(args.structural_state), default={})
    maker_state = load_json(_rooted(args.maker_state), default={})
    payload = build_checkin(
        prereg=_as_dict(prereg),
        structural_state=_as_dict(structural_state),
        maker_state=_as_dict(maker_state),
        generated_at=_utc_now_iso(),
    )
    atomic_write_json(_rooted(args.output), payload)
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["preregistration_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
