#!/usr/bin/env python3
"""Build the unified BTC5M morning ranked table for Fable's funding decision.

Flow stage: LEARN/PROMOTE. The table normalizes paper-only evidence from the
copy fleet, two-sided prime study, E-batch signal studies, and decompiler intake
into one EV/day ranking. It never submits orders or mutates live state.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_FLEET = "data/research/btc5m_live_paper_fleet_latest.json"
DEFAULT_TWO_SIDED = "data/research/btc5m_two_sided_prime_study_latest.json"
DEFAULT_E_BATCH = "data/research/btc5m_corpus_signal_batch_20260707_full.json"
DEFAULT_DECOMPILER = "data/research/wallet_copy_strategy_decompiler_intake_latest.json"
DEFAULT_OUTPUT = "data/research/btc5m_morning_ranked_table_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet", default=DEFAULT_FLEET)
    parser.add_argument("--two-sided", default=DEFAULT_TWO_SIDED)
    parser.add_argument("--e-batch", default=DEFAULT_E_BATCH)
    parser.add_argument("--decompiler", default=DEFAULT_DECOMPILER)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-fleet", type=int, default=50)
    return parser.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _artifact_status(payload: Any) -> str:
    return "PRESENT" if isinstance(payload, dict) and payload else "MISSING"


def _row(
    *,
    mechanism_id: str,
    family: str,
    candidate_id: str,
    paper_lane_id: str,
    status: str,
    holdout_passed: bool,
    ev_per_day_usd: float,
    oos_pnl_usd: float,
    oos_trades: int,
    evidence_pointer: str,
    proposed_funding_size_usd: float,
    notes: str = "",
    matrix_coverage: str = "",
) -> dict[str, Any]:
    return {
        "rank": 0,
        "mechanism_id": mechanism_id,
        "family": family,
        "candidate_id": candidate_id,
        "paper_lane_id": paper_lane_id,
        "status": status,
        "holdout_passed": bool(holdout_passed),
        "ev_per_day_usd": round(float(ev_per_day_usd), 6),
        "oos_pnl_usd": round(float(oos_pnl_usd), 6),
        "oos_trades": int(oos_trades),
        "evidence_pointer": evidence_pointer,
        "proposed_funding_size_usd": round(float(proposed_funding_size_usd), 6) if holdout_passed else 0.0,
        "matrix_coverage": matrix_coverage,
        "notes": notes,
    }


def _fleet_rows(payload: dict[str, Any], *, top_n: int, pointer: str) -> list[dict[str, Any]]:
    rows = []
    for item in (payload.get("fleet") if isinstance(payload.get("fleet"), list) else [])[: max(1, top_n)]:
        if not isinstance(item, dict):
            continue
        wallet = str(item.get("wallet") or "")
        paper_pnl = _float(item.get("paper_pnl_usd"), 0.0)
        coverage = str(item.get("matrix_coverage") or ("WINDOW_ROWS" if _int(item.get("matrix_rows")) > 0 else "NONE"))
        ready = str(item.get("admission_status") or "") == "READY_QUEUE"
        holdout = ready and coverage != "NONE" and paper_pnl > 0.0
        if holdout:
            status = "HOLDOUT_PASS_READY_QUEUE"
        elif coverage == "NONE":
            status = "DATA_INCOMPLETE"
        elif paper_pnl <= 0.0:
            status = "NON_POSITIVE_COPY_PNL"
        else:
            status = "NOT_READY_QUEUE"
        rows.append(
            _row(
                mechanism_id="copy-1to1-taker",
                family="copy",
                candidate_id=wallet,
                paper_lane_id=str(item.get("paper_lane_id") or f"paper_copy_1to1_top200_{wallet[-8:]}"),
                status=status,
                holdout_passed=holdout,
                ev_per_day_usd=paper_pnl,
                oos_pnl_usd=paper_pnl,
                oos_trades=_int(item.get("resolved_orders")),
                evidence_pointer=f"{pointer}#fleet[{_int(item.get('fleet_rank'))}]",
                proposed_funding_size_usd=4.0,
                notes=(
                    "matrix_coverage_missing"
                    if coverage == "NONE"
                    else str(item.get("admission_status") or "")
                ),
                matrix_coverage=coverage,
            )
        )
    return rows


def _two_sided_rows(payload: dict[str, Any], *, pointer: str) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("mechanism_rows") if isinstance(payload.get("mechanism_rows"), list) else []:
        if not isinstance(item, dict):
            continue
        rows.append(
            _row(
                mechanism_id=str(item.get("mechanism_id") or ""),
                family=str(item.get("family") or "structural"),
                candidate_id=str(item.get("candidate_id") or item.get("mechanism_id") or ""),
                paper_lane_id=str(item.get("paper_lane_id") or ""),
                status=str(item.get("status") or ""),
                holdout_passed=bool(item.get("holdout_passed")),
                ev_per_day_usd=_float(item.get("ev_per_day_usd"), 0.0),
                oos_pnl_usd=_float(item.get("oos_pnl_usd"), 0.0),
                oos_trades=_int(item.get("oos_trades")),
                evidence_pointer=str(item.get("evidence_pointer") or f"{pointer}#mechanism_rows"),
                proposed_funding_size_usd=_float(item.get("proposed_funding_size_usd"), 0.0),
                notes="two_sided_prime_study",
            )
        )
    return rows


def _span_days_from_corpus(payload: dict[str, Any]) -> float:
    summary = payload.get("resolution_summary") if isinstance(payload.get("resolution_summary"), dict) else {}
    start = _float(summary.get("min_window_start_s"), 0.0)
    end = _float(summary.get("max_window_start_s"), 0.0)
    if start <= 0 or end <= start:
        return 1.0
    return max((end - start + 300.0) / 86400.0 * 0.30, 1.0)


def _e_batch_rows(payload: dict[str, Any], *, pointer: str) -> list[dict[str, Any]]:
    studies = payload.get("studies") if isinstance(payload.get("studies"), dict) else {}
    span_days = _span_days_from_corpus(payload)
    rows = []
    for name, study in studies.items():
        if not isinstance(study, dict):
            continue
        best = study.get("best") if isinstance(study.get("best"), dict) else {}
        status = str(study.get("status") or "")
        oos_pnl = _float(best.get("oos_pnl_usd"), _float(best.get("pnl_usd"), 0.0))
        oos_trades = _int(best.get("oos_trades"), _int(best.get("trades")))
        holdout = status == "POSITIVE_OOS_REGION" and oos_trades > 0 and oos_pnl > 0.0
        rows.append(
            _row(
                mechanism_id=str(name),
                family="signal" if str(name).startswith("E") else "copy",
                candidate_id=str(best.get("key") or name),
                paper_lane_id=f"paper_{str(name).lower()}",
                status=status or "NO_STATUS",
                holdout_passed=holdout,
                ev_per_day_usd=oos_pnl / span_days,
                oos_pnl_usd=oos_pnl,
                oos_trades=oos_trades,
                evidence_pointer=f"{pointer}#studies.{name}.best",
                proposed_funding_size_usd=1.0,
                notes=str(best.get("key") or ""),
            )
        )
    return rows


def _decompiler_rows(payload: dict[str, Any], *, pointer: str) -> list[dict[str, Any]]:
    rows = []
    for idx, item in enumerate(payload.get("selected_wallets") if isinstance(payload.get("selected_wallets"), list) else [], start=1):
        if not isinstance(item, dict):
            continue
        pnl = _float(item.get("pnl_usd"), 0.0)
        span_days = max(_float(item.get("span_days"), 1.0), 1.0)
        rows.append(
            _row(
                mechanism_id="extracted-decompiler-rules",
                family="extracted",
                candidate_id=str(item.get("wallet") or ""),
                paper_lane_id=f"paper_extracted_decompiler_rules_{idx}",
                status="MODELING_ONLY_NO_HOLDOUT",
                holdout_passed=False,
                ev_per_day_usd=pnl / span_days,
                oos_pnl_usd=0.0,
                oos_trades=0,
                evidence_pointer=f"{pointer}#selected_wallets[{idx - 1}]",
                proposed_funding_size_usd=0.0,
                notes=f"source_pnl={round(pnl, 6)} roi={item.get('roi_pct')}",
            )
        )
    return rows


def _rank_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows.sort(
        key=lambda row: (
            bool(row.get("holdout_passed")),
            _float(row.get("ev_per_day_usd"), 0.0),
            _float(row.get("oos_pnl_usd"), 0.0),
            _int(row.get("oos_trades")),
        ),
        reverse=True,
    )
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    fleet = load_json(root / args.fleet, default={})
    two_sided = load_json(root / args.two_sided, default={})
    e_batch = load_json(root / args.e_batch, default={})
    decompiler = load_json(root / args.decompiler, default={})
    rows = []
    if isinstance(fleet, dict):
        rows.extend(_fleet_rows(fleet, top_n=int(args.top_fleet), pointer=args.fleet))
    if isinstance(two_sided, dict):
        rows.extend(_two_sided_rows(two_sided, pointer=args.two_sided))
    if isinstance(e_batch, dict):
        rows.extend(_e_batch_rows(e_batch, pointer=args.e_batch))
    if isinstance(decompiler, dict):
        rows.extend(_decompiler_rows(decompiler, pointer=args.decompiler))
    rows = _rank_rows(rows)
    families = Counter(str(row.get("family") or "") for row in rows)
    statuses = Counter(str(row.get("status") or "") for row in rows)
    top = rows[0] if rows else {}
    return {
        "schema_version": 1,
        "kind": "btc5m_morning_ranked_table",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "inputs": {
            "fleet": args.fleet,
            "two_sided": args.two_sided,
            "e_batch": args.e_batch,
            "decompiler": args.decompiler,
            "top_fleet": int(args.top_fleet),
        },
        "artifact_status": {
            "fleet": _artifact_status(fleet),
            "two_sided": _artifact_status(two_sided),
            "e_batch": _artifact_status(e_batch),
            "decompiler": _artifact_status(decompiler),
        },
        "summary": {
            "rows": len(rows),
            "holdout_passed_rows": sum(1 for row in rows if row.get("holdout_passed")),
            "families": dict(sorted(families.items())),
            "statuses": dict(sorted(statuses.items())),
            "matrix_coverage_none_rows": sum(1 for row in rows if row.get("matrix_coverage") == "NONE"),
            "top_rank": top,
        },
        "ranked_rows": rows,
        "morning_decision_contract": (
            "Fable reads ranked_rows, names the profitable direction, and funds only through the standard gate and single live guard."
        ),
    }


def main() -> int:
    args = parse_args()
    report = build_report(ROOT, args)
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    atomic_write_json(output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
