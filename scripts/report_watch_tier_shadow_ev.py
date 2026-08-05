#!/usr/bin/env python3
"""Score watch-tier gated-subpopulation shadow EV.

Flow stage: ROTATE/LEARN. This is measure-only evidence for the Fable
2026-07-09 re-admission bar: watch-tier wallets earn no copy rights unless
their <60s, in-band BTC-5m BUY subpopulation reaches the pre-registered
sample and ROI threshold.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_corrected_copyability_probe import _entry_offset_s, _is_btc5m  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_HISTORY = "data/research/wallet_copy_watch_tier_history_state.json"
DEFAULT_CONFIG = "configs/wallet_copy/watch_tier_wallets.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/watch_tier_shadow_ev_latest.json"
DEFAULT_READY_SHADOW_STATE = "data/research/wallet_copy_ready_shadow_lanes_state.json"
DEFAULT_READMISSION_RULINGS = "data/research/watch_tier_readmission_rulings.json"
DEFAULT_MIN_ROI_PCT = 13.7
MEASUREMENT_ONLY_ADMISSION_RULINGS = {"ADMITTED_MEASUREMENT_ONLY", "READMITTED_MEASUREMENT_ONLY"}
REVOKED_OR_SUSPENDED_RULINGS = {"ADMISSION_REVOKED_PRE_LANE", "SUSPENDED_BELOW_BAR"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY)
    parser.add_argument("--watch-tier-config", default=DEFAULT_CONFIG)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--ready-shadow-state", default=DEFAULT_READY_SHADOW_STATE)
    parser.add_argument("--readmission-rulings", default=DEFAULT_READMISSION_RULINGS)
    parser.add_argument("--max-entry-offset-s", type=float, default=60.0)
    parser.add_argument("--min-price", type=float, default=0.25)
    parser.add_argument("--max-price", type=float, default=0.50)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--min-resolved-signals", type=int, default=30)
    parser.add_argument("--min-roi-pct", type=float, default=DEFAULT_MIN_ROI_PCT)
    return parser.parse_args()


def _wallet(value: Any) -> str:
    wallet = str(value or "").strip().lower()
    return wallet if wallet.startswith("0x") and len(wallet) == 42 else ""


def _wallets_from_config(path: str | Path) -> list[str]:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return []
    wallets: list[str] = []
    for row in payload.get("wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        if wallet:
            wallets.append(wallet)
    return list(dict.fromkeys(wallets))


def _ready_shadow_lane_wallets(payload: dict[str, Any] | None) -> set[str]:
    if not isinstance(payload, dict):
        return set()
    lanes = payload.get("lanes") if isinstance(payload.get("lanes"), list) else []
    wallets: set[str] = set()
    for lane in lanes:
        if not isinstance(lane, dict):
            continue
        wallet = _wallet(lane.get("wallet") or lane.get("source_wallet"))
        if wallet:
            wallets.add(wallet)
    return wallets


def _readmission_rulings_by_wallet(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    rows = payload.get("rulings") if isinstance(payload.get("rulings"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        if not wallet:
            continue
        out[wallet] = row
    return out


def _denial_ruling_applies(wallet_row: dict[str, Any], ruling: dict[str, Any] | None) -> bool:
    if not isinstance(ruling, dict):
        return False
    decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
    if decision not in {"DENIED", "READMISSION_DENIED"}:
        return False
    min_roi_pct = ruling.get("min_roi_pct")
    if min_roi_pct is None:
        min_roi_pct = ruling.get("roi_floor_pct")
    if min_roi_pct is None:
        min_roi_pct = DEFAULT_MIN_ROI_PCT
    try:
        return float(wallet_row.get("roi_pct") or 0.0) < float(min_roi_pct)
    except (TypeError, ValueError):
        return False


def _measurement_only_admission_applies(ruling: dict[str, Any] | None) -> bool:
    if not isinstance(ruling, dict):
        return False
    decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
    return decision in MEASUREMENT_ONLY_ADMISSION_RULINGS


def _revoked_or_suspended_ruling_applies(wallet_row: dict[str, Any], ruling: dict[str, Any] | None) -> bool:
    if not isinstance(ruling, dict):
        return False
    decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
    if decision not in REVOKED_OR_SUSPENDED_RULINGS:
        return False
    min_roi_pct = ruling.get("min_roi_pct")
    if min_roi_pct is None:
        min_roi_pct = ruling.get("roi_floor_pct")
    if min_roi_pct is None:
        min_roi_pct = DEFAULT_MIN_ROI_PCT
    try:
        roi_below_floor = float(wallet_row.get("roi_pct") or 0.0) < float(min_roi_pct)
    except (TypeError, ValueError):
        roi_below_floor = True
    return roi_below_floor or not bool(wallet_row.get("readmission_consideration_eligible"))


def _event_price(row: dict[str, Any]) -> float:
    price = num(row.get("price"), 0.0)
    if price > 0:
        return price
    size = num(row.get("size"), 0.0)
    usd = num(row.get("usdc_size"), 0.0)
    return usd / size if size > 0 and usd > 0 else 0.0


def _empty_wallet(wallet: str, *, min_resolved_signals: int, min_roi_pct: float) -> dict[str, Any]:
    return {
        "source_wallet": wallet,
        "eligible_signals": 0,
        "resolved_signals": 0,
        "unresolved_signals": 0,
        "wins": 0,
        "losses": 0,
        "cost_usd": 0.0,
        "payout_usd": 0.0,
        "pnl_usd": 0.0,
        "roi_pct": 0.0,
        "sample_floor": int(min_resolved_signals),
        "roi_floor_pct": float(min_roi_pct),
        "readmission_consideration_eligible": False,
        "status": "NO_GATED_ELIGIBLE_SIGNALS",
    }


def _finalize_wallet(row: dict[str, Any], *, min_resolved_signals: int, min_roi_pct: float) -> dict[str, Any]:
    resolved = int(row.get("resolved_signals") or 0)
    cost = float(row.get("cost_usd") or 0.0)
    pnl = float(row.get("pnl_usd") or 0.0)
    roi = round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0
    row["cost_usd"] = round(cost, 6)
    row["payout_usd"] = round(float(row.get("payout_usd") or 0.0), 6)
    row["pnl_usd"] = round(pnl, 6)
    row["roi_pct"] = roi
    row["sample_floor"] = int(min_resolved_signals)
    row["roi_floor_pct"] = float(min_roi_pct)
    row["readmission_consideration_eligible"] = resolved >= int(min_resolved_signals) and roi >= float(min_roi_pct)
    if row["readmission_consideration_eligible"]:
        row["status"] = "READMISSION_RULING_DUE"
    elif resolved < int(min_resolved_signals):
        row["status"] = "SAMPLE_BELOW_FLOOR"
    else:
        row["status"] = "ROI_BELOW_READMISSION_BAR"
    return row


def build_report(
    *,
    history_state: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    configured_wallets: list[str],
    max_entry_offset_s: float,
    min_price: float,
    max_price: float,
    order_usd: float,
    min_resolved_signals: int,
    min_roi_pct: float,
    ready_shadow_state: dict[str, Any] | None = None,
    readmission_rulings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = history_state.get("events") if isinstance(history_state.get("events"), list) else []
    by_wallet: dict[str, dict[str, Any]] = {
        wallet: _empty_wallet(wallet, min_resolved_signals=min_resolved_signals, min_roi_pct=min_roi_pct)
        for wallet in configured_wallets
    }
    scored_rows: list[dict[str, Any]] = []
    skipped = defaultdict(int)
    for row in rows:
        if not isinstance(row, dict):
            skipped["non_dict"] += 1
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        if configured_wallets and wallet not in set(configured_wallets):
            skipped["wallet_not_in_watch_tier"] += 1
            continue
        if str(row.get("action") or "").upper() != "BUY":
            skipped["not_buy"] += 1
            continue
        if not _is_btc5m(row):
            skipped["not_btc5m"] += 1
            continue
        offset = _entry_offset_s(row)
        if offset is None or offset >= float(max_entry_offset_s):
            skipped["entry_offset_gte_max"] += 1
            continue
        price = _event_price(row)
        if price < float(min_price) or price > float(max_price):
            skipped["outside_price_band"] += 1
            continue
        synthetic_order = {
            "order_id": str(row.get("transaction_hash") or row.get("event_id") or ""),
            "intent_id": str(row.get("source_fingerprint") or row.get("event_id") or ""),
            "source_wallet": wallet,
            "wallet_name": row.get("wallet_name"),
            "condition_id": row.get("condition_id"),
            "market_slug": row.get("market_slug"),
            "outcome": row.get("outcome"),
            "status": "FILLED",
            "final_status": "FILLED",
            "filled_size_usd": float(order_usd),
            "filled_shares": float(order_usd) / price if price > 0 else 0.0,
            "submitted_at": row.get("timestamp") or row.get("event_ts"),
        }
        score = score_order(synthetic_order, resolutions)
        bucket = by_wallet.setdefault(
            wallet,
            _empty_wallet(wallet, min_resolved_signals=min_resolved_signals, min_roi_pct=min_roi_pct),
        )
        bucket["eligible_signals"] += 1
        if score.get("resolved"):
            bucket["resolved_signals"] += 1
            bucket["wins"] += 1 if score.get("win") else 0
            bucket["losses"] += 0 if score.get("win") else 1
            bucket["cost_usd"] += float(score.get("cost_usd") or 0.0)
            bucket["payout_usd"] += float(score.get("payout_usd") or 0.0)
            bucket["pnl_usd"] += float(score.get("pnl_usd") or 0.0)
        else:
            bucket["unresolved_signals"] += 1
        scored_rows.append(
            {
                "source_wallet": wallet,
                "market_slug": row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "outcome": row.get("outcome"),
                "price": round(price, 6),
                "entry_offset_s": round(float(offset), 6),
                "resolved": bool(score.get("resolved")),
                "win": score.get("win"),
                "pnl_usd": score.get("pnl_usd"),
                "roi_pct": score.get("roi_pct"),
            }
        )
    wallet_rows = [
        _finalize_wallet(row, min_resolved_signals=min_resolved_signals, min_roi_pct=min_roi_pct)
        for row in by_wallet.values()
    ]
    laned_wallets = _ready_shadow_lane_wallets(ready_shadow_state)
    rulings_by_wallet = _readmission_rulings_by_wallet(readmission_rulings)
    due: list[dict[str, Any]] = []
    already_laned: list[str] = []
    ruled_denied: list[str] = []
    admitted_by_ruling: list[str] = []
    revoked_or_suspended: list[str] = []
    for row in wallet_rows:
        wallet = str(row.get("source_wallet") or "").lower()
        row["readmission_pending"] = False
        ruling = rulings_by_wallet.get(wallet)
        if _revoked_or_suspended_ruling_applies(row, ruling):
            decision = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
            row["readmission_ruling_state"] = decision
            row["readmission_ruling_id"] = ruling.get("ruling_id") or ruling.get("authority")
            row["status"] = decision
            revoked_or_suspended.append(wallet)
            continue
        if wallet in laned_wallets:
            row["readmission_ruling_state"] = "ALREADY_LANED"
            if row.get("readmission_consideration_eligible"):
                row["status"] = "READMISSION_ALREADY_LANED"
                already_laned.append(wallet)
            continue
        if _denial_ruling_applies(row, ruling):
            row["readmission_ruling_state"] = "DENIED"
            row["readmission_ruling_id"] = ruling.get("ruling_id") or ruling.get("authority")
            row["status"] = "READMISSION_DENIED"
            ruled_denied.append(wallet)
            continue
        if row.get("readmission_consideration_eligible") and _measurement_only_admission_applies(ruling):
            row["readmission_ruling_state"] = str(ruling.get("ruling") or ruling.get("decision") or "").upper()
            row["readmission_ruling_id"] = ruling.get("ruling_id") or ruling.get("authority")
            row["status"] = "READMISSION_ADMITTED_MEASUREMENT_ONLY"
            admitted_by_ruling.append(wallet)
            continue
        if row.get("readmission_consideration_eligible"):
            row["readmission_pending"] = True
            due.append(row)
    wallet_rows.sort(
        key=lambda row: (
            not bool(row.get("readmission_pending")),
            -float(row["roi_pct"]),
            -int(row["resolved_signals"]),
            row["source_wallet"],
        )
    )
    return {
        "schema_version": 1,
        "kind": "watch_tier_shadow_ev",
        "flow_stage": "ROTATE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "criteria": {
            "max_entry_offset_s": float(max_entry_offset_s),
            "min_price": float(min_price),
            "max_price": float(max_price),
            "order_usd": float(order_usd),
            "min_resolved_signals": int(min_resolved_signals),
            "min_roi_pct": float(min_roi_pct),
            "authority": "Fable DIRECTION 2026-07-09T06:34Z watch-tier re-admission bar",
        },
        "summary": {
            "configured_wallets": len(configured_wallets),
            "history_events": len(rows),
            "eligible_signals": sum(int(row.get("eligible_signals") or 0) for row in wallet_rows),
            "resolved_signals": sum(int(row.get("resolved_signals") or 0) for row in wallet_rows),
            "readmission_ruling_due": len(due),
            "wallets_due": [row["source_wallet"] for row in due],
            "readmission_already_laned": len(already_laned),
            "wallets_already_laned": sorted(already_laned),
            "readmission_denied_by_ruling": len(ruled_denied),
            "wallets_ruled_denied": sorted(ruled_denied),
            "admission_revoked_or_suspended_by_ruling": len(revoked_or_suspended),
            "wallets_revoked_or_suspended": sorted(revoked_or_suspended),
            "measurement_only_admitted_by_ruling": len(admitted_by_ruling),
            "wallets_admitted_by_ruling": sorted(admitted_by_ruling),
            "status": "READMISSION_RULING_DUE" if due else "NO_READMISSION_RULING_DUE",
        },
        "skipped_counts": dict(sorted(skipped.items())),
        "wallets": wallet_rows,
        "sample_rows": scored_rows[-100:],
        "next_action": (
            "page Fable for re-admission ruling on due wallets"
            if due
            else (
                "continue watch-tier shadow measurement until >=30 resolved gated-eligible "
                f"signals with ROI>={float(min_roi_pct)}"
            )
        ),
    }


def main() -> int:
    args = parse_args()
    history = load_json(args.history_state, default={})
    history = history if isinstance(history, dict) else {}
    report = build_report(
        history_state=history,
        resolutions=load_resolutions(args.resolutions),
        configured_wallets=_wallets_from_config(args.watch_tier_config),
        max_entry_offset_s=float(args.max_entry_offset_s),
        min_price=float(args.min_price),
        max_price=float(args.max_price),
        order_usd=float(args.order_usd),
        min_resolved_signals=int(args.min_resolved_signals),
        min_roi_pct=float(args.min_roi_pct),
        ready_shadow_state=load_json(args.ready_shadow_state, default={}),
        readmission_rulings=load_json(args.readmission_rulings, default={}),
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
