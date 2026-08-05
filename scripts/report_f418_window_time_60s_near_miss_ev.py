#!/usr/bin/env python3
"""Paper-only EV attribution around f418's immutable 60-second window gate."""
from __future__ import annotations
import argparse, json, re, sys, time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
DEFAULT_COHORT_STATE = Path("data/research/order_flow_deadman_state.json")
FEE_RATE = 0.069997697
WINDOW_RE = re.compile(r"btc-(?:updown|up-or-down)-5m-(\d+)")
PRICE_CELL_CUTS = (.25, .32, .40, .50, .70, 2.0)
PRICE_CELL_NAMES = (
    "lt_025",
    "025_032",
    "032_040",
    "040_050",
    "050_070",
    "gte_070",
)

def _rows(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists(): return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try: row = json.loads(line)
            except json.JSONDecodeError: continue
            if isinstance(row, dict): yield row

def _winner_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    result = {}
    for row in rows:
        winner = str(row.get("winning_outcome") or row.get("direction") or "").lower()
        if winner not in {"up", "down"}: continue
        for field in ("condition_id", "market", "market_slug"):
            key = str(row.get(field) or "").lower()
            if key: result[key] = winner
    return result

def _bin(offset: float) -> str:
    return "lt_45" if offset < 45 else ("45_59" if offset < 60 else "gte_60_suppressed")

def _timestamp(value: Any) -> float:
    try:
        if isinstance(value, (int, float)): return float(value)
        return datetime.fromisoformat(str(value).replace("Z","+00:00")).timestamp()
    except (TypeError, ValueError): return -1

def _cell(value: float, cuts: tuple[float, ...], names: tuple[str, ...]) -> str:
    for cut, name in zip(cuts, names):
        if value < cut: return name
    return names[-1]

def _cohort_wallets(path: Path) -> list[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    member_signal_age = payload.get("member_signal_age")
    if not isinstance(member_signal_age, dict):
        return []
    return sorted(
        str(wallet).strip().lower()
        for wallet, row in member_signal_age.items()
        if str(wallet).strip()
        and isinstance(row, dict)
        and (
            int(row.get("signal_age_count") or 0) > 0
            or int(row.get("suppressed_intents") or 0) > 0
        )
    )


def _bin_groups(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for name in ("lt_45", "45_59", "gte_60_suppressed"):
        cohort = [row for row in rows if row["offset_bin"] == name]
        windows = {row["market_slug"] for row in cohort}
        total = sum(row["post_fee_pnl_usd"] for row in cohort)
        groups[name] = {
            "resolved_windows": len(windows),
            "fill_feasible_windows": len(
                {row["market_slug"] for row in cohort if row["fill_feasible"]}
            ),
            "post_fee_pnl_usd": round(total, 6),
            "post_fee_ev_per_window_usd": (
                round(total / len(windows), 6) if windows else None
            ),
        }
    return groups


def build_report(
    event_rows: Iterable[dict[str, Any]],
    resolution_rows: Iterable[dict[str, Any]],
    *,
    generated_at: str,
    wallets: Iterable[str] | None = None,
) -> dict[str, Any]:
    winners = _winner_index(resolution_rows)
    requested_wallets = {
        str(wallet).strip().lower() for wallet in (wallets or [F418]) if str(wallet).strip()
    }
    unique = {}
    for row in event_rows:
        source_wallet = str(row.get("source_wallet") or "").lower()
        if source_wallet not in requested_wallets: continue
        slug, event = str(row.get("market_slug") or ""), str(row.get("event") or "")
        rejected = event == "wallet_copy_live_profit_latency_suppression_reject" and row.get("reject_reason") == "window_time_gte_60s"
        lifecycle = event == "wallet_copy_live_order"
        if not WINDOW_RE.search(slug) or not (rejected or lifecycle): continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        start = float(WINDOW_RE.search(slug).group(1))
        offset = float(row.get("window_time_s") or payload.get("window_time_s") or (_timestamp(row.get("ts") or row.get("submitted_at"))-start))
        if offset < 0: continue
        outcome = str(row.get("outcome") or payload.get("outcome") or "")
        cohort = "suppressed" if rejected else "on_policy"
        unique.setdefault(
            (source_wallet, slug, outcome.lower(), cohort),
            row | {"_offset": offset, "_cohort": cohort, "_source_wallet": source_wallet},
        )
    resolved = []
    for row in unique.values():
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        slug = str(row.get("market_slug") or payload.get("market_slug") or "")
        condition = str(row.get("condition_id") or payload.get("condition_id") or "")
        winner = winners.get(condition.lower()) or winners.get(slug.lower())
        if not winner: continue
        outcome = str(row.get("outcome") or payload.get("outcome") or "")
        price = float(row.get("limit_price") or payload.get("limit_price") or 0)
        cost = float(row.get("copy_size_usd") or row.get("requested_size_usd") or payload.get("copy_size_usd") or 1)
        shares = cost / price if 0 < price < 1 else 0
        fee = FEE_RATE * shares * price * (1-price)
        pnl = (shares if outcome.lower() == winner else 0) - cost - fee
        age = float(row.get("signal_age_s") or 0)
        resolved.append({"source_wallet":row["_source_wallet"], "market_slug":slug, "cohort":row["_cohort"], "offset_bin":_bin(row["_offset"]),
            "window_offset_s":round(row["_offset"],6), "price":round(price,6),
            "price_cell":_cell(price,PRICE_CELL_CUTS,PRICE_CELL_NAMES),
            "move_magnitude_usd":round(cost,6),
            "move_magnitude_basis":"copy_intent_requested_size_usd",
            "source_age_s":round(age,6), "source_age_cell":_cell(age,(5,10,30,1e9),("lt_5","5_10","10_30","gte_30")),
            "fill_feasible":0 < price < 1, "executable_price_basis":"retained_limit_price_counterfactual",
            "expected_fee_usd":round(fee,6), "post_fee_pnl_usd":round(pnl,6)})
    resolved.sort(key=lambda r:(r["market_slug"], r["source_wallet"]))
    split = max(1,int(len(resolved)*.8)) if resolved else 0
    groups = _bin_groups(resolved)
    per_wallet = {
        wallet: {
            "cohorts": _bin_groups(
                [row for row in resolved if row["source_wallet"] == wallet]
            ),
            "resolved_rows": sum(
                row["source_wallet"] == wallet for row in resolved
            ),
        }
        for wallet in sorted(requested_wallets)
    }
    holdout=resolved[split:]; gate=groups["45_59"]["resolved_windows"]>=40 and groups["lt_45"]["resolved_windows"]>=40
    return {"kind":"f418_window_time_60s_near_miss_ev_shadow","generated_at":generated_at,"flow_stage":"OBSERVE/LEARN/LIVE",
        "paper_only":True,"live_mutation":False,"copy_intent_parity":True,
        "policy":{"gate":"window_time_gte_60s","frozen_bins":["lt_45","45_59","on_policy_lt_60"],
            "price_cell_edges":[0.25,0.32,0.40,0.50,0.70]},
        "cohort_source":"order_flow_deadman_state.member_signal_age_or_explicit_cli",
        "cohort_wallets":sorted(requested_wallets),"per_wallet":per_wallet,
        "cohorts":groups,"chronological_holdout":{"rows":len(holdout),"post_fee_pnl_usd":round(sum(r["post_fee_pnl_usd"] for r in holdout),6)},
        "cells":resolved,"gate":{"pass":gate,"minimum_resolved_windows_per_on_policy_bin":40},"status":"PASS" if gate else "ACCRUING"}

def _once(args: argparse.Namespace) -> None:
    now=datetime.now(tz=UTC).isoformat().replace("+00:00","Z")
    wallets = [str(wallet).lower() for wallet in args.wallet]
    if not wallets:
        wallets = _cohort_wallets(Path(args.wallets_state))
    report=build_report(
        _rows(Path(args.events)),
        _rows(Path(args.resolutions)),
        generated_at=now,
        wallets=wallets or [F418],
    )
    atomic_write_json(Path(args.output),report); print(json.dumps({"generated_at":now,"status":report["status"],"cohorts":report["cohorts"]}),flush=True)

def main() -> int:
    p=argparse.ArgumentParser(); p.add_argument("--events",default="data/research/wallet_copy_live_execution_events.jsonl")
    p.add_argument("--resolutions",default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    p.add_argument("--output",default="data/research/f418_window_time_60s_near_miss_ev_shadow_latest.json")
    p.add_argument("--wallet", action="append", default=[])
    p.add_argument("--wallets-state", default=str(DEFAULT_COHORT_STATE))
    p.add_argument("--interval-s",type=float,default=0); args=p.parse_args()
    while True:
        _once(args)
        if args.interval_s<=0:return 0
        time.sleep(args.interval_s)
if __name__=="__main__": raise SystemExit(main())
