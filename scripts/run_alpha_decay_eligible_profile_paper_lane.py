#!/usr/bin/env python3
"""Run the alpha-decay eligible-profile paper lane.

Flow stage: OBSERVE/PROMOTE_PREP. This paper-only runner pins the wallets
from an alpha-decay eligible-profile packet, filters BUY rows to the
profile's eligible entry price band, and reuses the top10 paper scorer.
It never creates live orders and never mutates guard state.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_top10_broad_paper_lane import (  # noqa: E402
    DEFAULT_CLOB_BASE,
    _disable_source_base_overrides,
    _normalized_realtime_event,
    _restore_source_base_overrides,
    _wallet_sides,
    build_measurement_state,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


DEFAULT_PACKET = "data/research/alpha_decay_eligible_profiles_20260719T162300Z.json"
DEFAULT_RTDS = "data/research/polygon_orderfilled_ws_shadow_resident.jsonl"
DEFAULT_OUTPUT = "data/research/alpha_decay_eligible_profiles_paper_lane_state.json"
DEFAULT_EVENTS = "data/research/alpha_decay_eligible_profiles_paper_events.jsonl"
DEFAULT_LOCK = "data/research/alpha_decay_eligible_profiles_paper_lane.lock"
EXPERIMENT_ID = "alpha-decay-eligible-profiles-paper-20260719"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", default=DEFAULT_PACKET)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--scan-limit", type=int, default=250_000)
    parser.add_argument("--max-events", type=int, default=500)
    parser.add_argument("--max-book-fetches", type=int, default=100)
    parser.add_argument("--wallet-fraction", type=float, default=0.1)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--min-order-usd", type=float, default=1.0)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--min-fill-ratio", type=float, default=0.999)
    parser.add_argument("--policy-id", default="alpha_decay_eligible_profile_paper_v1")
    parser.add_argument("--horizon-s", type=float, default=2.0)
    parser.add_argument("--max-observation-lag-s", type=float, default=5.0)
    parser.add_argument("--max-receipt-to-fetch-age-s", type=float, default=60.0)
    parser.add_argument("--iterations", type=int, default=0, help="0 runs continuously.")
    parser.add_argument("--sleep-s", type=float, default=0.5)
    parser.add_argument("--lock-file", default=DEFAULT_LOCK)
    parser.add_argument("--ignore-prior-state", action="store_true")
    parser.add_argument("--disable-source-base-overrides", action="store_true")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _price_in_band(price: float, band: str) -> bool:
    label = str(band or "").strip()
    if label == "<=0.25":
        return price <= 0.25
    if label == ">0.75":
        return price > 0.75
    if "-" in label:
        left, right = label.split("-", 1)
        try:
            low = float(left)
            high = float(right)
        except ValueError:
            return False
        return low < price <= high
    return False


def _packet_profiles(packet: dict[str, Any]) -> list[dict[str, Any]]:
    profiles = packet.get("profiles") if isinstance(packet.get("profiles"), list) else []
    return [profile for profile in profiles if isinstance(profile, dict) and _norm_wallet(profile.get("wallet"))]


def build_lane_state_from_packet(packet: dict[str, Any]) -> dict[str, Any]:
    ranked_wallets = []
    for idx, profile in enumerate(_packet_profiles(packet), start=1):
        wallet = _norm_wallet(profile.get("wallet"))
        best = profile.get("best_move_slice") if isinstance(profile.get("best_move_slice"), dict) else {}
        ranked_wallets.append(
            {
                "wallet": wallet,
                "user_name": f"alpha_decay_{wallet[-10:]}",
                "rank": idx,
                "category": "CRYPTO",
                "market_categories": ["btc_5m"],
                "primary_market_category": "btc_5m",
                "alpha_profile": profile,
                "alpha_entry_price_band": best.get("entry_price_band") or "",
                "alpha_fill_sample": profile.get("fill_sample"),
                "alpha_copyable_rate_pct": profile.get("copyable_rate_pct"),
                "alpha_mean_edge": profile.get("mean_edge"),
                "alpha_median_edge": profile.get("median_edge"),
            }
        )
    return {
        "schema_version": 1,
        "kind": "alpha_decay_eligible_profile_lane_state",
        "flow_stage": "OBSERVE/PROMOTE_PREP",
        "paper_only": True,
        "live_orders_allowed": False,
        "source_packet": packet.get("source_report") or "",
        "ranked_wallets": ranked_wallets,
        "summary": {
            "selected_wallets": len(ranked_wallets),
            "eligible_profile_count": packet.get("eligible_profile_count"),
            "alpha_status": packet.get("alpha_status"),
        },
        "updated_at": utc_now_iso(),
    }


def _bands_by_wallet(lane_state: dict[str, Any]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for row in lane_state.get("ranked_wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        band = str(row.get("alpha_entry_price_band") or "").strip()
        if wallet and band:
            out.setdefault(wallet, set()).add(band)
    return out


def _alpha_rows_by_wallet(lane_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in lane_state.get("ranked_wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet:
            rows[wallet] = row
    return rows


def filter_rows_to_profile_bands(rows: list[dict[str, Any]], lane_state: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    bands = _bands_by_wallet(lane_state)
    selected_wallets = set(bands)
    diagnostics = {
        "input_rows": 0,
        "non_realtime_row": 0,
        "non_selected_wallet": 0,
        "non_buy": 0,
        "outside_profile_band": 0,
        "selected_profile_band_rows": 0,
    }
    filtered: list[dict[str, Any]] = []
    for row in rows:
        diagnostics["input_rows"] += 1
        normalized = _normalized_realtime_event(row)
        if normalized is None:
            diagnostics["non_realtime_row"] += 1
            continue
        price = num(normalized.get("price"))
        wallet_sides = _wallet_sides(row, selected_wallets)
        if not wallet_sides:
            diagnostics["non_selected_wallet"] += 1
            continue
        matched = False
        saw_buy = False
        for wallet, side in wallet_sides:
            if side != "BUY":
                continue
            saw_buy = True
            if any(_price_in_band(price, band) for band in bands.get(wallet, set())):
                matched = True
                break
        if not saw_buy:
            diagnostics["non_buy"] += 1
            continue
        if not matched:
            diagnostics["outside_profile_band"] += 1
            continue
        diagnostics["selected_profile_band_rows"] += 1
        filtered.append(row)
    return filtered, diagnostics


def preserve_alpha_fields(measurement: dict[str, Any], lane_state: dict[str, Any]) -> None:
    alpha_rows = _alpha_rows_by_wallet(lane_state)
    for collection_key in ("ranked_wallets",):
        rows = measurement.get(collection_key) if isinstance(measurement.get(collection_key), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            alpha = alpha_rows.get(_norm_wallet(row.get("wallet")), {})
            for key in (
                "alpha_profile",
                "alpha_entry_price_band",
                "alpha_fill_sample",
                "alpha_copyable_rate_pct",
                "alpha_mean_edge",
                "alpha_median_edge",
            ):
                if key in alpha:
                    row[key] = alpha[key]
    wallets = measurement.get("wallets") if isinstance(measurement.get("wallets"), dict) else {}
    for wallet, row in wallets.items():
        if not isinstance(row, dict):
            continue
        alpha = alpha_rows.get(_norm_wallet(wallet), {})
        for key in (
            "alpha_entry_price_band",
            "alpha_fill_sample",
            "alpha_copyable_rate_pct",
            "alpha_mean_edge",
            "alpha_median_edge",
        ):
            if key in alpha:
                row[key] = alpha[key]


def _read_new_rows(path: str, *, offset: int, inode: int) -> tuple[list[dict[str, Any]], int, int]:
    target = Path(path)
    if not target.exists():
        return [], 0, 0
    stat = target.stat()
    current_inode = int(stat.st_ino)
    adjusted = False
    if inode and (current_inode != inode or stat.st_size < offset):
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    if not inode:
        return [], int(stat.st_size), current_inode
    if stat.st_size - offset > 50_000_000:
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    rows: list[dict[str, Any]] = []
    with target.open("rb") as handle:
        handle.seek(offset)
        if adjusted and offset > 0:
            handle.readline()
        for raw in handle:
            try:
                row = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
        offset = handle.tell()
    return rows, offset, current_inode


def _source_event_id(row: dict[str, Any], wallet: str) -> str:
    normalized = _normalized_realtime_event(row) or {}
    return "|".join(
        [
            wallet,
            str(normalized.get("tx") or ""),
            str(normalized.get("source_event_id") or row.get("log_index") or ""),
            str(normalized.get("asset") or ""),
        ]
    )


def _received_at_s(row: dict[str, Any]) -> float:
    normalized = _normalized_realtime_event(row) or {}
    return num(normalized.get("received_at_s"))


def pending_disposition(
    row: dict[str, Any], *, now_s: float, horizon_s: float, max_observation_lag_s: float
) -> str:
    received_at_s = _received_at_s(row)
    if received_at_s <= 0:
        return "MISSING_RECEIPT_TS"
    age_s = now_s - received_at_s
    if age_s < horizon_s:
        return "WAIT"
    if age_s > horizon_s + max_observation_lag_s:
        return "MISSED_OBSERVATION_LAG"
    return "OBSERVE"


def _run_once(args: argparse.Namespace, packet: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    lane_state = build_lane_state_from_packet(packet)
    rows, offset, inode = _read_new_rows(
        args.rtds_jsonl,
        offset=int(state.get("source_offset") or 0),
        inode=int(state.get("source_inode") or 0),
    )
    filtered, filter_diagnostics = filter_rows_to_profile_bands(rows, lane_state)
    seen_source_ids = {str(item) for item in state.get("seen_source_ids") or [] if str(item)}
    seen_intent_ids = {str(item) for item in state.get("seen_intent_ids") or [] if str(item)}
    pending = [item for item in state.get("pending") or [] if isinstance(item, dict)]
    selected_wallets = set(_bands_by_wallet(lane_state))
    for row in filtered:
        wallet = next((item[0] for item in _wallet_sides(row, selected_wallets) if item[1] == "BUY"), "")
        source_id = _source_event_id(row, wallet)
        if not wallet or source_id in seen_source_ids:
            continue
        intent_id = f"alpha_profile|{source_id}"
        seen_source_ids.add(source_id)
        seen_intent_ids.add(intent_id)
        pending.append({"source_event_id": source_id, "intent_id": intent_id, "row": row})

    now_s = time.time()
    due: list[dict[str, Any]] = []
    keep: list[dict[str, Any]] = []
    expired = 0
    missing_receipt = 0
    for item in pending:
        row = item.get("row") if isinstance(item.get("row"), dict) else {}
        disposition = pending_disposition(
            row,
            now_s=now_s,
            horizon_s=float(args.horizon_s),
            max_observation_lag_s=float(args.max_observation_lag_s),
        )
        if disposition == "WAIT":
            keep.append(item)
        elif disposition == "OBSERVE":
            due.append(row)
        elif disposition == "MISSING_RECEIPT_TS":
            missing_receipt += 1
        else:
            expired += 1

    measurement, new_events = build_measurement_state(
        lane_state=lane_state,
        polygon_rows=due,
        clob=CLOBMarketClient(host=args.clob_base_url, timeout_s=float(args.clob_timeout_s)),
        prior_state=state,
        wallet_fraction=float(args.wallet_fraction),
        max_order_usd=float(args.max_order_usd),
        min_order_usd=float(args.min_order_usd),
        slippage_bps=float(args.slippage_bps),
        min_fill_ratio=float(args.min_fill_ratio),
        max_events=int(args.max_events),
        max_book_fetches=int(args.max_book_fetches),
        policy_id=str(args.policy_id or ""),
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
        max_receipt_to_fetch_age_s=min(
            float(args.max_receipt_to_fetch_age_s),
            float(args.horizon_s) + float(args.max_observation_lag_s),
        ),
        now_s=now_s,
    )
    preserve_alpha_fields(measurement, lane_state)
    measurement.update(
        {
            "kind": "alpha_decay_eligible_profiles_paper_lane_state",
            "flow_stage": "OBSERVE/PROMOTE_PREP",
            "experiment_id": EXPERIMENT_ID,
            "source_packet": str(args.packet),
            "paper_only": True,
            "live_orders_allowed": False,
            "orders_submitted": 0,
            "observation_horizon_s": float(args.horizon_s),
            "max_observation_lag_s": float(args.max_observation_lag_s),
            "max_receipt_to_fetch_age_s": float(args.max_receipt_to_fetch_age_s),
            "collector_start_at": state.get("collector_start_at") or utc_now_iso(),
            "source_offset": offset,
            "source_inode": inode,
            "pending": keep,
            "pending_count": len(keep),
            "seen_source_ids": sorted(seen_source_ids),
            "seen_intent_ids": sorted(seen_intent_ids),
            "source_signals": len(seen_source_ids),
            "copyintents_created": len(seen_intent_ids),
            "expired_observation_count": int(state.get("expired_observation_count") or 0) + expired,
            "missing_receipt_ts_count": int(state.get("missing_receipt_ts_count") or 0) + missing_receipt,
            "copy_intent_parity": {
                "status": "PASS" if len(seen_source_ids) == len(seen_intent_ids) else "FAIL",
                "source_signals": len(seen_source_ids),
                "copy_intents": len(seen_intent_ids),
                "violations": abs(len(seen_source_ids) - len(seen_intent_ids)),
            },
        }
    )
    measurement["next_action"] = (
        "keep the alpha-decay eligible-profile paper lane accruing until the preregistered "
        "fresh profile-band sample gate is met; no live action without Fable review"
    )
    measurement.setdefault("source", {}).update(
        {
            "path": str(args.rtds_jsonl),
            "profile_band_filter": filter_diagnostics,
            "source_base_overrides_disabled": bool(args.disable_source_base_overrides),
        }
    )
    atomic_write_json(args.output, measurement)
    if new_events:
        append_jsonl_many(args.event_log, new_events)
    return measurement


def main() -> int:
    args = parse_args()
    lock_path = Path(args.lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_path.open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(json.dumps({"status": "ALREADY_RUNNING", "lock_file": str(lock_path)}, sort_keys=True))
        return 0
    prior_source_base = _disable_source_base_overrides(bool(args.disable_source_base_overrides))
    try:
        packet = load_json(args.packet, default={})
        packet = packet if isinstance(packet, dict) else {}
        if len(_packet_profiles(packet)) != int(packet.get("eligible_profile_count") or 0):
            raise ValueError("eligible profile packet count mismatch")
        state = {} if bool(args.ignore_prior_state) else load_json(args.output, default={})
        state = state if isinstance(state, dict) else {}
        completed = 0
        while int(args.iterations) <= 0 or completed < int(args.iterations):
            state = _run_once(args, packet, state)
            completed += 1
            if int(args.iterations) > 0 and completed >= int(args.iterations):
                break
            time.sleep(max(0.1, float(args.sleep_s)))
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0
    finally:
        _restore_source_base_overrides(prior_source_base)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
