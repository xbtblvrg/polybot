#!/usr/bin/env python3
"""Measure deferred-at-open execution for c539 pre-open BUY inventory.

This is a paper-only shadow.  It never emits CopyIntents and never writes any
live guard, mission, roster, or execution-ledger file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fees import expected_polymarket_buy_fee_usd
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.venue_executability import (  # noqa: E402
    row_is_venue_executable,
    venue_minimum_max_price,
)


WALLET = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
DIRECTION_ID = "2026-07-29T18:24:10Z"
SUCCESSOR_DIRECTION_ID = "2026-07-30T06:20:00Z"
UNREACHABLE_PREDECESSOR_FINGERPRINT = (
    "5e763791ce53ee72a09bc157932543f19fb1212ab9d9331ddd34c62a4c0b2378"
)
PRICE_BAND_PREDECESSOR_FINGERPRINT = (
    "ab7008fc4209a26b5fe2115a34e637013202ff33ccc526c892fad806306a5949"
)
WF050_SUCCESSOR_ALLOWED_POLICY_FIELDS = [
    "policy_fingerprint",
    "wallet_fraction",
]
FULL_BAND_SUCCESSOR_ALLOWED_POLICY_FIELDS = [
    "max_price",
    "policy_fingerprint",
    "policy_id",
    "wallet_fraction",
]
DEFAULT_HOT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_STATE = "data/research/c539_deferred_open_paper_probe_state.json"
BTC5M_SLUG = re.compile(r"(?:btc-updown-5m-|btc-5m-)(\d{10})$")
VENUE_EXECUTABLE_MAX_PRICE = venue_minimum_max_price()


def _checksum(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _window_start(row: dict[str, Any]) -> int | None:
    slug = str(row.get("market_slug") or row.get("event_slug") or "")
    match = BTC5M_SLUG.search(slug)
    return int(match.group(1)) if match else None


def _event_key(row: dict[str, Any]) -> str:
    event_id = str(row.get("event_id") or "").strip()
    if event_id:
        return event_id
    identity = {
        "wallet": str(row.get("source_wallet") or "").lower(),
        "transaction_hash": str(row.get("transaction_hash") or "").lower(),
        "token_id": str(row.get("token_id") or ""),
        "event_ts": num(row.get("event_ts")),
        "price": num(row.get("price")),
        "size": num(row.get("size")),
    }
    return f"deferred_{_checksum(identity)[:24]}"


def canonical_signal_key(row: dict[str, Any]) -> str:
    """Return Fable's cross-feed economic-signal identity."""
    identity = {
        "condition_id": str(row.get("condition_id") or "").lower(),
        "token_id": str(row.get("token_id") or ""),
        "window_start_s": int(num(row.get("window_start_s"))),
        "source_price": round(num(row.get("source_price")), 9),
        "source_size": round(num(row.get("source_size")), 9),
        "source_usdc": round(num(row.get("source_usdc")), 9),
    }
    return _checksum(identity)


def canonicalize_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse RTDS/Polygon copies, retaining the earliest observation."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if isinstance(row, dict):
            groups.setdefault(canonical_signal_key(row), []).append(row)
    distinct: list[dict[str, Any]] = []
    for signal_key, group in groups.items():
        ordered = sorted(
            group,
            key=lambda row: (num(row.get("observed_ts")), str(row.get("event_id") or "")),
        )
        winner = dict(ordered[0])
        winner["canonical_signal_key"] = signal_key
        winner["duplicate_source_event_ids"] = [
            str(row.get("event_id") or "") for row in ordered[1:]
        ]
        winner["source_event_ids"] = [str(row.get("event_id") or "") for row in ordered]
        winner["cross_feed_duplicate_count"] = max(0, len(ordered) - 1)
        distinct.append(winner)
    return sorted(
        distinct,
        key=lambda row: (num(row.get("observed_ts")), str(row.get("canonical_signal_key"))),
    )


def _policy_from_guard(guard: dict[str, Any]) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    policies = runtime.get("policy_by_wallet") if isinstance(runtime.get("policy_by_wallet"), dict) else {}
    policy = policies.get(WALLET) if isinstance(policies.get(WALLET), dict) else {}
    if not policy:
        for member in runtime.get("members") or []:
            if (
                isinstance(member, dict)
                and str(member.get("source_wallet") or "").lower() == WALLET
            ):
                policy = member
                break
    frozen = {
        "policy_id": str(policy.get("policy_id") or ""),
        "wallet_fraction": num(policy.get("wallet_fraction"), 0.0),
        "max_order_usd": num(policy.get("max_order_usd"), 0.0),
        "min_order_usd": num(
            policy.get("min_order_usd"),
            policy.get("drip_min_tranche_usd"),
        ),
        "min_price": num(policy.get("min_price"), 0.0),
        "max_price": num(policy.get("max_price"), 0.0),
        "max_seconds_from_open": num(policy.get("max_seconds_from_open"), 300.0),
        "fee_model_id": "polymarket_embedded_buy_fee_v1",
        "inventory_rule": "min(sum(source_usdc)*wallet_fraction,max_order_usd)",
        "execution_rule": "marketable_limit_at_frozen_max_price_at_market_open",
    }
    required_positive = (
        frozen["policy_id"]
        and frozen["wallet_fraction"] > 0
        and frozen["max_order_usd"] > 0
        and frozen["max_price"] > frozen["min_price"]
    )
    if not required_positive:
        raise RuntimeError("c539 current runtime policy is absent or unusable")
    return {**frozen, "policy_fingerprint": _checksum(frozen)}


def _new_state(policy: dict[str, Any], now_s: float) -> dict[str, Any]:
    registered = datetime.fromtimestamp(now_s, timezone.utc)
    return {
        "schema_version": 1,
        "kind": "c539_deferred_open_paper_probe",
        "flow_stage": "OBSERVE/LEARN/PROMOTE",
        "direction_id": DIRECTION_ID,
        "generated_at": utc_now_iso(),
        "registered_at": registered.isoformat(),
        "observation_deadline_at": (registered + timedelta(hours=24)).isoformat(),
        "status": "ACCRUING_PAPER_ONLY",
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintents_emitted": 0,
        "live_files_written": [],
        "source_wallet": WALLET,
        "frozen_policy": policy,
        "registered_policy_fingerprint": policy["policy_fingerprint"],
        "source_rows": [],
        "window_evaluations": [],
        "summary": {},
        "admission": {
            "eligible": False,
            "live_authority": False,
            "required_bars": {
                "resolved_gte_50": False,
                "post_fee_positive": False,
                "both_halves_positive": False,
                "dual_gate": False,
                "f1_f4_same_identity": False,
                "venue_executable": False,
            },
        },
    }


def preregister_wf050_successor(state: dict[str, Any], *, now_s: float) -> dict[str, Any]:
    """Close the unreachable cohort and preregister its comparable successor."""
    frozen = state.get("frozen_policy") if isinstance(state.get("frozen_policy"), dict) else {}
    if frozen.get("policy_fingerprint") != UNREACHABLE_PREDECESSOR_FINGERPRINT:
        raise RuntimeError("refusing to terminate an unexpected c539 policy fingerprint")
    successor_policy = dict(frozen)
    successor_policy.pop("policy_fingerprint", None)
    successor_policy["wallet_fraction"] = 0.50
    successor_policy["policy_fingerprint"] = _checksum(successor_policy)
    changed_fields = sorted(
        key
        for key in successor_policy
        if successor_policy.get(key) != frozen.get(key)
    )
    if changed_fields != WF050_SUCCESSOR_ALLOWED_POLICY_FIELDS:
        raise RuntimeError(f"c539 successor policy drifted in fields: {changed_fields}")

    terminal_record = {
        "status": "PROBE_UNREACHABLE_BY_CONSTRUCTION",
        "terminated_at": datetime.fromtimestamp(now_s, timezone.utc).isoformat(),
        "registered_at": state.get("registered_at"),
        "original_deadline_at": state.get("observation_deadline_at"),
        "policy_fingerprint": frozen["policy_fingerprint"],
        "terminal_cohort_evaluations": 40,
        "refusal_decomposition": {
            "inventory_below_frozen_min_order": 20,
            "price_band_or_depth_gate": 2,
            "resident_not_running_inside_open_grace": 18,
            "verbatim": "20/2/18",
        },
        "evidence_interpretation": (
            "unreachable by construction; never admissible as evidence against c539"
        ),
    }
    successor = _new_state(successor_policy, now_s)
    successor["schema_version"] = 2
    successor["direction_id"] = SUCCESSOR_DIRECTION_ID
    successor["predecessor_terminal_records"] = [
        *(state.get("predecessor_terminal_records") or []),
        terminal_record,
    ]
    successor["preregistration"] = {
        "status": "PREREGISTERED_LIVE_READY_PAPER_ONLY",
        "predecessor_policy_fingerprint": frozen["policy_fingerprint"],
        "allowed_policy_fields": WF050_SUCCESSOR_ALLOWED_POLICY_FIELDS,
        "changed_policy_fields": changed_fields,
        "rationale": (
            "wf=0.50 is 5x mirror ratio; $1 cap makes evidence comparable"
        ),
        "min_order_unchanged_usd": 1.0,
    }
    return successor


def preregister_full_band_successor(state: dict[str, Any], *, now_s: float) -> dict[str, Any]:
    """Close the inherited tail-band cohort and start the comparable WIDE clock."""
    frozen = state.get("frozen_policy") if isinstance(state.get("frozen_policy"), dict) else {}
    if frozen.get("policy_fingerprint") != PRICE_BAND_PREDECESSOR_FINGERPRINT:
        raise RuntimeError("refusing to terminate an unexpected c539 price-band fingerprint")
    successor_policy = dict(frozen)
    successor_policy.pop("policy_fingerprint", None)
    successor_policy.update(
        {
            "policy_id": "c539_open_mirror_wf0p50_cap1_full_band",
            "min_price": 0.0,
            "max_price": 1.0,
        }
    )
    successor_policy["policy_fingerprint"] = _checksum(successor_policy)
    changed_fields = sorted(
        key
        for key in successor_policy
        if successor_policy.get(key) != frozen.get(key)
    )
    required_changes = ["max_price", "policy_fingerprint", "policy_id"]
    unexpected_fields = sorted(
        set(changed_fields) - set(FULL_BAND_SUCCESSOR_ALLOWED_POLICY_FIELDS)
    )
    if unexpected_fields or changed_fields != required_changes:
        raise RuntimeError(f"c539 full-band successor drifted in fields: {changed_fields}")

    terminal_record = {
        "status": "PROBE_UNREACHABLE_BY_CONSTRUCTION_PRICE_BAND",
        "terminated_at": datetime.fromtimestamp(now_s, timezone.utc).isoformat(),
        "registered_at": state.get("registered_at"),
        "original_deadline_at": state.get("observation_deadline_at"),
        "policy_fingerprint": frozen["policy_fingerprint"],
        "terminal_cohort_evaluations": 3,
        "refusal_decomposition": {
            "inventory_below_frozen_min_order": 0,
            "price_band_or_depth_gate": 1,
            "resident_not_running_inside_open_grace": 2,
            "verbatim": "0/1/2",
        },
        "source_price_distribution": [0.49, 0.49, 0.49, 0.48],
        "frozen_max_price": 0.25,
        "evidence_interpretation": (
            "unreachable by inherited price band; never admissible as evidence against c539"
        ),
    }
    successor = _new_state(successor_policy, now_s)
    successor["schema_version"] = 3
    successor["direction_id"] = "2026-07-30T06:52:00Z"
    successor["predecessor_terminal_records"] = [
        *(state.get("predecessor_terminal_records") or []),
        terminal_record,
    ]
    successor["preregistration"] = {
        "status": "PREREGISTERED_LIVE_READY_PAPER_ONLY",
        "predecessor_policy_fingerprint": frozen["policy_fingerprint"],
        "allowed_policy_fields": FULL_BAND_SUCCESSOR_ALLOWED_POLICY_FIELDS,
        "changed_policy_fields": changed_fields,
        "rationale": (
            "full 0.0-1.0 WIDE band matches the whole-distribution c539 edge; "
            "$1 minimum remains venue-executable and comparable"
        ),
        "min_order_unchanged_usd": 1.0,
        "promotion_requires_separately_authorized_live_override_rewrite": True,
    }
    return successor


def _captured_source_row(row: dict[str, Any], *, registered_s: float) -> dict[str, Any] | None:
    if str(row.get("source_wallet") or "").lower() != WALLET:
        return None
    if str(row.get("action") or "").upper() != "BUY":
        return None
    observed_s = num(row.get("observed_ts") or row.get("event_ts"))
    start_s = _window_start(row)
    if observed_s < registered_s or start_s is None:
        return None
    return {
        "event_id": _event_key(row),
        "source_fingerprint": str(row.get("source_fingerprint") or ""),
        "market_slug": str(row.get("market_slug") or row.get("event_slug") or ""),
        "window_start_s": start_s,
        "condition_id": str(row.get("condition_id") or ""),
        "token_id": str(row.get("token_id") or ""),
        "outcome": str(row.get("outcome") or ""),
        "source": str(row.get("source") or ""),
        "event_ts": num(row.get("event_ts")),
        "observed_ts": observed_s,
        "source_price": num(row.get("price")),
        "source_size": num(row.get("size")),
        "source_usdc": num(row.get("usdc_size"), num(row.get("price")) * num(row.get("size"))),
        "seconds_before_open": round(max(0.0, start_s - observed_s), 6),
        "not_open_yet": observed_s < start_s,
    }


def _group_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            str(row.get("market_slug") or ""),
            str(row.get("token_id") or ""),
            str(row.get("outcome") or "").upper(),
        )
    )


def _fillable_at_cap(book: dict[str, Any], *, requested_usd: float, cap_price: float) -> dict[str, Any]:
    asks = sorted(
        (
            (num(level.get("price")), num(level.get("size")))
            for level in book.get("asks") or []
            if isinstance(level, dict)
        ),
        key=lambda value: value[0],
    )
    remaining = max(0.0, requested_usd)
    spent = shares = 0.0
    for price, available_shares in asks:
        if price <= 0 or available_shares <= 0:
            continue
        if price > cap_price + 1e-12:
            break
        take_usd = min(remaining, price * available_shares)
        shares += take_usd / price
        spent += take_usd
        remaining -= take_usd
        if remaining <= 1e-9:
            break
    return {
        "best_ask": round(asks[0][0], 6) if asks else 0.0,
        "requested_usd": round(requested_usd, 6),
        "fillable_usd": round(spent, 6),
        "fillable_shares": round(shares, 6),
        "avg_fill_price": round(spent / shares, 6) if shares > 0 else 0.0,
        "fill_ratio": round(spent / requested_usd, 6) if requested_usd > 0 else 0.0,
    }


def _resolution_index(path: str | Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in _read_jsonl(path):
        slug = str(row.get("market_slug") or row.get("slug") or "")
        if slug:
            result[slug] = row
    return result


def _winner(resolution: dict[str, Any]) -> str:
    value = str(
        resolution.get("winner")
        or resolution.get("outcome")
        or resolution.get("direction")
        or ""
    ).strip().upper()
    if value in {"UP", "YES"}:
        return "UP"
    if value in {"DOWN", "NO"}:
        return "DOWN"
    return value


def _normalized_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    return "UP" if text in {"UP", "YES"} else "DOWN" if text in {"DOWN", "NO"} else text


def _evaluate_group(
    rows: list[dict[str, Any]],
    *,
    policy: dict[str, Any],
    now_s: float,
    client: CLOBMarketClient,
) -> dict[str, Any]:
    first = min(rows, key=lambda row: (num(row.get("observed_ts")), str(row.get("event_id"))))
    source_inventory_usd = sum(num(row.get("source_usdc")) for row in rows)
    requested = min(
        source_inventory_usd * num(policy.get("wallet_fraction")),
        num(policy.get("max_order_usd")),
    )
    base = {
        "evaluation_id": f"c539_open_{_checksum([_group_key(first), policy['policy_fingerprint']])[:20]}",
        "market_slug": first["market_slug"],
        "window_start_s": first["window_start_s"],
        "token_id": first["token_id"],
        "condition_id": first["condition_id"],
        "outcome": first["outcome"],
        "source_event_ids": sorted(row["event_id"] for row in rows),
        "source_row_count": len(rows),
        "source_inventory_usd": round(source_inventory_usd, 6),
        "target_copy_usd": round(requested, 6),
        "policy_id": policy["policy_id"],
        "policy_fingerprint": policy["policy_fingerprint"],
        "evaluated_at": datetime.fromtimestamp(now_s, timezone.utc).isoformat(),
        "resident_up": True,
        "first_evaluation_lag_s": round(
            max(0.0, now_s - num(first["window_start_s"])),
            6,
        ),
        "paper_only": True,
        "live_orders_allowed": False,
        "resolved": False,
        "post_fee_pnl_usd": None,
    }
    if requested + 1e-12 < num(policy.get("min_order_usd")):
        return {**base, "status": "REFUSED_AT_OPEN", "reason": "inventory_below_frozen_min_order"}
    if not first.get("token_id"):
        return {**base, "status": "REFUSED_AT_OPEN", "reason": "token_id_missing"}
    try:
        book = client.get_book(str(first["token_id"]))
    except Exception as exc:  # the resident loop retries while the grace window is open
        return {
            **base,
            "status": "BOOK_RETRY",
            "reason": "clob_book_fetch_failed",
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }
    fill = _fillable_at_cap(
        book,
        requested_usd=requested,
        cap_price=num(policy.get("max_price")),
    )
    fill_pass = fill["fill_ratio"] >= 0.999
    return {
        **base,
        "status": "PAPER_FILLED_AT_OPEN" if fill_pass else "REFUSED_AT_OPEN",
        "reason": "filled" if fill_pass else "price_band_or_depth_gate",
        "book_timestamp": book.get("timestamp"),
        "book_hash": book.get("hash"),
        "frozen_min_price": policy["min_price"],
        "frozen_max_price": policy["max_price"],
        **fill,
    }


def _attach_resolutions(
    evaluations: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
) -> None:
    for row in evaluations:
        if row.get("status") != "PAPER_FILLED_AT_OPEN":
            continue
        resolution = resolutions.get(str(row.get("market_slug") or ""))
        winner = _winner(resolution or {})
        if not winner:
            continue
        won = _normalized_outcome(row.get("outcome")) == winner
        shares = num(row.get("fillable_shares"))
        cost = num(row.get("fillable_usd"))
        price = num(row.get("avg_fill_price"))
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        row.update(
            {
                "resolved": True,
                "winner": winner,
                "won": won,
                "expected_fee_usd": fee,
                "post_fee_pnl_usd": round((shares if won else 0.0) - cost - fee, 6),
            }
        )


def _summarize(state: dict[str, Any], *, now_s: float) -> None:
    raw_rows = state.get("raw_source_rows") or state.get("source_rows") or []
    source_rows = state.get("distinct_signals") or state.get("source_rows") or []
    evaluations = state["window_evaluations"]
    c539_buy_rows = len(source_rows)
    not_open_rows = sum(bool(row.get("not_open_yet")) for row in source_rows)
    fills = [row for row in evaluations if row.get("status") == "PAPER_FILLED_AT_OPEN"]
    min_order_usd = num(
        (state.get("frozen_policy") or {}).get("min_order_usd"),
        1.0,
    )
    venue_executable_fills = [
        row
        for row in fills
        if row_is_venue_executable(row, min_order_usd=min_order_usd)
    ]
    venue_executable_source_rows = [
        row
        for row in source_rows
        if row_is_venue_executable(row, min_order_usd=min_order_usd)
    ]
    frozen_band_rows = source_rows if source_rows else fills
    frozen_band_reachable_rows = (
        venue_executable_source_rows if source_rows else venue_executable_fills
    )
    resolved = sorted(
        (row for row in venue_executable_fills if row.get("resolved")),
        key=lambda row: (num(row.get("window_start_s")), str(row.get("evaluation_id"))),
    )
    midpoint = (len(resolved) + 1) // 2
    pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved), 6)
    first_pnl = (
        round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved[:midpoint]), 6)
        if resolved
        else None
    )
    second_pnl = (
        round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved[midpoint:]), 6)
        if len(resolved) > 1
        else None
    )
    bars = {
        "resolved_gte_50": len(resolved) >= 50,
        "post_fee_positive": bool(resolved) and pnl > 0,
        "both_halves_positive": bool(
            len(resolved) > 1 and num(first_pnl) > 0 and num(second_pnl) > 0
        ),
        "dual_gate": bool(venue_executable_fills),
        "f1_f4_same_identity": False,
        "venue_executable": bool(venue_executable_fills),
    }
    hourly_coverage: dict[str, dict[str, int]] = {}
    covered_evaluations = 0
    for row in evaluations:
        hour = datetime.fromtimestamp(
            num(row.get("window_start_s")),
            timezone.utc,
        ).strftime("%Y-%m-%dT%H:00:00Z")
        bucket = hourly_coverage.setdefault(hour, {"covered": 0, "total": 0})
        bucket["total"] += 1
        if row.get("status") != "OPEN_SAMPLE_MISSED":
            bucket["covered"] += 1
            covered_evaluations += 1
    coverage = (
        round(covered_evaluations / len(evaluations), 6)
        if evaluations
        else None
    )
    instrumented = [
        row for row in evaluations if row.get("first_evaluation_lag_s") is not None
    ]
    instrumented_covered = sum(
        row.get("status") != "OPEN_SAMPLE_MISSED" for row in instrumented
    )
    instrumented_coverage = (
        round(instrumented_covered / len(instrumented), 6)
        if instrumented
        else None
    )
    coverage_rows = [
        {
            "window_start_s": int(num(row.get("window_start_s"))),
            "resident_up": row.get("resident_up"),
            "first_evaluation_lag_s": row.get("first_evaluation_lag_s"),
            "missed_detected_after_s": row.get("missed_detected_after_s"),
            "covered": row.get("status") != "OPEN_SAMPLE_MISSED",
        }
        for row in evaluations
    ]
    deadline_s = datetime.fromisoformat(state["observation_deadline_at"]).timestamp()
    registered_s = datetime.fromisoformat(state["registered_at"]).timestamp()
    observation_window_s = max(1.0, deadline_s - registered_s)
    elapsed_s = min(observation_window_s, max(0.0, now_s - registered_s))
    observed_intent_rate_per_hour = (
        c539_buy_rows / (elapsed_s / 3600.0) if elapsed_s > 0 else 0.0
    )
    projected_forward_n_at_deadline = round(
        c539_buy_rows
        + observed_intent_rate_per_hour * max(0.0, deadline_s - now_s) / 3600.0,
        6,
    )
    projected_expiry_without_evidence = projected_forward_n_at_deadline < 50
    projection = {
        "status": (
            "PROJECTED_EXPIRY_WITHOUT_EVIDENCE"
            if projected_expiry_without_evidence
            else "PROJECTED_TO_REACH_EVIDENCE_GATE"
        ),
        "forward_n_now": c539_buy_rows,
        "resolved_n_now": len(resolved),
        "required_forward_n": 50,
        "elapsed_s": round(elapsed_s, 6),
        "remaining_s": round(max(0.0, deadline_s - now_s), 6),
        "observed_intent_rate_per_hour": round(observed_intent_rate_per_hour, 6),
        "projected_forward_n_at_deadline": projected_forward_n_at_deadline,
        "cause": (
            "retained forward intents accruing below the rate "
            "required to reach n=50 before the preregistered deadline"
            if projected_expiry_without_evidence
            else None
        ),
        "projection_rule": (
            "forward_n_now + observed_intent_rate_per_hour * remaining_hours; "
            "retrospective rows receive zero credit"
        ),
    }
    state["generated_at"] = datetime.fromtimestamp(now_s, timezone.utc).isoformat()
    state["status"] = (
        "PAPER_EVIDENCE_GATE_PASSED"
        if all(bars.values())
        else "OBSERVATION_COMPLETE"
        if now_s >= deadline_s
        else "ACCRUING_PAPER_ONLY"
    )
    state["summary"] = {
        "raw_rows": len(raw_rows),
        "distinct_signals": c539_buy_rows,
        "cross_feed_duplicate_rows": max(0, len(raw_rows) - c539_buy_rows),
        "c539_buy_rows": c539_buy_rows,
        "not_open_yet_rows": not_open_rows,
        "not_open_yet_share": round(not_open_rows / c539_buy_rows, 6) if c539_buy_rows else 0.0,
        "deferred_window_outcomes": len(evaluations),
        "survived_open_re_evaluation": len(fills),
        "survival_rate": round(len(fills) / len(evaluations), 6) if evaluations else 0.0,
        "venue_executable_max_price": VENUE_EXECUTABLE_MAX_PRICE,
        "venue_executable_open_fills": len(venue_executable_fills),
        "venue_unreachable_open_fills": len(fills) - len(venue_executable_fills),
        "frozen_band_venue_reachable_share_pct": (
            round(
                100.0 * len(frozen_band_reachable_rows) / len(frozen_band_rows),
                6,
            )
            if frozen_band_rows
            else None
        ),
        "open_fill_venue_reachable_share_pct": (
            round(100.0 * len(venue_executable_fills) / len(fills), 6)
            if fills
            else None
        ),
        "venue_executable_resolved": len(resolved),
        "venue_unreachable_resolved": sum(
            row.get("resolved") is True for row in fills
        )
        - len(resolved),
        "resolved": len(resolved),
        "forward_evidence_projection": projection,
        "post_fee_pnl_usd": pnl,
        "first_half_post_fee_pnl_usd": first_pnl,
        "second_half_post_fee_pnl_usd": second_pnl,
        "open_grace_covered_windows": covered_evaluations,
        "open_grace_total_windows": len(evaluations),
        "open_grace_coverage": coverage,
        "open_grace_instrumented_covered_windows": instrumented_covered,
        "open_grace_instrumented_total_windows": len(instrumented),
        "open_grace_instrumented_coverage": instrumented_coverage,
        "open_grace_coverage_below_60pct": coverage is not None and coverage < 0.60,
        "open_grace_coverage_rows": coverage_rows,
        "open_grace_coverage_by_hour": [
            {
                "hour": hour,
                **counts,
                "coverage": round(counts["covered"] / counts["total"], 6),
            }
            for hour, counts in sorted(hourly_coverage.items())
        ],
    }
    state["admission"] = {
        "eligible": all(bars.values()),
        "live_authority": False,
        "same_policy_fingerprint_only": True,
        "required_bars": bars,
        "next": (
            "package the same-identity all-pass packet for Fable; this paper state has no live authority"
            if all(bars.values())
            else "continue the frozen 24h paper clock and accrue open fills/resolutions"
        ),
    }


def _evaluation_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            str(row.get("market_slug") or ""),
            str(row.get("token_id") or ""),
            str(row.get("outcome") or "").upper(),
        )
    )


def _evaluate_pending_groups(
    state: dict[str, Any],
    *,
    args: argparse.Namespace,
    now_s: float,
    client: CLOBMarketClient,
    mark_missed: bool,
) -> None:
    evaluations = {
        _evaluation_key(row): row
        for row in state.get("window_evaluations") or []
    }
    pending_groups: dict[str, list[dict[str, Any]]] = {}
    for row in state["distinct_signals"]:
        if row.get("not_open_yet"):
            pending_groups.setdefault(_group_key(row), []).append(row)
    for key, rows in pending_groups.items():
        start_s = int(rows[0]["window_start_s"])
        existing_eval = evaluations.get(key)
        if existing_eval and existing_eval.get("status") != "BOOK_RETRY":
            continue
        if start_s <= now_s <= start_s + float(args.open_grace_s):
            evaluations[key] = _evaluate_group(
                rows,
                policy=state["frozen_policy"],
                now_s=now_s,
                client=client,
            )
        elif mark_missed and now_s > start_s + float(args.open_grace_s) and existing_eval is None:
            evaluations[key] = {
                "evaluation_id": (
                    f"c539_open_{_checksum([key, state['frozen_policy']['policy_fingerprint']])[:20]}"
                ),
                "market_slug": rows[0]["market_slug"],
                "window_start_s": start_s,
                "token_id": rows[0]["token_id"],
                "outcome": rows[0]["outcome"],
                "source_event_ids": sorted(row["event_id"] for row in rows),
                "source_row_count": len(rows),
                "source_inventory_usd": round(
                    sum(num(row.get("source_usdc")) for row in rows),
                    6,
                ),
                "policy_id": state["frozen_policy"]["policy_id"],
                "policy_fingerprint": state["frozen_policy"]["policy_fingerprint"],
                "status": "OPEN_SAMPLE_MISSED",
                "reason": "resident_not_running_inside_open_grace",
                "resident_up": None,
                "first_evaluation_lag_s": None,
                "missed_detected_after_s": round(now_s - start_s, 6),
                "paper_only": True,
                "live_orders_allowed": False,
                "resolved": False,
                "post_fee_pnl_usd": None,
            }
    state["window_evaluations"] = sorted(
        evaluations.values(),
        key=lambda row: (num(row.get("window_start_s")), str(row.get("evaluation_id"))),
    )


def _next_unevaluated_open_s(state: dict[str, Any], *, now_s: float) -> float | None:
    evaluated = {
        _evaluation_key(row)
        for row in state.get("window_evaluations") or []
        if row.get("status") != "BOOK_RETRY"
    }
    starts = [
        num(row.get("window_start_s"))
        for row in state.get("distinct_signals") or []
        if row.get("not_open_yet")
        and _group_key(row) not in evaluated
        and num(row.get("window_start_s")) > now_s
    ]
    return min(starts) if starts else None


def run_once(args: argparse.Namespace, *, now_s: float | None = None, client: CLOBMarketClient | None = None) -> dict[str, Any]:
    now_s = time.time() if now_s is None else float(now_s)
    state = load_json(args.state, default={})
    try:
        current_policy = _policy_from_guard(
            load_json(args.guard, default={}) or {}
        )
        runtime_policy_present = True
    except RuntimeError:
        frozen = (
            state.get("frozen_policy")
            if isinstance(state, dict)
            and isinstance(state.get("frozen_policy"), dict)
            else {}
        )
        if not frozen or not frozen.get("policy_fingerprint"):
            raise
        current_policy = dict(frozen)
        runtime_policy_present = False
    if not isinstance(state, dict) or state.get("kind") != "c539_deferred_open_paper_probe":
        state = _new_state(current_policy, now_s)
    frozen_policy = state["frozen_policy"]
    preregistration = (
        state.get("preregistration")
        if isinstance(state.get("preregistration"), dict)
        else {}
    )
    registered_policy_fingerprint = str(
        state.get("registered_policy_fingerprint")
        or current_policy["policy_fingerprint"]
    )
    if (
        preregistration
        and registered_policy_fingerprint
        == str(preregistration.get("predecessor_policy_fingerprint") or "")
    ):
        registered_policy_fingerprint = str(frozen_policy["policy_fingerprint"])
    state["registered_policy_fingerprint"] = registered_policy_fingerprint
    state["current_policy_fingerprint"] = current_policy["policy_fingerprint"]
    state["runtime_policy_present"] = runtime_policy_present
    state["runtime_policy_absence_expected"] = not runtime_policy_present
    runtime_policy_diff_expected = bool(
        preregistration
        and current_policy["policy_fingerprint"] != frozen_policy["policy_fingerprint"]
    )
    state["runtime_policy_diff_expected"] = runtime_policy_diff_expected
    state["policy_drift"] = (
        registered_policy_fingerprint != frozen_policy["policy_fingerprint"]
        or (
            not runtime_policy_diff_expected
            and current_policy["policy_fingerprint"] != frozen_policy["policy_fingerprint"]
        )
    )

    registered_s = datetime.fromisoformat(state["registered_at"]).timestamp()
    history = load_json(args.hot_history, default={}) or {}
    retained_raw = state.get("raw_source_rows")
    if not isinstance(retained_raw, list):
        retained_raw = state.get("source_rows") or []
    existing = {str(row.get("event_id")): row for row in retained_raw if isinstance(row, dict)}
    for raw in history.get("events") or []:
        if not isinstance(raw, dict):
            continue
        captured = _captured_source_row(raw, registered_s=registered_s)
        if captured is not None:
            existing.setdefault(captured["event_id"], captured)
    state["raw_source_rows"] = sorted(
        existing.values(),
        key=lambda row: (num(row.get("observed_ts")), str(row.get("event_id"))),
    )
    state["distinct_signals"] = canonicalize_source_rows(state["raw_source_rows"])
    # Backward-compatible field now carries canonical economic signals.
    state["source_rows"] = state["distinct_signals"]

    book_client = client or CLOBMarketClient(host=args.clob_base, timeout_s=args.timeout_s, retries=1)
    _evaluate_pending_groups(
        state,
        args=args,
        now_s=now_s,
        client=book_client,
        mark_missed=True,
    )
    _attach_resolutions(state["window_evaluations"], _resolution_index(args.resolutions))
    _summarize(state, now_s=now_s)
    atomic_write_json(args.state, state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hot-history", default=DEFAULT_HOT_HISTORY)
    parser.add_argument("--guard", default=DEFAULT_GUARD)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--clob-base", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-s", type=float, default=3.0)
    parser.add_argument("--open-grace-s", type=float, default=15.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--duration-s", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--preregister-wf050-successor", action="store_true")
    parser.add_argument("--preregister-full-band-successor", action="store_true")
    args = parser.parse_args()

    if args.preregister_wf050_successor:
        state = load_json(args.state, default={}) or {}
        successor = preregister_wf050_successor(state, now_s=time.time())
        atomic_write_json(args.state, successor)
        print(
            json.dumps(
                {
                    "status": successor["status"],
                    "policy_fingerprint": successor["frozen_policy"]["policy_fingerprint"],
                    "wallet_fraction": successor["frozen_policy"]["wallet_fraction"],
                    "terminal_status": successor["predecessor_terminal_records"][-1]["status"],
                },
                sort_keys=True,
            )
        )
        return 0
    if args.preregister_full_band_successor:
        state = load_json(args.state, default={}) or {}
        successor = preregister_full_band_successor(state, now_s=time.time())
        atomic_write_json(args.state, successor)
        print(
            json.dumps(
                {
                    "status": successor["status"],
                    "policy_fingerprint": successor["frozen_policy"]["policy_fingerprint"],
                    "max_price": successor["frozen_policy"]["max_price"],
                    "terminal_status": successor["predecessor_terminal_records"][-1]["status"],
                },
                sort_keys=True,
            )
        )
        return 0

    started = time.monotonic()
    book_client = CLOBMarketClient(host=args.clob_base, timeout_s=args.timeout_s, retries=1)
    while True:
        state = run_once(args, client=book_client)
        if args.once or args.duration_s <= 0 or time.monotonic() - started >= args.duration_s:
            print(json.dumps({"status": state["status"], **state["summary"]}, sort_keys=True))
            return 0
        next_open_s = _next_unevaluated_open_s(state, now_s=time.time())
        if next_open_s is not None:
            time.sleep(max(0.0, next_open_s - time.time()))
            wake_s = time.time()
            _evaluate_pending_groups(
                state,
                args=args,
                now_s=wake_s,
                client=book_client,
                mark_missed=False,
            )
            _summarize(state, now_s=wake_s)
            atomic_write_json(args.state, state)
        else:
            time.sleep(max(0.1, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
