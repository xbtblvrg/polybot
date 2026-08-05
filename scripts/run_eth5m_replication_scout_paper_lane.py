#!/usr/bin/env python3
"""Accrue prospective ETH-5m paper intents from the shared RTDS feed.

Flow stage: DISCOVER/OBSERVE. This collector is paper-only and has no order
submission path. It admits only fresh events observed after the preregistration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


EXPERIMENT_ID = "eth5m-replication-scout-paper-20260720"
DEFAULT_SOURCE = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_STATE = "data/research/eth5m_replication_scout_paper_state.json"
DEFAULT_EVENTS = "data/research/eth5m_replication_scout_paper_events.jsonl"
PREREGISTERED_AT = 1784506995.0  # 2026-07-20T00:23:15Z
MAX_SOURCE_AGE_S = 30.0


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _eligible(row: dict[str, Any]) -> tuple[bool, str, float | None]:
    if row.get("event") != "rtds_trade_event":
        return False, "not_trade_event", None
    slug = str(row.get("market_slug") or "").lower()
    if not (slug.startswith("eth-updown-5m-") or slug.startswith("ethereum-updown-5m-")):
        return False, "not_eth5m", None
    if str(row.get("side") or "").upper() != "BUY":
        return False, "not_buy", None
    try:
        event_ts = float(row.get("event_ts"))
        received_at = float(row.get("received_at_s"))
    except (TypeError, ValueError):
        return False, "missing_timestamp", None
    age_s = received_at - event_ts
    if event_ts < PREREGISTERED_AT:
        return False, "before_preregistration", age_s
    if age_s < 0 or age_s >= MAX_SOURCE_AGE_S:
        return False, "source_age_outside_gate", age_s
    if not row.get("condition_id") or not row.get("asset") or not row.get("source_wallet"):
        return False, "copyintent_identity_missing", age_s
    try:
        price = float(row.get("price"))
    except (TypeError, ValueError):
        return False, "missing_price", age_s
    if not 0 < price < 1:
        return False, "invalid_price", age_s
    return True, "eligible", age_s


def _intent_key(row: dict[str, Any]) -> str:
    raw = "|".join(
        str(row.get(key) or "").lower()
        for key in ("market_slug", "source_wallet", "asset")
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _paper_intent(row: dict[str, Any], age_s: float) -> dict[str, Any]:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return {
        "intent_key": _intent_key(row),
        "source_event_id": row.get("event_id"),
        "source_wallet": str(row.get("source_wallet") or "").lower(),
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("market_slug"),
        "token_id": str(row.get("asset")),
        "outcome": raw.get("outcome"),
        "side": "BUY",
        "source_price": round(float(row.get("price")), 8),
        "paper_notional_usd": 1.0,
        "source_event_ts": float(row.get("event_ts")),
        "observed_at_s": float(row.get("received_at_s")),
        "observed_at": _iso(float(row.get("received_at_s"))),
        "source_age_s": round(age_s, 6),
        "resolution_status": "PENDING",
        "post_fee_pnl_usd": None,
    }


def collect_once(
    source: Path,
    state_path: Path,
    events_path: Path,
    *,
    initial_tail_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    now = time.time()
    prior = load_json(state_path, default={}) or {}
    prior_intents = prior.get("paper_intents") if isinstance(prior.get("paper_intents"), list) else []
    legacy_intents = [row for row in prior_intents if isinstance(row, dict)]
    seen = {
        str(key) if len(str(key)) == 40 else hashlib.sha1(str(key).encode("utf-8")).hexdigest()
        for key in prior.get("seen_intent_keys") or []
        if key
    }
    seen.update(
        str(key) if len(str(key)) == 40 else hashlib.sha1(str(key).encode("utf-8")).hexdigest()
        for row in legacy_intents
        if (key := row.get("intent_key"))
    )
    window_slugs = {str(slug) for slug in prior.get("window_slugs") or [] if slug}
    window_slugs.update(str(row.get("market_slug") or "") for row in legacy_intents)
    observations = int(prior.get("observations") or len(legacy_intents))
    first_observed = prior.get("first_observed_at_s")
    if first_observed is None:
        first_observed = min((float(row["observed_at_s"]) for row in legacy_intents), default=None)
    source_size = source.stat().st_size
    from_state = prior.get("source_offset_bytes") is not None
    offset = (
        int(prior["source_offset_bytes"])
        if from_state
        else max(0, source_size - initial_tail_bytes)
    )
    if offset > source_size:
        from_state = False
        offset = max(0, source_size - initial_tail_bytes)
    admitted: list[dict[str, Any]] = []
    rejected_counts: dict[str, int] = {}
    with source.open("rb") as handle:
        handle.seek(offset)
        if offset and not from_state:
            handle.readline()
        while line := handle.readline():
            try:
                row = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                rejected_counts["invalid_json"] = rejected_counts.get("invalid_json", 0) + 1
                continue
            passed, reason, age_s = _eligible(row)
            if not passed:
                rejected_counts[reason] = rejected_counts.get(reason, 0) + 1
                continue
            key = _intent_key(row)
            if key in seen:
                rejected_counts["duplicate_wallet_window_outcome"] = rejected_counts.get(
                    "duplicate_wallet_window_outcome", 0
                ) + 1
                continue
            intent = _paper_intent(row, float(age_s))
            seen.add(key)
            window_slugs.add(str(intent["market_slug"]))
            observations += 1
            observed_at_s = float(intent["observed_at_s"])
            first_observed = observed_at_s if first_observed is None else min(float(first_observed), observed_at_s)
            admitted.append(intent)
        next_offset = handle.tell()
    if admitted:
        events_path.parent.mkdir(parents=True, exist_ok=True)
        with events_path.open("a", encoding="utf-8") as handle:
            for row in admitted:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    parity_pass = all(
        row.get("intent_key")
        and row.get("condition_id")
        and row.get("token_id")
        and row.get("side") == "BUY"
        and row.get("paper_notional_usd") == 1.0
        for row in admitted
    )
    state = {
        "schema_version": 2,
        "kind": "eth5m_replication_scout_paper_lane",
        "experiment_id": EXPERIMENT_ID,
        "flow_stage": "DISCOVER/OBSERVE",
        "generated_at": _iso(now),
        "status": "ACCRUING" if observations else "ACCRUING_WAITING_FIRST_ELIGIBLE_EVENT",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_order_attempts": 0,
        "copyintent_parity": (
            "PASS_EVENT_TO_PAPER_INTENT_FIELDS"
            if observations and parity_pass
            else "PENDING_FIRST_INTENT" if not observations else "FAIL"
        ),
        "single_guard_path": "paper_only_no_submitter; promotion must route CopyIntent through run_wallet_copy_live_guard.py",
        "eligibility_rule": "first fresh BUY per wallet+ETH5m window+token; observed after prereg; source_age_s < 30",
        "preregistered_at": _iso(PREREGISTERED_AT),
        "max_source_age_s": MAX_SOURCE_AGE_S,
        "source_path": str(source),
        "source_offset_bytes": next_offset,
        "events_path": str(events_path),
        "seen_intent_keys": sorted(seen),
        "window_slugs": sorted(window_slugs),
        "observations": observations,
        "distinct_windows": len(window_slugs),
        "resolved_intents": int(prior.get("resolved_intents") or 0),
        "resolved_windows": int(prior.get("resolved_windows") or 0),
        "post_fee_pnl_usd": prior.get("post_fee_pnl_usd"),
        "fee_model": prior.get("fee_model"),
        "resolution_gate": prior.get("resolution_gate"),
        "promotion_gate_pass": prior.get("promotion_gate_pass"),
        "first_observed_at_s": first_observed,
        "first_observed_at": _iso(first_observed) if first_observed is not None else None,
        "accrual_age_s": round(now - first_observed, 6) if first_observed is not None else 0.0,
        "last_scan": {
            "admitted": len(admitted),
            "rejected_counts": rejected_counts,
            "source_size_bytes": source_size,
        },
    }
    atomic_write_json(state_path, state)
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--events", default=DEFAULT_EVENTS)
    parser.add_argument("--sleep-s", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = Path(args.source)
    state = Path(args.state)
    events = Path(args.events)
    while True:
        payload = collect_once(source, state, events)
        print(json.dumps({key: payload[key] for key in ("status", "observations", "distinct_windows")}), flush=True)
        if args.once:
            return 0
        time.sleep(max(0.25, args.sleep_s))


if __name__ == "__main__":
    raise SystemExit(main())
