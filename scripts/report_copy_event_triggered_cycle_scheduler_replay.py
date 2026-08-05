#!/usr/bin/env python3
"""Replay the copy-event-triggered cycle scheduler hypothesis.

Flow stage: LEARN/SELF-DEV. This is a paper-only counterfactual report: it
counts SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP windows that would have met the
30s freshness horizon if the guard scheduled an evaluation cycle when the
source event arrived.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_WAVE_ATTRIBUTION = "data/research/wave_gate_attribution_20260714T2000Z.json"
DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_OUTPUT = "data/research/copy_event_triggered_cycle_scheduler_replay_20260715.json"
TARGET_CLASS = "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP"
MARKET_SLUG_TS_RE = re.compile(r"-(\d{10})(?:$|[^0-9])")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _market_close_from_slug(market_slug: Any, *, window_seconds: int = 300) -> datetime | None:
    match = MARKET_SLUG_TS_RE.search(str(market_slug or ""))
    if not match:
        return None
    return datetime.fromtimestamp(int(match.group(1)) + int(window_seconds), tz=timezone.utc)


def _seconds_between(later: datetime | None, earlier: datetime | None) -> float | None:
    if later is None or earlier is None:
        return None
    return (later - earlier).total_seconds()


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _load_guard_cycles(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            profile = event.get("guard_loop_profile") if isinstance(event.get("guard_loop_profile"), dict) else {}
            cycle_started_at = _parse_iso(profile.get("cycle_started_at") or event.get("generated_at"))
            if cycle_started_at is None:
                continue
            rows.append(
                {
                    "cycle_started_at": cycle_started_at,
                    "cycle_started_at_iso": cycle_started_at.isoformat().replace("+00:00", "Z"),
                    "source_wallet": str(event.get("source_wallet") or "").lower(),
                    "candidate_id": str(event.get("candidate_id") or ""),
                    "pid": event.get("pid"),
                    "cycle": event.get("cycle"),
                }
            )
    return rows


def _matching_cycles(
    cycles: list[dict[str, Any]],
    *,
    source_wallet: str,
    candidate_id: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    wallet = source_wallet.lower()
    return [
        row
        for row in cycles
        if start <= row["cycle_started_at"] <= end
        and (row.get("source_wallet") == wallet or row.get("candidate_id") == candidate_id)
    ]


def _nearest_cycle_metrics(cycles: list[dict[str, Any]], event_at: datetime) -> dict[str, Any]:
    if not cycles:
        return {
            "actual_matching_cycle_count": 0,
            "nearest_actual_cycle_delta_s": None,
            "next_actual_cycle_lag_s": None,
            "nearest_actual_cycle_started_at": None,
        }
    nearest = min(cycles, key=lambda row: abs((row["cycle_started_at"] - event_at).total_seconds()))
    after_cycles = [row for row in cycles if row["cycle_started_at"] >= event_at]
    next_cycle = min(after_cycles, key=lambda row: row["cycle_started_at"]) if after_cycles else None
    return {
        "actual_matching_cycle_count": len(cycles),
        "nearest_actual_cycle_delta_s": round((nearest["cycle_started_at"] - event_at).total_seconds(), 6),
        "next_actual_cycle_lag_s": (
            round((next_cycle["cycle_started_at"] - event_at).total_seconds(), 6) if next_cycle else None
        ),
        "nearest_actual_cycle_started_at": nearest.get("cycle_started_at_iso"),
    }


def replay_scheduler(
    wave_attribution: dict[str, Any],
    guard_cycles: list[dict[str, Any]],
    *,
    generated_at: str,
    fresh_horizon_s: float,
    trigger_delay_s: float,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    members = wave_attribution.get("members") if isinstance(wave_attribution.get("members"), list) else []
    target_members = [member for member in members if member.get("classification") == TARGET_CLASS]
    for member in target_members:
        source_wallet = str(member.get("source_wallet") or "")
        candidate_id = str(member.get("candidate_id") or "")
        source_activity = member.get("source_activity") if isinstance(member.get("source_activity"), dict) else {}
        details = (
            source_activity.get("policy_window_details")
            if isinstance(source_activity.get("policy_window_details"), list)
            else []
        )
        for detail in details:
            if bool(detail.get("cycle_fresh_overlap")):
                continue
            event_at = _parse_iso(detail.get("first_policy_event_iso") or detail.get("first_event_iso"))
            received_at = _parse_iso(detail.get("first_policy_received_iso") or detail.get("first_received_iso"))
            close_at = _market_close_from_slug(detail.get("market_slug"))
            if received_at is None:
                continue
            trigger_at = received_at + timedelta(seconds=float(trigger_delay_s))
            event_to_trigger_s = _seconds_between(trigger_at, event_at or received_at)
            trigger_before_close_s = _seconds_between(close_at, trigger_at)
            in_horizon = event_to_trigger_s is not None and event_to_trigger_s <= fresh_horizon_s
            before_close = trigger_before_close_s is None or trigger_before_close_s >= 0
            recovered = bool(in_horizon and before_close)
            nearby_start = received_at.replace(microsecond=0)
            nearby_cycles = _matching_cycles(
                guard_cycles,
                source_wallet=source_wallet,
                candidate_id=candidate_id,
                start=nearby_start.replace(tzinfo=timezone.utc) if nearby_start.tzinfo is None else nearby_start,
                end=(close_at if close_at is not None else received_at),
            )
            metrics = _nearest_cycle_metrics(nearby_cycles, received_at)
            rows.append(
                {
                    "source_wallet": source_wallet,
                    "candidate_id": candidate_id,
                    "market_slug": detail.get("market_slug"),
                    "source_rows": int(detail.get("rows") or 0),
                    "actual_cycle_fresh_overlap": bool(detail.get("cycle_fresh_overlap")),
                    "actual_event_cycle_fresh_overlap": bool(detail.get("event_cycle_fresh_overlap")),
                    "first_policy_event_iso": detail.get("first_policy_event_iso") or detail.get("first_event_iso"),
                    "first_policy_received_iso": detail.get("first_policy_received_iso") or detail.get("first_received_iso"),
                    "counterfactual_trigger_at_iso": (
                        (trigger_at.isoformat().replace("+00:00", "Z")) if trigger_at else None
                    ),
                    "counterfactual_trigger_delay_s": float(trigger_delay_s),
                    "counterfactual_event_to_trigger_s": (
                        round(event_to_trigger_s + trigger_delay_s, 6) if event_to_trigger_s is not None else None
                    ),
                    "market_close_iso": close_at.isoformat().replace("+00:00", "Z") if close_at else None,
                    "counterfactual_trigger_before_close_s": (
                        round(trigger_before_close_s, 6) if trigger_before_close_s is not None else None
                    ),
                    "counterfactual_fresh_overlap_recovered": recovered,
                    **metrics,
                }
            )

    recovered_rows = [row for row in rows if row["counterfactual_fresh_overlap_recovered"]]
    summary = {
        "target_class": TARGET_CLASS,
        "target_members": len(target_members),
        "target_policy_windows": len(rows),
        "recovered_source_active_but_no_fresh_cycle_overlap_windows": len(recovered_rows),
        "recovered_members": len({row["source_wallet"] for row in recovered_rows}),
        "fresh_horizon_s": float(fresh_horizon_s),
        "trigger_delay_s": float(trigger_delay_s),
        "actual_total_cycles_landed_from_wave_packet": (
            wave_attribution.get("summary", {}).get("total_cycles_landed")
            if isinstance(wave_attribution.get("summary"), dict)
            else None
        ),
    }
    status = "PASS_PAPER_SEED_NEXT" if summary["recovered_source_active_but_no_fresh_cycle_overlap_windows"] > 0 else "FAIL_TOMBSTONE_R8"
    return {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_replay",
        "flow_stage": "LEARN/SELF-DEV",
        "generated_at": generated_at,
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintent_parity_change": False,
        "single_submitter_change": False,
        "decision": (
            "PAPER_SEED_NEXT; live activation still requires Fable decision and managed restart"
            if status == "PASS_PAPER_SEED_NEXT"
            else "TOMBSTONE_R8_FOR_THIS_WAVE"
        ),
        "inputs": {
            "wave_attribution_kind": wave_attribution.get("kind"),
            "wave_generated_at": wave_attribution.get("generated_at"),
        },
        "summary": summary,
        "rows": rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wave-attribution", default=DEFAULT_WAVE_ATTRIBUTION)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--fresh-horizon-s", type=float, default=30.0)
    parser.add_argument("--trigger-delay-s", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    wave_path = _rooted(args.wave_attribution)
    guard_events_path = _rooted(args.guard_events)
    output_path = _rooted(args.output)
    payload = replay_scheduler(
        _load_json(wave_path),
        _load_guard_cycles(guard_events_path),
        generated_at=_utc_now_iso(),
        fresh_horizon_s=args.fresh_horizon_s,
        trigger_delay_s=args.trigger_delay_s,
    )
    payload["inputs"].update(
        {
            "wave_attribution": str(wave_path.relative_to(ROOT) if wave_path.is_relative_to(ROOT) else wave_path),
            "guard_events": str(
                guard_events_path.relative_to(ROOT) if guard_events_path.is_relative_to(ROOT) else guard_events_path
            ),
        }
    )
    atomic_write_json(output_path, payload)
    print(json.dumps({"status": payload["status"], **payload["summary"]}, sort_keys=True))
    return 0 if str(payload["status"]).startswith("PASS") else 2


if __name__ == "__main__":
    raise SystemExit(main())
