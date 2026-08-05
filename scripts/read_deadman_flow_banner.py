#!/usr/bin/env python3
"""Read the last order-flow deadman state for ask_fable's prompt banner."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


DEFAULT_STATE = "data/research/order_flow_deadman_state.json"
DEFAULT_STDERR_LOG = "data/research/ask_fable_deadman_banner_stderr.log"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fail_closed(*, reason: str, now: datetime, checked_at: str | None = None, age_s: float | None = None) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "order_flow_deadman_banner",
        "status": "INCIDENT_ORDER_FLOW_DEAD_UNVERIFIED",
        "checked_at": checked_at,
        "banner_checked_at": now.isoformat(),
        "deadman_banner_age_s": None if age_s is None else round(age_s, 6),
        "fail_closed_reason": reason,
        "next_action": "readable order_flow_deadman_state.json <= 15m old is required before rendering flow clear",
    }


def build_banner_state(
    *,
    state_path: Path,
    now: datetime,
    max_age_s: float,
    stderr_log: Path | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except OSError as exc:
        if stderr_log is not None:
            stderr_log.parent.mkdir(parents=True, exist_ok=True)
            stderr_log.write_text(f"state_read_error: {exc}\n", encoding="utf-8")
        return _fail_closed(reason="state_missing_or_unreadable", now=now)
    except json.JSONDecodeError as exc:
        if stderr_log is not None:
            stderr_log.parent.mkdir(parents=True, exist_ok=True)
            stderr_log.write_text(f"state_json_error: {exc}\n", encoding="utf-8")
        return _fail_closed(reason="state_unparseable", now=now)

    checked = _parse_ts(payload.get("checked_at") or payload.get("generated_at"))
    if checked is None:
        return _fail_closed(reason="state_missing_checked_at", now=now)
    age_s = max(0.0, (now - checked).total_seconds())
    if age_s > float(max_age_s):
        return _fail_closed(
            reason="state_stale",
            now=now,
            checked_at=checked.isoformat(),
            age_s=age_s,
        )
    policy_choke = payload.get("policy_choke") if isinstance(payload.get("policy_choke"), dict) else {}
    active_set_runtime = (
        payload.get("active_set_runtime")
        if isinstance(payload.get("active_set_runtime"), dict)
        else {}
    )
    selected_member = (
        active_set_runtime.get("selected_member")
        if isinstance(active_set_runtime.get("selected_member"), dict)
        else {}
    )
    selected_identity = (
        payload.get("selected_identity")
        if isinstance(payload.get("selected_identity"), dict)
        else {}
    )
    return {
        "schema_version": 1,
        "kind": "order_flow_deadman_banner",
        "status": payload.get("status"),
        "checked_at": payload.get("checked_at") or payload.get("generated_at"),
        "idle_s": payload.get("idle_s"),
        "max_idle_s": payload.get("max_idle_s"),
        "can_trade": payload.get("can_trade"),
        "can_trade_reason": payload.get("can_trade_reason"),
        "money_anchored_status": payload.get("money_anchored_status"),
        "mechanical_escalation": payload.get("mechanical_escalation"),
        "deadman_class": payload.get("deadman_class"),
        "wallet_policy_diagnostic": payload.get("wallet_policy_diagnostic")
        or policy_choke.get("wallet_policy_diagnostic")
        or payload.get("episode_fire_wallet_policy_diagnostic"),
        "selected_candidate_id": payload.get("selected_candidate_id")
        or selected_member.get("candidate_id"),
        "selected_wallet": payload.get("selected_wallet") or selected_member.get("source_wallet"),
        "selected_policy_id": payload.get("selected_policy_id") or selected_member.get("policy_id"),
        "selected_identity": {
            key: selected_identity.get(key)
            for key in ("status", "source_wallet", "candidate_id", "generated_at", "source")
            if selected_identity.get(key) is not None
        },
        "selected_identity_resolved": payload.get("selected_identity_resolved"),
        "active_member_count": payload.get("active_member_count")
        or active_set_runtime.get("member_count"),
        "qualified_member_count": payload.get("qualified_member_count")
        or active_set_runtime.get("qualified_member_count"),
        "accepted_orders": payload.get("accepted_orders"),
        "latest_submitted_at": payload.get("latest_submitted_at"),
        "deadman_banner_age_s": round(age_s, 6),
        "deadman_banner_source": str(state_path),
        "deadman_banner_freshness_status": "PASS_FRESH_STATE",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--max-age-s", type=float, default=15 * 60)
    parser.add_argument("--stderr-log", default=DEFAULT_STDERR_LOG)
    parser.add_argument("--now", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = _parse_ts(args.now) if args.now else _utc_now()
    if now is None:
        now = _utc_now()
    payload = build_banner_state(
        state_path=Path(args.state),
        now=now,
        max_age_s=float(args.max_age_s),
        stderr_log=Path(args.stderr_log) if args.stderr_log else None,
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
