#!/usr/bin/env python3
"""Persist operator-notification dedupe and daily incident aggregates."""

from __future__ import annotations

import datetime as dt
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE = ROOT / "data/research/operator_notification_discipline_state.json"
DEFAULT_EVENTS = ROOT / "data/research/operator_notification_discipline_events.jsonl"
STABLE_INCIDENT_CLASS_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")


def is_stable_incident_class(value: Any) -> bool:
    return bool(STABLE_INCIDENT_CLASS_RE.fullmatch(str(value or "").strip().upper()))


def record_operator_event(
    *,
    incident_class: str,
    message: str,
    title: str,
    actionable: bool = False,
    self_healed: bool = False,
    state_path: str | Path = DEFAULT_STATE,
    events_path: str | Path = DEFAULT_EVENTS,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Notify only on a new class or a newly actionable transition."""

    now = now or dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")
    day = now.date().isoformat()
    incident_class = str(incident_class or "UNKNOWN_INCIDENT").strip().upper()
    state = load_json(state_path, default={})
    state = state if isinstance(state, dict) else {}
    seen = {
        str(value)
        for value in state.get("seen_incident_classes") or []
        if is_stable_incident_class(value)
    }
    active_actionable = {
        str(value)
        for value in state.get("active_actionable_classes") or []
        if is_stable_incident_class(value)
    }
    daily = state.get("daily_counts") if isinstance(state.get("daily_counts"), dict) else {}
    daily = {
        str(day_key): {
            str(class_key): class_row
            for class_key, class_row in day_rows.items()
            if is_stable_incident_class(class_key) and isinstance(class_row, dict)
        }
        for day_key, day_rows in daily.items()
        if isinstance(day_rows, dict)
    }
    day_counts = daily.get(day) if isinstance(daily.get(day), dict) else {}
    row = day_counts.get(incident_class) if isinstance(day_counts.get(incident_class), dict) else {}
    count = int(row.get("count") or 0) + 1

    is_new_class = incident_class not in seen
    if self_healed:
        notify = False
        decision_reason = "self_healed_logged_not_pushed"
        active_actionable.discard(incident_class)
    elif actionable and incident_class not in active_actionable:
        notify = True
        decision_reason = "new_actionable_transition"
        active_actionable.add(incident_class)
    elif is_new_class:
        notify = True
        decision_reason = "first_occurrence_new_incident_class"
    else:
        notify = False
        decision_reason = "repeat_aggregated_not_pushed"

    seen.add(incident_class)
    day_counts[incident_class] = {
        "count": count,
        "actionable_count": int(row.get("actionable_count") or 0) + int(bool(actionable)),
        "self_healed_count": int(row.get("self_healed_count") or 0) + int(bool(self_healed)),
        "push_count": int(row.get("push_count") or 0) + int(bool(notify)),
        "last_seen_at": now_iso,
        "last_message": str(message),
    }
    daily[day] = day_counts
    kept_days = sorted(daily)[-14:]
    updated = {
        "schema_version": 1,
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": now_iso,
        "seen_incident_classes": sorted(seen),
        "active_actionable_classes": sorted(active_actionable),
        "daily_counts": {key: daily[key] for key in kept_days},
        "rule": (
            "push only the first occurrence of a new incident class or a newly actionable "
            "operator transition; aggregate repeats and self-healed events by UTC day"
        ),
    }
    atomic_write_json(state_path, updated)
    event = {
        "generated_at": now_iso,
        "day_utc": day,
        "incident_class": incident_class,
        "message": str(message),
        "title": str(title),
        "actionable": bool(actionable),
        "self_healed": bool(self_healed),
        "notify": bool(notify),
        "decision_reason": decision_reason,
        "daily_count": count,
    }
    append_jsonl(events_path, event)

    push_enabled = os.getenv("POLYMARKET_DEADMAN_NOTIFY", "1") != "0"
    if notify and push_enabled:
        try:
            safe_message = str(message).replace('"', "'")
            safe_title = str(title).replace('"', "'")
            subprocess.run(
                ["osascript", "-e", f'display notification "{safe_message}" with title "{safe_title}"'],
                check=False,
                timeout=5,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.SubprocessError):
            pass
    return {**event, "push_enabled": push_enabled}
