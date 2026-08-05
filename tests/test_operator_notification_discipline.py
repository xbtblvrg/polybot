from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from scripts.operator_notification_discipline import record_operator_event
from scripts.order_flow_deadman import _notify


def test_new_class_pushes_once_then_daily_repeats_aggregate(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"
    now = dt.datetime(2026, 7, 28, 20, tzinfo=dt.timezone.utc)

    first = record_operator_event(
        incident_class="FEED_PARSE",
        message="bad frame",
        title="test",
        state_path=state,
        events_path=events,
        now=now,
    )
    repeat = record_operator_event(
        incident_class="FEED_PARSE",
        message="bad frame again",
        title="test",
        state_path=state,
        events_path=events,
        now=now + dt.timedelta(minutes=1),
    )

    assert first["notify"] is True
    assert repeat["notify"] is False
    stored = json.loads(state.read_text())
    aggregate = stored["daily_counts"]["2026-07-28"]["FEED_PARSE"]
    assert aggregate["count"] == 2
    assert aggregate["push_count"] == 1


def test_self_healed_event_is_logged_and_never_pushed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    result = record_operator_event(
        incident_class="FEED_GAP",
        message="feed resumed",
        title="test",
        self_healed=True,
        state_path=tmp_path / "state.json",
        events_path=tmp_path / "events.jsonl",
        now=dt.datetime(2026, 7, 28, 20, tzinfo=dt.timezone.utc),
    )

    assert result["notify"] is False
    assert result["decision_reason"] == "self_healed_logged_not_pushed"


def test_order_flow_notifications_use_stable_class_and_temp_root(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    first = _notify(
        "No live orders for 45 min",
        incident_class="ORDER_FLOW_DEADMAN_INCIDENT",
        root_path=tmp_path,
    )
    repeat = _notify(
        "No live orders for 60 min",
        incident_class="ORDER_FLOW_DEADMAN_INCIDENT",
        root_path=tmp_path,
    )

    assert first["notify"] is True
    assert repeat["notify"] is False
    assert (tmp_path / "data/research/operator_notification_discipline_state.json").exists()


def test_legacy_message_shaped_classes_are_pruned_from_aggregate(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "seen_incident_classes": ["NO LIVE ORDERS FOR 60 MIN", "ORDER_FLOW_DEADMAN_POLICY_CHOKE"],
                "daily_counts": {
                    "2026-07-28": {
                        "NO LIVE ORDERS FOR 60 MIN": {"count": 99},
                        "ORDER_FLOW_DEADMAN_POLICY_CHOKE": {"count": 2},
                    }
                },
            }
        )
    )

    record_operator_event(
        incident_class="ORDER_FLOW_DEADMAN_POLICY_CHOKE",
        message="repeat",
        title="test",
        state_path=state,
        events_path=tmp_path / "events.jsonl",
        now=dt.datetime(2026, 7, 28, 21, tzinfo=dt.timezone.utc),
    )

    stored = json.loads(state.read_text())
    assert "NO LIVE ORDERS FOR 60 MIN" not in stored["seen_incident_classes"]
    assert "NO LIVE ORDERS FOR 60 MIN" not in stored["daily_counts"]["2026-07-28"]
    assert stored["daily_counts"]["2026-07-28"]["ORDER_FLOW_DEADMAN_POLICY_CHOKE"]["count"] == 3
