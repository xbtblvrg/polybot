from __future__ import annotations

import json
from pathlib import Path

from scripts.probe_polymarket_activity_ws import _is_recv_timeout, _record_circuit_event
from src.wallet_copy.runtime_paths import LEGACY_RTDS_ACTIVITY_JSONL, rtds_activity_jsonl


def test_runtime_path_comes_from_shared_config(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "runtime_paths.json"
    config.write_text(json.dumps({"rtds_activity_jsonl": "data/research/shared.jsonl"}))
    monkeypatch.delenv("WALLET_COPY_RTDS_JSONL", raising=False)

    assert rtds_activity_jsonl(config) == "data/research/shared.jsonl"


def test_runtime_path_falls_back_when_config_is_invalid(tmp_path: Path, monkeypatch) -> None:
    config = tmp_path / "runtime_paths.json"
    config.write_text("not-json")
    monkeypatch.delenv("WALLET_COPY_RTDS_JSONL", raising=False)

    assert rtds_activity_jsonl(config) == LEGACY_RTDS_ACTIVITY_JSONL


def test_circuit_breaker_names_crash_loop_on_third_start(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"

    first = _record_circuit_event(
        state_path=str(state),
        events_path=str(events),
        status="STARTING",
        reason="start",
        crash_loop_window_s=300,
        crash_loop_threshold=3,
    )
    second = _record_circuit_event(
        state_path=str(state),
        events_path=str(events),
        status="STARTING",
        reason="start",
        crash_loop_window_s=300,
        crash_loop_threshold=3,
    )
    third = _record_circuit_event(
        state_path=str(state),
        events_path=str(events),
        status="STARTING",
        reason="start",
        crash_loop_window_s=300,
        crash_loop_threshold=3,
    )

    assert first["status"] == "STARTING"
    assert second["status"] == "STARTING"
    assert third["status"] == "INCIDENT_RTDS_FEED_CRASH_LOOP"
    assert third["recent_process_starts"] == 3
    assert (tmp_path / "operator_notification_discipline_state.json").exists()


def test_websocket_timeout_is_an_idle_tick_not_a_writer_exit() -> None:
    class WebSocketTimeoutException(Exception):
        pass

    assert _is_recv_timeout(WebSocketTimeoutException("Connection timed out")) is True
    assert _is_recv_timeout(TimeoutError("timed out")) is True
    assert _is_recv_timeout(ConnectionResetError("closed")) is False


def test_flowing_frame_clears_crash_loop_even_while_starts_remain(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_DEADMAN_NOTIFY", "0")
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"
    for _ in range(3):
        incident = _record_circuit_event(
            state_path=str(state),
            events_path=str(events),
            status="STARTING",
            reason="start",
            crash_loop_window_s=300,
            crash_loop_threshold=3,
        )
    recovered = _record_circuit_event(
        state_path=str(state),
        events_path=str(events),
        status="OK",
        reason="websocket frames flowing",
        crash_loop_window_s=300,
        crash_loop_threshold=3,
    )

    assert incident["status"] == "INCIDENT_RTDS_FEED_CRASH_LOOP"
    assert recovered["status"] == "OK"
    assert recovered["recent_process_starts"] == 3
