from __future__ import annotations

import argparse
import fcntl
import json

from scripts.capture_dataapi_wallet_events import _poll_command
from scripts import run_same_window_research_capture as capture
from scripts.run_same_window_research_capture import _capture_gates, dirty_capture_paths, selected_wallets


def test_same_window_selection_unions_ranked_queue_top10_and_exact_policy_packets() -> None:
    queue = {"ranked_queue": [{"wallet": "0x" + "1" * 40}, {"wallet": "0x" + "2" * 40}]}
    lane = {"ranked_wallets": [{"wallet": "0x" + "2" * 40}, {"wallet": "0x" + "3" * 40}]}
    packets = {"packets": [{"wallet": "0x" + "3" * 40}, {"wallet": "0x" + "4" * 40}]}

    assert selected_wallets(queue, lane, packets) == [
        "0x" + "1" * 40,
        "0x" + "2" * 40,
        "0x" + "3" * 40,
        "0x" + "4" * 40,
    ]


def test_dataapi_capture_uses_isolated_research_paths() -> None:
    args = argparse.Namespace(
        python="python3",
        history_state="out/history.json",
        history_window_index="out/index.json",
        wallet_event_log="out/events.jsonl",
        dataapi_first_seen_jsonl="out/first_seen.jsonl",
        observation_watermark_state="out/watermarks.json",
        poll_state="out/poll.json",
        limit=500,
        pages=2,
        timeout_s=2.0,
        retries=1,
        max_workers=8,
        poll_interval_s=15.0,
        duration_s=7500.0,
    )

    command = _poll_command(args, ["0x" + "1" * 40])

    assert command[:2] == ["python3", "scripts/merge_dataapi_active_set_events.py"]
    assert command[command.index("--history-state") + 1] == "out/history.json"
    assert command[command.index("--wallet-event-log") + 1] == "out/events.jsonl"
    assert "data/research/wallet_copy_history_state.json" not in command


def test_capture_gates_read_nested_overlap() -> None:
    gates = _capture_gates(
        {"capture_windows": {"overlap_s": 7430.0}, "fills_with_any_book_coverage": 6096},
        {"coverage": {"windows": 17}},
        {"source": {"rows_scanned": 8184}},
    )

    assert all(gates.values())


def test_dirty_capture_paths_find_state_and_jsonl_only(tmp_path) -> None:
    (tmp_path / "launchd.plist").write_text("plist", encoding="utf-8")
    (tmp_path / "launchd.out.log").write_text("log", encoding="utf-8")

    assert dirty_capture_paths(tmp_path) == []

    state = tmp_path / "same_window_capture_state.json"
    stream = tmp_path / "polygon_orderfilled.jsonl"
    state.write_text("{}", encoding="utf-8")
    stream.write_text("{}\n", encoding="utf-8")

    assert dirty_capture_paths(tmp_path) == sorted([str(state), str(stream)])


def test_orchestrator_refuses_dirty_run_as_terminal_success(tmp_path, monkeypatch, capsys) -> None:
    run_id = "20260719T162300Z"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    (run_dir / "same_window_capture_state.json").write_text("{}", encoding="utf-8")
    args = argparse.Namespace(
        run_id=run_id,
        output_dir=str(tmp_path),
        lock_file=str(tmp_path / ".capture.lock"),
        allow_dirty_run_dir=False,
    )
    monkeypatch.setattr(capture, "parse_args", lambda: args)

    assert capture.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "DIRTY_RUN_DIR_REFUSED"


def test_orchestrator_held_lock_is_terminal_success(tmp_path, monkeypatch, capsys) -> None:
    lock_path = tmp_path / ".capture.lock"
    with lock_path.open("a+", encoding="utf-8") as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        args = argparse.Namespace(lock_file=str(lock_path))
        monkeypatch.setattr(capture, "parse_args", lambda: args)

        assert capture.main() == 0
        assert json.loads(capsys.readouterr().out)["status"] == "HELD"
