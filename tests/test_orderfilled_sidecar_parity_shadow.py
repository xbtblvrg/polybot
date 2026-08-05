import argparse
import json
from pathlib import Path

from scripts import run_orderfilled_sidecar_parity_shadow as shadow


def _append(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def test_sidecar_shadow_requires_exact_payload_and_measures_guard_lead(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        shadow.resource,
        "getrusage",
        lambda _scope: type("Usage", (), {"ru_maxrss": 1024})(),
    )
    mixed = tmp_path / "mixed.jsonl"
    sidecar = tmp_path / "sidecar.jsonl"
    cursor = tmp_path / "cursor.json"
    state = tmp_path / "state.json"
    mixed.touch()
    sidecar.touch()
    cursor.write_text(json.dumps({"next_byte_offset": 0}), encoding="utf-8")
    args = argparse.Namespace(
        mixed_jsonl=str(mixed),
        sidecar_jsonl=str(sidecar),
        guard_cursor_state=str(cursor),
        state=str(state),
        required_identities=1,
        required_p95_lead_s=5.0,
        loss_grace_s=60.0,
        max_records=100,
    )
    shadow.run_once(args, now_s=100.0)
    row = {
        "event": "polygon_orderfilled_log",
        "transaction_hash": "0xabc",
        "log_index": 7,
        "received_at_s": 101.0,
        "decoded": {"price": 0.38},
    }
    _append(sidecar, row)
    shadow.run_once(args, now_s=102.0)
    _append(mixed, row)
    cursor.write_text(json.dumps({"next_byte_offset": mixed.stat().st_size}), encoding="utf-8")
    report = shadow.run_once(args, now_s=108.0)
    assert report["unique_exact_payload_matched_identities"] == 1
    assert report["p95_sidecar_lead_vs_mixed_guard_s"] == 7.0
    assert report["overdue_sidecar_only_payloads"] == 0
    assert report["overdue_mixed_only_payloads"] == 0
    assert report["live_source_wiring_gate_passed"] is True
    assert report["gate_passed_once"] is True

    _append(sidecar, {**row, "transaction_hash": "0xdef", "log_index": 8})
    report = shadow.run_once(args, now_s=200.0)
    assert report["current_window_gate_passed"] is False
    assert report["live_source_wiring_gate_passed"] is True
    assert report["status"] == "PASS_LATCHED"


def test_sidecar_shadow_does_not_match_changed_payload(tmp_path: Path) -> None:
    mixed = tmp_path / "mixed.jsonl"
    sidecar = tmp_path / "sidecar.jsonl"
    cursor = tmp_path / "cursor.json"
    state = tmp_path / "state.json"
    mixed.touch()
    sidecar.touch()
    cursor.write_text(json.dumps({"next_byte_offset": 0}), encoding="utf-8")
    args = argparse.Namespace(
        mixed_jsonl=str(mixed),
        sidecar_jsonl=str(sidecar),
        guard_cursor_state=str(cursor),
        state=str(state),
        required_identities=1,
        required_p95_lead_s=5.0,
        loss_grace_s=1.0,
        max_records=100,
    )
    shadow.run_once(args, now_s=100.0)
    base = {
        "event": "polygon_orderfilled_log",
        "transaction_hash": "0xabc",
        "log_index": 7,
        "received_at_s": 101.0,
    }
    _append(sidecar, {**base, "decoded": {"price": 0.38}})
    shadow.run_once(args, now_s=102.0)
    _append(mixed, {**base, "decoded": {"price": 0.39}})
    cursor.write_text(json.dumps({"next_byte_offset": mixed.stat().st_size}), encoding="utf-8")
    shadow.run_once(args, now_s=104.0)
    report = shadow.run_once(args, now_s=106.0)
    assert report["unique_exact_payload_matched_identities"] == 0
    assert report["overdue_sidecar_only_payloads"] == 1
    assert report["overdue_mixed_only_payloads"] == 1
    assert report["live_source_wiring_gate_passed"] is False


def test_live_cursor_handoff_starts_before_newest_known_identity(tmp_path: Path) -> None:
    sidecar = tmp_path / "sidecar.jsonl"
    old_cursor = tmp_path / "mixed-cursor.json"
    new_cursor = tmp_path / "sidecar-cursor.json"
    older = {
        "event": "polygon_orderfilled_log",
        "transaction_hash": "0xolder",
        "log_index": 1,
    }
    newest = {
        "event": "polygon_orderfilled_log",
        "transaction_hash": "0xnewest",
        "log_index": 2,
    }
    _append(sidecar, older)
    expected_offset = sidecar.stat().st_size
    _append(sidecar, newest)
    _append(sidecar, newest)
    old_cursor.write_text(
        json.dumps(
            {
                "next_byte_offset": 123456,
                "seen_identities": ["0xolder|1", "0xnewest|2"],
            }
        ),
        encoding="utf-8",
    )
    report = shadow.initialize_live_cursor_handoff(
        mixed_cursor_state=old_cursor,
        sidecar_jsonl=sidecar,
        sidecar_cursor_state=new_cursor,
    )
    assert report["boundary_identity"] == "0xnewest|2"
    assert report["next_byte_offset"] == expected_offset
    assert report["boundary_identity_loss"] == 0
    assert report["safe_replay_expected"] is True
    assert json.loads(new_cursor.read_text(encoding="utf-8")) == report
