from __future__ import annotations

import json
from pathlib import Path

import scripts.audit_post_panic_integrity as integrity


def _write_jsonl(path: Path, rows: list[dict], *, torn_tail: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row) + "\n" for row in rows)
    if torn_tail:
        text += torn_tail
    path.write_text(text, encoding="utf-8")


def test_post_panic_integrity_passes_inventory_and_ledger_jsonl(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(integrity, "ROOT", tmp_path)
    capture = tmp_path / "data/research/capture.jsonl"
    ledger = tmp_path / "data/research/wallet_copy_live_execution_events.jsonl"
    _write_jsonl(capture, [{"n": 1}])
    _write_jsonl(ledger, [{"event": "wallet_copy_live_order"}])
    inventory = tmp_path / "data/research/research_capture_rotation_inventory.json"
    inventory.write_text(json.dumps({"entries": [{"path": "data/research/capture.jsonl"}]}), encoding="utf-8")

    report = integrity.build_report(
        inventory=inventory,
        ledger_jsonl=["data/research/wallet_copy_live_execution_events.jsonl"],
        output=tmp_path / "out.json",
        repair=False,
        max_tail_bytes=1024,
    )

    assert report["status"] == "PASS"
    assert report["checked_count"] == 2
    assert {row["status"] for row in report["rows"]} == {"PASS"}


def test_post_panic_integrity_repairs_only_final_torn_jsonl_row(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(integrity, "ROOT", tmp_path)
    ledger = tmp_path / "data/research/wallet_copy_live_execution_events.jsonl"
    _write_jsonl(ledger, [{"event": "ok"}, {"event": "still_ok"}], torn_tail='{"event":')
    inventory = tmp_path / "data/research/research_capture_rotation_inventory.json"
    inventory.parent.mkdir(parents=True, exist_ok=True)
    inventory.write_text(json.dumps({"entries": []}), encoding="utf-8")

    failed = integrity.build_report(
        inventory=inventory,
        ledger_jsonl=["data/research/wallet_copy_live_execution_events.jsonl"],
        output=tmp_path / "out.json",
        repair=False,
        max_tail_bytes=1024,
    )
    repaired = integrity.build_report(
        inventory=inventory,
        ledger_jsonl=["data/research/wallet_copy_live_execution_events.jsonl"],
        output=tmp_path / "out.json",
        repair=True,
        max_tail_bytes=1024,
    )

    assert failed["status"] == "FAIL"
    assert repaired["status"] == "PASS"
    assert repaired["repair_count"] == 1
    assert ledger.read_text(encoding="utf-8").endswith('{"event": "still_ok"}\n')
