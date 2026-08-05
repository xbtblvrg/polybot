from __future__ import annotations

import json
from pathlib import Path

from scripts.rotate_structured_jsonl_log import rotate_structured_jsonl


def test_rotate_structured_jsonl_archives_full_log_and_reseeds_line_aligned_tail(tmp_path: Path) -> None:
    log = tmp_path / "events.jsonl"
    source = b'partial-line\n{"event": 1}\n{"event": 2}\n{"event": 3}\n'
    log.write_bytes(source)

    result = rotate_structured_jsonl(
        log_path=log,
        state_path=tmp_path / "state.json",
        event_log_path=tmp_path / "rotation_events.jsonl",
        archive_dir=tmp_path / "archives",
        max_bytes=20,
        keep_tail_bytes=28,
    )

    assert result["status"] == "REPAIRED"
    archive = Path(result["archive_path"])
    assert archive.read_bytes() == source
    retained = log.read_text(encoding="utf-8").splitlines()
    assert retained == ['{"event": 2}', '{"event": 3}']
    assert result["tail_line_aligned"] is True
    assert result["archive_size_bytes"] == len(source)

    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["archive_path"] == str(archive)
    assert "rename_archive_reseed_tail" in (tmp_path / "rotation_events.jsonl").read_text(encoding="utf-8")


def test_rotate_structured_jsonl_passes_below_cap_without_archive(tmp_path: Path) -> None:
    log = tmp_path / "events.jsonl"
    log.write_text('{"event": 1}\n', encoding="utf-8")

    result = rotate_structured_jsonl(
        log_path=log,
        state_path=tmp_path / "state.json",
        event_log_path=tmp_path / "rotation_events.jsonl",
        archive_dir=tmp_path / "archives",
        max_bytes=1024,
        keep_tail_bytes=128,
    )

    assert result["status"] == "PASS"
    assert result["reason"] == "below_cap"
    assert not (tmp_path / "archives").exists()
    assert log.read_text(encoding="utf-8") == '{"event": 1}\n'
