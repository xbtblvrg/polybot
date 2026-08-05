from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path

import scripts.research_disk_deadman as disk_deadman


def test_copytruncate_tail_preserves_inode_and_line_aligned_tail(tmp_path: Path) -> None:
    capture = tmp_path / "capture.jsonl"
    capture.write_bytes(b'partial\n{"n": 1}\n{"n": 2}\n{"n": 3}\n')
    inode_before = capture.stat().st_ino

    result = disk_deadman.copytruncate_tail(
        path=capture,
        keep_tail_bytes=20,
        state_path=tmp_path / "state.json",
        event_log_path=tmp_path / "events.jsonl",
        reason="test",
    )

    assert result["status"] == "REPAIRED"
    assert result["inode_preserved"] is True
    assert capture.stat().st_ino == inode_before
    assert capture.read_text(encoding="utf-8").splitlines() == ['{"n": 2}', '{"n": 3}']
    assert result["tail_line_aligned"] is True
    assert "copytruncate_line_aligned_tail" in (tmp_path / "events.jsonl").read_text(encoding="utf-8")


def test_audit_disk_flags_uninventoried_large_research_files(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    large = data / "uncapped.jsonl"
    large.write_bytes(b"x" * 64)
    inventory = data / "research_capture_rotation_inventory.json"
    inventory.write_text(json.dumps({"entries": []}), encoding="utf-8")
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)

    result = disk_deadman.audit_disk(
        inventory_path=inventory,
        state_path=data / "state.json",
        event_log_path=data / "events.jsonl",
        free_incident_bytes=0,
        uninventoried_threshold_bytes=10,
    )

    assert result["status"] == "INCIDENT_RESEARCH_DISK"
    assert result["incident"] is True
    assert result["incident_keys"] == ["uninventoried_large_research_files"]
    assert result["uninventoried_large_files"][0]["path"] == "data/research/uncapped.jsonl"


def test_audit_disk_passes_when_large_file_is_in_inventory(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    large = data / "capped.jsonl"
    large.write_bytes(b"x" * 64)
    inventory = data / "research_capture_rotation_inventory.json"
    inventory.write_text(
        json.dumps({"entries": [{"path": "data/research/capped.jsonl", "cap_bytes": 64}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)

    result = disk_deadman.audit_disk(
        inventory_path=inventory,
        state_path=data / "state.json",
        event_log_path=data / "events.jsonl",
        free_incident_bytes=0,
        uninventoried_threshold_bytes=10,
    )

    assert result["status"] == "OK"
    assert result["incident"] is False
    assert result["uninventoried_large_files"] == []


def test_enforce_inventory_can_archive_reseed_structured_jsonl(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    capture = data / "events.jsonl"
    source = b'partial\n{"event": 1}\n{"event": 2}\n{"event": 3}\n'
    capture.write_bytes(source)
    inventory = data / "research_capture_rotation_inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "path": "data/research/events.jsonl",
                        "cap_bytes": 20,
                        "keep_tail_bytes": 28,
                        "rotation_action": "structured_jsonl_archive_reseed",
                        "archive_dir": str(data / "log_archives"),
                        "state_path": str(data / "rotation_state.json"),
                        "event_log_path": str(data / "rotation_events.jsonl"),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)

    result = disk_deadman.enforce_inventory(
        inventory_path=inventory,
        state_path=data / "state.json",
        event_log_path=data / "events_deadman.jsonl",
    )

    assert result["status"] == "REPAIRED"
    assert result["repaired_count"] == 1
    action = result["actions"][0]
    assert action["action"] == "rename_archive_reseed_tail"
    assert Path(action["archive_path"]).read_bytes() == source
    assert capture.read_text(encoding="utf-8").splitlines() == ['{"event": 2}', '{"event": 3}']
    assert "rename_archive_reseed_tail" in (data / "rotation_events.jsonl").read_text(encoding="utf-8")


def test_enforce_inventory_can_rotate_by_segment_age(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    data.mkdir(parents=True)
    capture = data / "events.jsonl"
    source = b'{"event": 1}\n{"event": 2}\n'
    capture.write_bytes(source)
    inventory = data / "research_capture_rotation_inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "path": "data/research/events.jsonl",
                        "cap_bytes": 10_000,
                        "keep_tail_bytes": 64,
                        "max_segment_age_s": 1,
                        "rotation_action": "structured_jsonl_archive_reseed",
                        "archive_dir": str(data / "log_archives"),
                        "state_path": str(data / "rotation_state.json"),
                        "event_log_path": str(data / "rotation_events.jsonl"),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)
    file_ctime = capture.stat().st_ctime

    class FutureDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromtimestamp(file_ctime + 10, tz=tz or timezone.utc)

    monkeypatch.setattr(disk_deadman, "datetime", FutureDateTime)

    result = disk_deadman.enforce_inventory(
        inventory_path=inventory,
        state_path=data / "state.json",
        event_log_path=data / "events_deadman.jsonl",
    )

    assert result["status"] == "REPAIRED"
    action = result["actions"][0]
    assert action["rotation_trigger"] == "age"
    assert action["max_segment_age_s"] == 1
    assert action["action"] == "rename_archive_reseed_tail"


def test_compress_log_archives_gzips_old_jsonl_in_place(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    archive_dir = data / "log_archives"
    archive_dir.mkdir(parents=True)
    raw = archive_dir / "polymarket_activity_ws_capture_archived.jsonl"
    raw.write_bytes(b'{"event": 1}\n{"event": 2}\n')
    old_time = raw.stat().st_mtime - 7200
    os.utime(raw, (old_time, old_time))
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)

    result = disk_deadman.compress_log_archives(
        archive_dir=archive_dir,
        state_path=data / "state.json",
        event_log_path=data / "events.jsonl",
        older_than_s=3600,
    )

    gz_path = archive_dir / "polymarket_activity_ws_capture_archived.jsonl.gz"
    assert result["status"] == "REPAIRED"
    assert result["compressed_count"] == 1
    assert not raw.exists()
    assert gz_path.exists()
    with gzip.open(gz_path, "rb") as handle:
        assert handle.read() == b'{"event": 1}\n{"event": 2}\n'


def test_compress_log_archives_enforces_rtds_retention_oldest_first(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    data = root / "data" / "research"
    archive_dir = data / "log_archives"
    archive_dir.mkdir(parents=True)
    newest = archive_dir / "polymarket_activity_ws_capture_new.jsonl.gz"
    oldest = archive_dir / "polymarket_activity_ws_capture_old.jsonl.gz"
    guard = archive_dir / "wallet_copy_live_guard_events_old.jsonl.gz"
    for path, payload in (
        (oldest, b"old" * 100),
        (newest, b"new" * 100),
        (guard, b"guard" * 100),
    ):
        with gzip.open(path, "wb") as handle:
            handle.write(payload)
    os.utime(oldest, (1000, 1000))
    os.utime(newest, (2000, 2000))
    os.utime(guard, (500, 500))
    monkeypatch.setattr(disk_deadman, "ROOT", root)
    monkeypatch.setattr(disk_deadman, "DATA", data)

    cap = newest.stat().st_size + 1
    result = disk_deadman.compress_log_archives(
        archive_dir=archive_dir,
        state_path=data / "state.json",
        event_log_path=data / "events.jsonl",
        older_than_s=3600,
        rtds_retention_cap_bytes=cap,
    )

    assert result["status"] == "REPAIRED"
    assert result["retention_deleted_count"] == 1
    assert not oldest.exists()
    assert newest.exists()
    assert guard.exists()


def test_build_memory_swap_vitals_incidents_on_sustained_swapfile_count() -> None:
    result = disk_deadman.build_memory_swap_vitals(
        previous_state={"memory_swap": {"swapfiles": {"above_threshold": True}}},
        swapusage_text="vm.swapusage: total = 4096.00M  used = 2857.50M  free = 1238.50M  (encrypted)",
        swapusage_ok=True,
        memory_pressure_text="System-wide memory free percentage: 50%",
        memory_pressure_ok=True,
        swapfile_count=21,
        swapfile_incident_count=20,
    )

    assert result["status"] == "INCIDENT_MEMORY_SWAP"
    assert result["incident_keys"] == ["swapfile_count_sustained_gt_threshold"]
    assert result["swapusage"]["used_mib"] == 2857.5


def test_build_memory_swap_vitals_watches_first_high_swapfile_count() -> None:
    result = disk_deadman.build_memory_swap_vitals(
        previous_state={},
        swapusage_text="vm.swapusage: total = 4.00G  used = 1.00G  free = 3.00G",
        swapusage_ok=True,
        memory_pressure_text="System-wide memory free percentage: 50%",
        memory_pressure_ok=True,
        swapfile_count=21,
        swapfile_incident_count=20,
    )

    assert result["status"] == "WATCH_MEMORY_SWAP"
    assert result["incident"] is False


def test_build_memory_swap_vitals_incidents_on_critical_pressure() -> None:
    result = disk_deadman.build_memory_swap_vitals(
        previous_state={},
        swapusage_text="",
        swapusage_ok=False,
        memory_pressure_text="Compressor Info: 36% of compressed pages limit and 100% of segments limit",
        memory_pressure_ok=True,
        swapfile_count=1,
    )

    assert result["status"] == "INCIDENT_MEMORY_SWAP"
    assert result["incident_keys"] == ["memory_pressure_critical"]
    assert result["memory_pressure"]["compressor_segment_limit_pct"] == 100
