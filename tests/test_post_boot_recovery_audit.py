import json
import os
from pathlib import Path
from types import SimpleNamespace

import scripts.post_boot_recovery_audit as audit


def _seed_rtds(monkeypatch, tmp_path: Path, *, inode: int, offset: int, state_size: int) -> None:
    capture = tmp_path / "capture.jsonl"
    capture.write_text("seed\n", encoding="utf-8")
    offset_path = tmp_path / "rtds_offset.json"
    offset_path.write_text(
        json.dumps({"inode": inode, "offset": offset, "size": state_size}),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "RTDS_CAPTURE", capture)
    monkeypatch.setattr(audit, "RTDS_OFFSET", offset_path)


def test_stale_remnants_excludes_lock_with_live_default_holder(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "fee_aware_long_horizon_copy_paper.lock"
    lock.touch()
    os.utime(lock, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)

    result = audit.stale_remnants(
        processes=[
            {"pid": 34725, "command": "python scripts/run_fee_aware_long_horizon_copy_paper_lane.py"}
        ],
        now_ts=5001,
    )

    assert result["stale_locks"] == []
    assert result["held_locks"][0]["path"] == "data/research/fee_aware_long_horizon_copy_paper.lock"
    assert result["held_locks"][0]["holder_pid"] == 34725
    assert result["held_locks"][0]["holder_alive"] is True


def test_stale_remnants_flags_lock_without_live_holder(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "fee_aware_long_horizon_copy_paper.lock"
    lock.touch()
    os.utime(lock, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)

    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_locks"] == ["data/research/fee_aware_long_horizon_copy_paper.lock"]
    assert result["held_locks"] == []


def test_stale_remnants_deduplicates_hidden_lock_globs(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / ".shared_state.json.lock"
    lock.touch()
    os.utime(lock, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)

    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_locks"] == ["data/research/.shared_state.json.lock"]


def test_stale_remnants_ignores_tmp_removed_after_glob(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    transient = data / ".state.json.race.tmp"
    transient.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)
    original_stat = Path.stat

    def racing_stat(path: Path, *args, **kwargs):
        if path == transient:
            transient.unlink(missing_ok=True)
            raise FileNotFoundError(transient)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", racing_stat)
    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_tmp"] == []
    assert result["stale_tmp_evidence"] == []


def test_stale_lock_retouch_is_cyclic_active_until_mtime_stops(
    monkeypatch, tmp_path: Path
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "cyclic_writer.jsonl.lock"
    lock.touch()
    os.utime(lock, (2000, 2000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)

    retouched = audit.stale_remnants(
        processes=[],
        now_ts=6001,
        previous_lock_mtimes={"data/research/cyclic_writer.jsonl.lock": 1000.0},
    )
    unchanged = audit.stale_remnants(
        processes=[],
        now_ts=6001,
        previous_lock_mtimes={"data/research/cyclic_writer.jsonl.lock": 2000.0},
    )

    assert retouched["stale_locks"] == []
    assert retouched["cyclic_active_locks"][0]["classification"] == "CYCLIC_ACTIVE"
    assert unchanged["stale_locks"] == [
        "data/research/cyclic_writer.jsonl.lock"
    ]


def test_stale_remnants_keeps_kernel_held_lock(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "shared_state.json.lock"
    lock.touch()
    os.utime(lock, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)
    monkeypatch.setattr(
        audit,
        "_nonblocking_flock_probe",
        lambda _path: {
            "flock_probe_ok": True,
            "flock_acquired": False,
            "kernel_holder_present": True,
            "flock_error": None,
        },
    )

    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_locks"] == []
    assert result["held_locks"][0]["kernel_holder_present"] is True


def test_stale_tmp_requires_clean_lsof_probe(monkeypatch, tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    tmp = data / ".state.json.abc.tmp"
    tmp.touch()
    os.utime(tmp, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)
    monkeypatch.setattr(
        audit,
        "_lsof_open_handle",
        lambda _path: {
            "lsof_probe_ok": True,
            "open_handle_present": True,
            "open_handle_pids": [123],
            "lsof_error": None,
        },
    )

    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_tmp"] == []
    assert result["held_tmp"][0]["open_handle_pids"] == [123]


def test_sweep_stale_remnants_refuses_while_guard_is_live(monkeypatch) -> None:
    monkeypatch.setattr(
        audit,
        "guard_processes",
        lambda: [{"pid": 123, "command": "python scripts/run_wallet_copy_live_guard.py"}],
    )

    result = audit.sweep_stale_remnants()

    assert result["status"] == "REFUSED_GUARD_NOT_QUIESCENT"
    assert result["deleted"] == []


def test_sweep_stale_remnants_deletes_only_allowlisted_reproven_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "stale.jsonl.lock"
    other = data / "other.jsonl.lock"
    tmp = data / ".state.json.abc.tmp"
    for path in (lock, other, tmp):
        path.touch()
        os.utime(path, (1000, 1000))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)
    monkeypatch.setattr(audit, "guard_processes", lambda: [])
    monkeypatch.setattr(audit, "process_rows", lambda: [])
    monkeypatch.setattr(
        audit,
        "_lsof_open_handle",
        lambda _path: {
            "lsof_probe_ok": True,
            "open_handle_present": False,
            "open_handle_pids": [],
            "lsof_error": None,
        },
    )

    result = audit.sweep_stale_remnants(
        allowed_paths={lock, tmp},
        now_ts=5001,
    )

    assert result["status"] == "PARTIAL_RETAINED"
    assert not lock.exists()
    assert not tmp.exists()
    assert other.exists()
    assert {row["sweep_status"] for row in result["deleted"]} == {
        "DELETED_PROVEN_STALE_LOCK",
        "DELETED_PROVEN_STALE_TMP",
    }
    assert result["retained"][0]["sweep_status"] == "RETAINED_NOT_ALLOWLISTED"


def test_stale_remnants_excludes_restart_lock_matching_collector_start(
    monkeypatch, tmp_path: Path
) -> None:
    data = tmp_path / "data" / "research"
    data.mkdir(parents=True)
    lock = data / "fee_aware_long_horizon_copy_paper.lock"
    lock.touch()
    os.utime(lock, (1000, 1000))
    state = data / "fee_aware_long_horizon_copy_paper_latest.json"
    state.write_text(json.dumps({"collector_start_ts": 1000.5}))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "DATA", data)
    monkeypatch.setattr(
        audit,
        "DEFAULT_LOCK_COLLECTOR_STATES",
        {lock.name: (state.name,)},
    )

    result = audit.stale_remnants(processes=[], now_ts=5001)

    assert result["stale_locks"] == []
    evidence = result["held_locks"][0]
    assert evidence["holder_alive"] is False
    assert evidence["collector_start_matches_lock_mtime"] is True
    assert evidence["collector_start_mtime_delta_s"] == 0.5


def test_rtds_audit_recovers_unchanged_inode_growing_capture_race(monkeypatch, tmp_path: Path) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=120, state_size=120)
    stats = iter(
        [
            SimpleNamespace(st_ino=87861304, st_size=100),
            SimpleNamespace(st_ino=87861304, st_size=150),
        ]
    )
    monkeypatch.setattr(audit, "_capture_stat", lambda _path: next(stats))

    result = audit.rtds_audit()

    assert result["ok"] is True
    assert result["restat_recovered"] is True
    assert result["initial_capture_size"] == 100
    assert result["capture_size"] == 150
    assert result["race_classification"] == "KNOWN_BENIGN_GROWING_CAPTURE_RACE"


def test_rtds_audit_waits_for_delayed_growing_capture_race(monkeypatch, tmp_path: Path) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=120, state_size=120)
    stats = iter(
        [
            SimpleNamespace(st_ino=87861304, st_size=100),
            SimpleNamespace(st_ino=87861304, st_size=110),
            SimpleNamespace(st_ino=87861304, st_size=150),
        ]
    )
    monkeypatch.setattr(audit, "_capture_stat", lambda _path: next(stats))
    monkeypatch.setattr(audit.time, "sleep", lambda _seconds: None)

    result = audit.rtds_audit()

    assert result["ok"] is True
    assert result["restat_recovered"] is True
    assert result["initial_capture_size"] == 100
    assert result["restat_capture_size"] == 150
    assert result["capture_size"] == 150
    assert result["race_classification"] == "KNOWN_BENIGN_GROWING_CAPTURE_RACE"


def test_rtds_audit_accepts_state_size_ahead_when_offset_is_readable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=90, state_size=120)
    stats = iter(
        [
            SimpleNamespace(st_ino=87861304, st_size=100),
            SimpleNamespace(st_ino=87861304, st_size=100),
        ]
    )
    monkeypatch.setattr(audit, "_capture_stat", lambda _path: next(stats))

    result = audit.rtds_audit()

    assert result["ok"] is True
    assert result["state_size_consistent"] is False
    assert result["race_classification"] == "KNOWN_BENIGN_STATE_SIZE_AHEAD_CAPTURE_RACE"
    assert result["restat_capture_size"] == 100


def test_rtds_audit_still_fails_when_restat_does_not_cover_offset(monkeypatch, tmp_path: Path) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=120, state_size=120)
    stats = iter(
        [
            SimpleNamespace(st_ino=87861304, st_size=100),
            SimpleNamespace(st_ino=87861304, st_size=110),
        ]
    )
    monkeypatch.setattr(audit, "_capture_stat", lambda _path: next(stats))

    result = audit.rtds_audit()

    assert result["ok"] is False
    assert result["restat_recovered"] is False
    assert result["capture_size"] == 100
    assert result["restat_capture_size"] == 110


def test_rtds_audit_accepts_fresh_rotation_after_old_file_fully_consumed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=120, state_size=120)
    monkeypatch.setattr(
        audit,
        "_capture_stat",
        lambda _path: SimpleNamespace(st_ino=99999123, st_size=50, st_mtime=4900),
    )

    result = audit.rtds_audit(now_ts=5000)

    assert result["ok"] is True
    assert result["rotation_fully_consumed"] is True
    assert result["current_capture_fresh"] is True
    assert (
        result["race_classification"]
        == "EXPECTED_ROTATION_FULLY_CONSUMED_PENDING_MIDNIGHT_ADOPTION"
    )


def test_rtds_audit_fails_closed_on_unconsumed_rotated_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=110, state_size=120)
    monkeypatch.setattr(
        audit,
        "_capture_stat",
        lambda _path: SimpleNamespace(st_ino=99999123, st_size=50, st_mtime=4900),
    )

    result = audit.rtds_audit(now_ts=5000)

    assert result["ok"] is False
    assert result["rotation_fully_consumed"] is False
    assert result["race_classification"] is None


def test_rtds_audit_accepts_stale_but_growing_rotated_capture(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_rtds(monkeypatch, tmp_path, inode=87861304, offset=120, state_size=120)
    stats = iter(
        [
            SimpleNamespace(st_ino=99999123, st_size=50, st_mtime=1000),
            SimpleNamespace(st_ino=99999123, st_size=75, st_mtime=1000),
        ]
    )
    monkeypatch.setattr(audit, "_capture_stat", lambda _path: next(stats))
    monkeypatch.setattr(audit.time, "sleep", lambda _seconds: None)

    result = audit.rtds_audit(now_ts=5000)

    assert result["ok"] is True
    assert result["rotation_growing"] is True
    assert (
        result["race_classification"]
        == "EXPECTED_ROTATION_FULLY_CONSUMED_PENDING_MIDNIGHT_ADOPTION"
    )
