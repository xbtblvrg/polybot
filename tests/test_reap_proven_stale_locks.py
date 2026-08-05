import json
import os

from scripts import reap_proven_stale_locks as reaper


def test_reap_targets_unlinks_only_allowlisted_dead_lock_while_flock_held(
    monkeypatch,
    tmp_path,
):
    target = tmp_path / "proven.lock"
    target.write_text("", encoding="utf-8")
    os.utime(target, (100.0, 100.0))
    journal = tmp_path / "events.jsonl"
    state = tmp_path / "latest.json"
    monkeypatch.setattr(reaper, "process_rows", lambda: [])
    monkeypatch.setattr(
        reaper,
        "_lock_holder",
        lambda _path, _rows: {
            "holder_pid": None,
            "holder_alive": False,
            "holder_command": None,
        },
    )

    result = reaper.reap_targets(
        targets=(target,),
        allowed_paths={target},
        journal_path=journal,
        state_path=state,
        now_ts=4000.0,
    )

    assert result["status"] == "PASS"
    assert not target.exists()
    row = json.loads(journal.read_text(encoding="utf-8"))
    assert row["status"] == "DELETED_PROVEN_STALE_LOCK"
    assert row["allowlisted"] is True
    assert row["holder_alive"] is False
    assert row["exclusive_nonblocking_flock_acquired"] is True
    assert row["unlinked_while_flock_held"] is True


def test_reap_targets_retains_non_allowlisted_path(monkeypatch, tmp_path):
    target = tmp_path / "not-allowed.lock"
    target.write_text("", encoding="utf-8")
    monkeypatch.setattr(reaper, "process_rows", lambda: [])

    result = reaper.reap_targets(
        targets=(target,),
        allowed_paths=set(),
        journal_path=tmp_path / "events.jsonl",
        state_path=tmp_path / "latest.json",
        now_ts=4000.0,
    )

    assert result["status"] == "DEFECT"
    assert target.exists()
    assert result["targets"][0]["status"] == "RETAINED_NOT_ALLOWLISTED"
