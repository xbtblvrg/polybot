import json
from argparse import Namespace
from pathlib import Path

import pytest

from scripts.capture_live_golden_config import build_snapshot, write_snapshot
from scripts.plan_live_canary_rollout import build_plan
from scripts.record_live_change_journal import build_entry
from scripts.restore_live_golden_config import restore


def test_golden_config_snapshot_excludes_secretish_files_and_restores(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "configs/wallet_copy").mkdir(parents=True)
    config = tmp_path / "configs/wallet_copy/wallets.json"
    config.write_text('{"wallets": []}\n')
    (tmp_path / "configs/wallet_copy/private_key.json").write_text('{"bad": true}\n')
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/run_wallet_copy_live_guard.py").write_text("print('guard')\n")

    monkeypatch.setattr("scripts.capture_live_golden_config._git_head", lambda root: "abc123")
    monkeypatch.setattr("scripts.capture_live_golden_config._git_status", lambda root: [])
    monkeypatch.setattr("scripts.capture_live_golden_config._guard_processes", lambda root: ["123 guard"])

    snapshot = build_snapshot(tmp_path, note="unit")
    assert "configs/wallet_copy/wallets.json" in snapshot["config_files"]
    assert "configs/wallet_copy/private_key.json" not in snapshot["config_files"]
    assert snapshot["skipped"] == [{"path": "configs/wallet_copy/private_key.json", "reason": "secret_like_name"}]

    path = write_snapshot(tmp_path, snapshot, "data/research")
    config.write_text('{"wallets": ["changed"]}\n')
    dry = restore(path, tmp_path, dry_run=True, confirm=False)
    assert dry["status"] == "DRY_RUN"
    assert "changed" in config.read_text()
    restored = restore(path, tmp_path, dry_run=False, confirm=True)
    assert restored["status"] == "RESTORED"
    assert config.read_text() == '{"wallets": []}\n'


def test_golden_config_snapshot_stores_large_files_as_assets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "configs/wallet_copy").mkdir(parents=True)
    config = tmp_path / "configs/wallet_copy/wallets.json"
    config.write_text("x" * 1_000_001)

    monkeypatch.setattr("scripts.capture_live_golden_config._git_head", lambda root: "abc123")
    monkeypatch.setattr("scripts.capture_live_golden_config._git_status", lambda root: [])
    monkeypatch.setattr("scripts.capture_live_golden_config._guard_processes", lambda root: [])

    snapshot = build_snapshot(tmp_path)
    assert snapshot["config_files"]["configs/wallet_copy/wallets.json"]["inline"] is False
    path = write_snapshot(tmp_path, snapshot, "data/research")
    written = json.loads(path.read_text())
    meta = written["config_files"]["configs/wallet_copy/wallets.json"]
    assert "text" not in meta
    assert meta["content_asset"].endswith("configs/wallet_copy/wallets.json.gz")

    config.write_text("changed")
    restore(path, tmp_path, dry_run=False, confirm=True)
    assert config.read_text() == "x" * 1_000_001


def test_live_change_journal_requires_enemy_or_defect() -> None:
    args = Namespace(
        change_id="chg-1",
        mode="canary",
        enemy_id="CAMPAIGN-LAT",
        defect_id="",
        justification="CAMPAIGN-LAT attacks latency",
        expected_effect="+10 windows/day",
        diff_or_commit_ref="",
        golden_snapshot="golden.json",
        rollback_command="restore --dry-run",
        touched_path=["configs/wallet_copy/wallets.json"],
        measured_outcome="",
    )
    entry = build_entry(args)
    assert entry["enemy_id"] == "CAMPAIGN-LAT"
    args.enemy_id = ""
    with pytest.raises(ValueError, match="enemy-id or --defect-id"):
        build_entry(args)


def test_live_change_journal_appends_without_truncating(tmp_path: Path) -> None:
    from scripts.record_live_change_journal import main

    journal = tmp_path / "journal.jsonl"
    base = [
        "--journal", str(journal),
        "--mode", "staged",
        "--defect-id", "d-1",
        "--justification", "j",
        "--expected-effect", "e",
        "--golden-snapshot", "golden.json",
        "--rollback-command", "restore --dry-run",
    ]
    assert main(["--change-id", "chg-a", *base]) == 0
    first = journal.read_text()
    assert main(["--change-id", "chg-b", *base]) == 0
    rows = [json.loads(line) for line in journal.read_text().splitlines()]
    assert [r["change_id"] for r in rows] == ["chg-a", "chg-b"]
    assert journal.read_text().startswith(first)


def test_canary_rollout_plan_is_plan_only_and_has_revert_gate() -> None:
    args = Namespace(
        out="unused.json",
        change_id="chg-2",
        golden_snapshot="data/research/golden_config_latest.json",
        enemy_id="CAMPAIGN-BREADTH",
        defect_id="",
        justification="CAMPAIGN-BREADTH attacks concentration",
        expected_effect="top producer share below 70%",
        slice_type="member",
        slice_value="0xe6db",
        pass_metric="canary_post_fee_pnl_usd",
        pass_threshold=">0 over n>=20",
        auto_revert_metric="rolling20_pnl_usd",
        auto_revert_threshold="< -16",
        rollback_command="restore --dry-run",
    )
    plan = build_plan(args)
    assert plan["live_mutation"] is False
    assert plan["stages"][0]["name"] == "canary"
    assert plan["auto_revert"]["rollback_command"] == "restore --dry-run"
    json.dumps(plan)
