import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_agy_advisor.py"


def test_agy_advisor_dry_run_declares_mode_and_authority(tmp_path: Path) -> None:
    artifact = tmp_path / "packet.md"
    artifact.write_text("promotion packet without secrets\n")

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--mode",
            "prosecutor",
            "--task",
            "Attack this promotion packet.",
            "--artifact",
            str(artifact),
            "--dry-run",
        ],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "MODE: ADVISOR/prosecutor" in proc.stdout
    assert "AUTHORITY: advisory only" in proc.stdout
    assert str(artifact) in proc.stdout


def test_agy_advisor_rejects_secret_like_artifacts(tmp_path: Path) -> None:
    artifact = tmp_path / "packet.md"
    artifact.write_text("CLOB_API_SECRET=abcdefghijklmnopqrstuvwxyz\n")

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--mode",
            "env-sweeper",
            "--task",
            "Sweep environment changes.",
            "--artifact",
            str(artifact),
            "--dry-run",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "secret-like content" in proc.stderr


def test_agy_advisor_provider_log_tag_includes_mode(tmp_path: Path) -> None:
    fake_agy = tmp_path / "agy"
    fake_agy.write_text("#!/usr/bin/env sh\necho advisor-ok\n")
    fake_agy.chmod(0o755)
    log_dir = tmp_path / "logs"

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--mode",
            "env-sweeper",
            "--task",
            "Sweep environment changes.",
            "--agy-bin",
            str(fake_agy),
            "--log-dir",
            str(log_dir),
        ],
        text=True,
        capture_output=True,
        check=True,
    )

    logs = list(log_dir.glob("*_agy_ADVISOR_env-sweeper_rc0.log"))
    assert len(logs) == 1
    assert str(logs[0]) in proc.stdout
    assert logs[0].read_text() == "advisor-ok\n"
