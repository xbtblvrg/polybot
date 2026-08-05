import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "codex_serving_mutex.py"


def run_mutex(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_mutex_acquire_held_release_cycle(tmp_path: Path) -> None:
    lock_dir = tmp_path / "lock"
    token_file = tmp_path / "token"

    acquired = run_mutex(
        "--lock-dir",
        str(lock_dir),
        "acquire",
        "--owner",
        "test",
        "--holder-pid",
        str(os.getpid()),
        "--token-file",
        str(token_file),
    )
    assert acquired.returncode == 0
    assert json.loads(acquired.stdout)["status"] == "ACQUIRED"
    assert token_file.read_text().strip()

    held = run_mutex("--lock-dir", str(lock_dir), "acquire", "--owner", "second")
    assert held.returncode == 75
    assert json.loads(held.stdout)["status"] == "HELD"

    released = run_mutex("--lock-dir", str(lock_dir), "release", "--token-file", str(token_file))
    assert released.returncode == 0
    assert json.loads(released.stdout)["status"] == "RELEASED"
    assert not lock_dir.exists()


def test_mutex_replaces_stale_lock(tmp_path: Path) -> None:
    lock_dir = tmp_path / "lock"
    token_file = tmp_path / "token"
    lock_dir.mkdir()
    (lock_dir / "metadata.json").write_text(
        json.dumps(
            {
                "owner": "stale",
                "token": "old",
                "acquired_at": "2000-01-01T00:00:00Z",
                "expires_after_s": 1,
            }
        ),
        encoding="utf-8",
    )

    acquired = run_mutex(
        "--lock-dir",
        str(lock_dir),
        "acquire",
        "--owner",
        "fresh",
        "--token-file",
        str(token_file),
        "--ttl-s",
        "1",
    )
    assert acquired.returncode == 0
    metadata = json.loads((lock_dir / "metadata.json").read_text())
    assert metadata["owner"] == "fresh"
    assert token_file.read_text().strip() == metadata["token"]


def test_mutex_records_holder_pid(tmp_path: Path) -> None:
    lock_dir = tmp_path / "lock"
    token_file = tmp_path / "token"

    acquired = run_mutex(
        "--lock-dir",
        str(lock_dir),
        "acquire",
        "--owner",
        "holder",
        "--holder-pid",
        str(os.getpid()),
        "--token-file",
        str(token_file),
    )
    assert acquired.returncode == 0
    metadata = json.loads((lock_dir / "metadata.json").read_text())
    assert metadata["pid"] == os.getpid()
    assert metadata["acquire_pid"] != metadata["pid"]
