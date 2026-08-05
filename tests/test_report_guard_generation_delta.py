import datetime as dt
import hashlib
import os
from pathlib import Path
import subprocess

from scripts import report_guard_generation_delta as report


def _git(root: Path, *args: str, env: dict[str, str] | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return completed.stdout.strip()


def _commit(root: Path, message: str, when: str) -> str:
    env = dict(os.environ)
    env.update({"GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when})
    _git(root, "add", "-A", env=env)
    _git(root, "commit", "-m", message, env=env)
    return _git(root, "rev-parse", "HEAD")


def _generation_sha(rows: list[tuple[str, bytes | None]]) -> str:
    digest = hashlib.sha256()
    for path, data in rows:
        digest.update(path.encode())
        digest.update(b"\0")
        digest.update((hashlib.sha256(data).hexdigest() if data is not None else "MISSING").encode())
        digest.update(b"\0")
    return digest.hexdigest()


def test_report_reconstructs_loaded_tuple_and_names_only_canonical_disk_drift(tmp_path: Path):
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    (tmp_path / "scripts").mkdir()
    source = tmp_path / report.HISTORICAL_GENERATION_SOURCE
    source.write_text(
        'from pathlib import Path\nROOT = Path(".")\n'
        'GENERATION_FILES = (ROOT / "a.txt", ROOT / "b.txt", ROOT / "missing.json")\n'
    )
    (tmp_path / "a.txt").write_text("a-v1")
    (tmp_path / "b.txt").write_text("b-v1")
    loaded_commit = _commit(tmp_path, "loaded", "2026-01-01T00:00:00Z")
    loaded_sha = _generation_sha(
        [("a.txt", b"a-v1"), ("b.txt", b"b-v1"), ("missing.json", None)]
    )

    source.write_text(
        'from pathlib import Path\nROOT = Path(".")\n'
        'GENERATION_FILES = (ROOT / "a.txt", ROOT / "b.txt")\n'
    )
    (tmp_path / "a.txt").write_text("a-v2")
    _commit(tmp_path, "disk", "2026-01-01T00:02:00Z")
    disk_rows = [
        {"path": "a.txt", "exists": True, "sha256": hashlib.sha256(b"a-v2").hexdigest()},
        {"path": "b.txt", "exists": True, "sha256": hashlib.sha256(b"b-v1").hexdigest()},
    ]
    state = {
        "loaded_generation": {
            "sha256": loaded_sha,
            "pid": 123,
            "started_at_utc": "2026-01-01T00:01:00Z",
        },
        "disk_generation": {
            "sha256": report._aggregate_generation(disk_rows),
            "files": disk_rows,
        },
    }

    payload = report.build_report(root=tmp_path, state=state, generated_at="2026-01-01T00:03:00Z")

    assert payload["loaded_commit"] == loaded_commit
    assert payload["reconstruction_status"] == "MATCH"
    assert payload["citation_allowed"] is True
    assert payload["historical_loaded_file_count"] == 3
    assert payload["comparison_file_count"] == 2
    assert payload["loaded_only_paths"] == ["missing.json"]
    assert payload["changed_count"] == 1
    assert [row["path"] for row in payload["rows"] if row["changed"]] == ["a.txt"]


def test_report_fails_closed_when_loaded_aggregate_does_not_reconstruct(tmp_path: Path):
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    (tmp_path / "scripts").mkdir()
    (tmp_path / report.HISTORICAL_GENERATION_SOURCE).write_text(
        'from pathlib import Path\nROOT = Path(".")\nGENERATION_FILES = (ROOT / "a.txt",)\n'
    )
    (tmp_path / "a.txt").write_text("a-v1")
    _commit(tmp_path, "loaded", "2026-01-01T00:00:00Z")
    disk_sha = hashlib.sha256(b"a-v1").hexdigest()
    state = {
        "loaded_generation": {
            "sha256": "not-the-loaded-sha",
            "started_at_utc": "2026-01-01T00:01:00Z",
        },
        "disk_generation": {
            "sha256": "disk",
            "files": [{"path": "a.txt", "exists": True, "sha256": disk_sha}],
        },
    }

    payload = report.build_report(root=tmp_path, state=state)

    assert payload["reconstruction_status"] == "MISMATCH"
    assert payload["citation_allowed"] is False
    assert payload["status"] == "FAIL_RECONSTRUCTION_MISMATCH"
