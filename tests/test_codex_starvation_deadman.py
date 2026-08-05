from __future__ import annotations

from datetime import datetime, timezone

import subprocess
from pathlib import Path

from scripts.codex_starvation_deadman import (
    direction_has_nonempty_next,
    evaluate,
    last_codex_commit,
    latest_direction_block,
)


def test_codex_starvation_detects_nonempty_latest_direction_queue() -> None:
    text = (
        "# HANDOFF\n\n"
        "## 2026-07-09T01:00Z fable DIRECTION [LIVE]\n"
        "- QUEUE: already served\n\n"
        "## 2026-07-09T02:00Z codex STATUS [LIVE]\n"
        "- done\n\n"
        "## 2026-07-09T03:00Z fable DIRECTION [SELF-DEV]\n"
        "- RESUMPTION QUEUE (codex, in order):\n"
        "  1. P1c handback.\n"
    )

    heading, block = latest_direction_block(text)

    assert "03:00Z" in heading
    assert direction_has_nonempty_next(block) is True


def test_codex_starvation_fires_when_heartbeat_pass_and_commit_stale() -> None:
    now = datetime(2026, 7, 9, 4, 0, tzinfo=timezone.utc)
    payload = evaluate(
        handoff_text=(
            "## 2026-07-09T03:00Z fable DIRECTION [SELF-DEV]\n"
            "- CODEX BACKLOG (reordered):\n"
            "  1. Implement service check.\n"
        ),
        last_commit={"status": "PASS", "commit": "abc", "commit_ts": now.timestamp() - 3600, "subject": "codex: old"},
        heartbeat={"status": "PASS", "pass": True},
        now=now,
        threshold_s=2700,
    )

    assert payload["status"] == "CODEX_STARVATION"
    assert payload["fable_escalation_armed"] is True
    assert payload["last_codex_commit_age_s"] == 3600


def test_codex_starvation_ok_when_commit_recent() -> None:
    now = datetime(2026, 7, 9, 4, 0, tzinfo=timezone.utc)
    payload = evaluate(
        handoff_text=(
            "## 2026-07-09T03:00Z fable DIRECTION [SELF-DEV]\n"
            "- QUEUE: serve this.\n"
        ),
        last_commit={"status": "PASS", "commit": "abc", "commit_ts": now.timestamp() - 60, "subject": "codex: fresh"},
        heartbeat={"status": "PASS", "pass": True},
        now=now,
        threshold_s=2700,
    )

    assert payload["status"] == "OK"


def test_codex_starvation_arms_on_missing_next_contract_breach() -> None:
    now = datetime(2026, 7, 9, 4, 0, tzinfo=timezone.utc)
    payload = evaluate(
        handoff_text=(
            "## 2026-07-09T03:00Z fable DIRECTION [SELF-DEV]\n"
            "- diagnostics only.\n"
        ),
        last_commit={"status": "PASS", "commit": "abc", "commit_ts": now.timestamp() - 60, "subject": "codex: fresh"},
        heartbeat={"status": "PASS", "pass": True},
        now=now,
        threshold_s=2700,
    )

    assert payload["status"] == "FABLE_DIRECTION_CONTRACT_BREACH"
    assert payload["fable_escalation_armed"] is True
    assert payload["latest_direction_has_next"] is False


def test_codex_starvation_arms_on_handoff_parse_failure() -> None:
    now = datetime(2026, 7, 9, 4, 0, tzinfo=timezone.utc)
    payload = evaluate(
        handoff_text="# no direction heading\n",
        last_commit={"status": "PASS", "commit": "abc", "commit_ts": now.timestamp() - 60, "subject": "codex: fresh"},
        heartbeat={"status": "PASS", "pass": True},
        now=now,
        threshold_s=2700,
    )

    assert payload["status"] == "HANDOFF_PARSE_FAILED"
    assert payload["fable_escalation_armed"] is True


def _git(repo: Path, *argv: str) -> None:
    subprocess.run(
        ["git", *argv],
        cwd=repo,
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@t",
            "HOME": str(repo),
            "PATH": "/usr/bin:/bin",
        },
    )


def test_last_codex_commit_prefers_newer_unprefixed_serving_commit(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "codex: old prefixed serving commit")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "fable: 00:00Z DIRECTION")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "Refresh ORDER2d heartbeat evidence")

    row = last_codex_commit(tmp_path)

    assert row["status"] == "FALLBACK_NON_FABLE_PREFIX"
    assert row["subject"] == "Refresh ORDER2d heartbeat evidence"
