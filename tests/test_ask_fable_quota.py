import json
import os
import shutil
import subprocess
from pathlib import Path


def _write_tool(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env sh\n" + body)
    path.chmod(0o755)


def test_ask_fable_skips_agy_during_quota_window(tmp_path: Path) -> None:
    root = tmp_path
    (root / "scripts").mkdir()
    (root / "docs" / "agents").mkdir(parents=True)
    (root / "data" / "research").mkdir(parents=True)
    shutil.copy(Path(__file__).resolve().parents[1] / "scripts" / "ask_fable.sh", root / "scripts" / "ask_fable.sh")
    (root / "docs" / "agents" / "HANDOFF.md").write_text("# handoff\n")
    (root / "data" / "research" / "state_digest.md").write_text("pnl: smoke\nvolume: smoke\n")

    bin_dir = root / "bin"
    bin_dir.mkdir()
    _write_tool(bin_dir / "claude", "echo 'session limit' >&2\nexit 1\n")
    _write_tool(bin_dir / "grok", "echo 'grok fail' >&2\nexit 43\n")
    count_file = root / "agy_count"
    _write_tool(
        bin_dir / "agy",
        f"count_file='{count_file}'\n"
        "count=$(cat \"$count_file\" 2>/dev/null || echo 0)\n"
        "count=$((count + 1))\n"
        "echo \"$count\" > \"$count_file\"\n"
        "echo 'Error: Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 1h2m3s.' >&2\n"
        "exit 1\n",
    )
    _write_tool(bin_dir / "codex", "echo 'codex fallback disabled in test' >&2\nexit 1\n")

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "ASK_FABLE_CLAUDE_TIMEOUT_S": "1",
            "ASK_FABLE_GROK_TIMEOUT_S": "1",
            "AGY_TIMEOUT_S": "1",
            "AGY_PRINT_TIMEOUT": "1s",
            "CODEX_TIMEOUT_S": "1",
        }
    )

    first = subprocess.run(
        ["bash", "scripts/ask_fable.sh", "quota test"],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )
    quota_state = json.loads((root / "data" / "research" / "agy_quota_state.json").read_text())
    assert quota_state["status"] == "DEGRADED_QUOTA"
    assert quota_state["raw_reset"] == "Resets in 1h2m3s"
    assert count_file.read_text().strip() == "2"

    second = subprocess.run(
        ["bash", "scripts/ask_fable.sh", "quota test"],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )

    assert "agy skipped (quota until " in second.stdout
    assert count_file.read_text().strip() == "2"
    assert first.returncode == 0
    assert second.returncode == 0
