import subprocess
from pathlib import Path

from scripts import append_handoff_and_commit as atomic_handoff


def test_append_and_commit_records_only_handoff_transaction(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    handoff = repo / "docs/agents/HANDOFF.md"
    handoff.parent.mkdir(parents=True)
    handoff.write_text("# HANDOFF\n", encoding="utf-8")
    unrelated = repo / "unrelated.txt"
    unrelated.write_text("tracked\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    unrelated.write_text("user delta\n", encoding="utf-8")
    pending = repo / "pending.md"
    pending.write_text("\n## brainless STATUS\n- next=continue\n", encoding="utf-8")
    monkeypatch.setattr(atomic_handoff, "ROOT", repo)

    result = atomic_handoff.append_and_commit(
        handoff=Path("docs/agents/HANDOFF.md"),
        pending=pending,
        lock=Path(".git/handoff.lock"),
        message="test: atomic handoff",
    )

    assert result["status"] == "APPENDED_AND_COMMITTED"
    assert "brainless STATUS" in handoff.read_text(encoding="utf-8")
    assert subprocess.check_output(["git", "show", "--pretty=", "--name-only", "HEAD"], cwd=repo, text=True).strip() == "docs/agents/HANDOFF.md"
    assert subprocess.check_output(["git", "status", "--short"], cwd=repo, text=True).splitlines() == [" M unrelated.txt", "?? pending.md"]
