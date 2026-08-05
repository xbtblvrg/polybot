import hashlib
import json
from pathlib import Path

from scripts.write_research_artifact_digest import build_digest


def test_digest_preserves_status_counters_clocks_hash_and_row_count(tmp_path: Path) -> None:
    source = tmp_path / "large.json"
    payload = {
        "status": "PASS",
        "generated_at": "2026-07-20T04:00:00Z",
        "summary": {"events": 3, "pnl_usd": 1.25},
        "decision_deadline_utc": "2026-07-22T19:00:00Z",
        "rows": [{"samples": [1, 2]}, {"samples": []}],
    }
    raw = json.dumps(payload).encode()
    source.write_bytes(raw)
    digest = build_digest(source, repo_root=tmp_path)
    assert digest["source_status"] == "PASS"
    assert digest["counters"] == payload["summary"]
    assert digest["preregistered_clock_fields"]["decision_deadline_utc"] == "2026-07-22T19:00:00Z"
    assert digest["sha256"] == hashlib.sha256(raw).hexdigest()
    assert digest["row_count"] == 4
    assert digest["top_level_list_counts"] == {"rows": 2}


def test_digest_bounds_nested_counter_rows(tmp_path: Path) -> None:
    source = tmp_path / "nested.json"
    source.write_text(json.dumps({"summary": {"rows": [{"large": "payload"}] * 1000}}))
    digest = build_digest(source, repo_root=tmp_path)
    assert digest["counters"] == {"rows": {"row_count": 1000}}
