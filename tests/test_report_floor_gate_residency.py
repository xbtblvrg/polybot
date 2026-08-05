import datetime as dt
import json
from pathlib import Path

from scripts import report_floor_gate_residency as report


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_history_row_uses_canonical_helper_and_unknown_stale_generation(tmp_path: Path):
    scorecard = tmp_path / "scorecard.json"
    restart = tmp_path / "restart.json"
    _write(
        scorecard,
        {
            "canonical_pnl_truth": {
                "by_day": {
                    "2026-08-04": {
                        "fills": 9,
                        "floor_gate_enforced_fill_count": 0,
                        "ruled_floor_breach_count": 4,
                        "ruled_floor_breach_cost_usd": 4.759996,
                    }
                }
            }
        },
    )
    _write(
        restart,
        {
            "generated_at": "2026-08-04T15:00:00Z",
            "generation_mismatch": True,
            "loaded_generation": {"sha256": "loaded"},
            "disk_generation": {"sha256": "disk"},
        },
    )

    row = report.build_row(
        scorecard_path=scorecard,
        restart_state_path=restart,
        now=dt.datetime(2026, 8, 4, 16, tzinfo=dt.timezone.utc),
    )

    assert row is not None
    assert row["floor_gate_enforced_fill_rate"] == 0.0
    assert row["ruled_floor_breach_cost_usd"] == 4.759996
    assert row["generation_evidence_status"] == "UNKNOWN_STALE_OR_MISSING"
    assert row["generation_mismatch_citable"] is False


def test_history_upsert_is_one_row_per_day(tmp_path: Path):
    history = tmp_path / "events.jsonl"
    first = {"day_utc": "2026-08-04", "fills": 8, "captured_at": "one"}
    replacement = {"day_utc": "2026-08-04", "fills": 9, "captured_at": "two"}
    next_day = {"day_utc": "2026-08-05", "fills": 1, "captured_at": "three"}

    report.retain_daily_row(history, first)
    report.retain_daily_row(history, replacement)
    rows = report.retain_daily_row(history, next_day)

    assert rows == [replacement, next_day]
    assert [json.loads(line) for line in history.read_text().splitlines()] == rows
