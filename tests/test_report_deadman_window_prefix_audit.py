import datetime as dt
import json
from pathlib import Path

from scripts import report_deadman_window_prefix_audit as report


def test_prefix_audit_uses_deciding_function_and_names_incomplete_span(tmp_path: Path):
    incidents = tmp_path / "incidents.jsonl"
    incidents.write_text(
        json.dumps(
            {
                "checked_at": "2026-08-04T15:00:00Z",
                "gated_quiet_classification": {
                    "decision_gate_taxonomy": {
                        "window:live_order_rejected": 2,
                        "window:stale_book_at_gate": 1,
                        "window:window_time_gte_60s": 3,
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    payload = report.build_report(
        incidents_path=incidents,
        now=dt.datetime(2026, 8, 4, 16, tzinfo=dt.timezone.utc),
    )
    rows = {row["reason"]: row for row in payload["rows"]}
    assert rows["window:live_order_rejected"]["approved"] is True
    assert rows["window:live_order_rejected"]["prefix_only_approval"] is True
    assert rows["window:window_time_gte_60s"]["approved"] is True
    assert rows["window:window_time_gte_60s"]["prefix_only_approval"] is False
    assert rows["window:stale_book_at_gate"]["approved"] is False
    assert rows["window:stale_book_at_gate"]["base_reason_evidence_stale"] is True
    assert payload["source_coverage"]["full_seven_day_coverage"] is False
    assert payload["summary"]["verdict"] == "INSUFFICIENT_OBSERVED_SPAN"
    assert payload["summary"]["counter_semantics"] == (
        "snapshot_multiplicative_not_distinct_windows"
    )
    assert payload["summary"]["distinct_incident_snapshots"] == 1
