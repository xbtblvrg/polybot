import datetime as dt
import json

from scripts import order_flow_deadman as deadman
from scripts import run_wallet_copy_live_guard as live_guard
from scripts.wide_direct_handoff_journal import append_envelopes, load_jsonl


def _envelope(*, rows: int) -> dict:
    generation = "generation"
    attempt_rows = [
        {
            "attempt_id": f"attempt-{index}",
            "order_id": f"order-{index}",
            "wallet": "0x" + "a" * 40,
            "recorded_at": f"2026-08-01T12:00:0{index}+00:00",
            "f1_f4_terminal": {
                "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
                "F4_executable_book": "PASS",
            },
        }
        for index in range(rows)
    ]
    return {
        "kind": "wide_direct_handoff_generation",
        "captured_at": f"2026-08-01T12:00:0{rows}+00:00",
        "source_generation": generation,
        "identity": {"run_id": "run", "policy_id": "policy"},
        "input_rows": rows,
        "terminal_rows": rows,
        "input_equals_terminal": True,
        "rows": attempt_rows,
        "copyable_orders": attempt_rows,
    }


def test_append_envelopes_writes_identity_deltas_and_readers_reconstruct(tmp_path) -> None:
    journal = tmp_path / "direct.jsonl"

    assert append_envelopes(journal, [_envelope(rows=1)]) == 1
    assert append_envelopes(journal, [_envelope(rows=2)]) == 1
    assert append_envelopes(journal, [_envelope(rows=2)]) == 0

    physical = [json.loads(line) for line in journal.read_text().splitlines()]
    assert [len(row["rows"]) for row in physical] == [1, 1]
    assert [len(row["copyable_orders"]) for row in physical] == [1, 1]
    assert physical[1]["input_rows"] == 2
    assert physical[1]["delta_encoding"] == {
        "schema_version": 1,
        "cumulative_snapshot": True,
        "prior_generation_seen": True,
        "delta_rows": 1,
        "delta_copyable_orders": 1,
        "cumulative_input_rows": 2,
        "cumulative_terminal_rows": 2,
    }

    snapshot = deadman._wide_direct_source_snapshot(
        {},
        now=dt.datetime(2026, 8, 1, 12, 1, tzinfo=dt.timezone.utc),
        max_packet_age_s=3600,
        journal=load_jsonl(journal),
    )
    assert snapshot["current_attempted_buy_rows"] == 2
    assert snapshot["current_copyable_rows"] == 2
    assert snapshot["copyable_parity"] is True


def test_delta_journal_tail_covers_thirty_minute_window_under_four_mb(tmp_path) -> None:
    journal = tmp_path / "direct.jsonl"
    base = dt.datetime(2026, 8, 1, 12, 0, tzinfo=dt.timezone.utc)
    cumulative: list[dict] = []
    for index in range(40):
        captured = base + dt.timedelta(minutes=index)
        cumulative.append(
            {
                "attempt_id": f"attempt-{index}",
                "wallet": "0x" + "b" * 40,
                "recorded_at": captured.isoformat(),
                "f1_f4_terminal": {"terminal": "REFUSED_TEST"},
            }
        )
        append_envelopes(
            journal,
            [
                {
                    "kind": "wide_direct_handoff_generation",
                    "captured_at": captured.isoformat(),
                    "source_generation": "generation",
                    "identity": {"run_id": "run", "policy_id": "policy"},
                    "input_rows": len(cumulative),
                    "terminal_rows": len(cumulative),
                    "input_equals_terminal": True,
                    "rows": list(cumulative),
                    "copyable_orders": [],
                }
            ],
        )

    rows, covered, bytes_read = live_guard._order135_journal_tail(
        journal,
        cutoff=base + dt.timedelta(minutes=8),
    )

    assert covered is True
    assert len(rows) == 40
    assert bytes_read < 4 * 1024 * 1024
    assert journal.stat().st_size < 100_000
