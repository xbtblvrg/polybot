from __future__ import annotations

import json

from scripts.report_wallet_copy_shadow_rollup import build_rollup
from src.wallet_copy.store import atomic_write_json


def test_shadow_rollup_counts_provenance_and_zero_submit(tmp_path):
    events = tmp_path / "events.jsonl"
    state = tmp_path / "state.json"
    events.write_text(
        "\n".join(
            [
                json.dumps({"event": "wallet_copy_guard_shadow_lanes_snapshot"}),
                json.dumps({"event": "shadow_intent_first_seen", "lane": "e5", "intent_id": "ci_1"}),
                json.dumps({"event": "shadow_book_test", "lane": "e5", "intent_id": "ci_1"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    atomic_write_json(
        state,
        {
            "rows": [
                {
                    "lane": "e5",
                    "intent_id": "ci_1",
                    "first_seen_at": "2026-07-06T06:40:00Z",
                    "orders_submitted": 0,
                    "would_have_filled": True,
                    "book_aware_fill_test": {
                        "instant_fill_status": "PASS",
                        "fallback_source": "direct_clob_after_primary_failure",
                    },
                    "pricing_provenance": {
                        "category": "direct_fallback",
                        "fallback_source": "direct_clob_after_primary_failure",
                    },
                },
                {
                    "lane": "e5",
                    "intent_id": "ci_2",
                    "first_seen_at": "2026-07-06T06:41:00Z",
                    "orders_submitted": 0,
                    "would_have_filled": False,
                    "book_aware_fill_test": {"instant_fill_status": "BLOCKED"},
                    "pricing_provenance": {"category": "genuine_book"},
                },
            ]
        },
    )

    rollup = build_rollup(events_path=events, state_path=state)

    assert rollup["zero_live_assertion"] == {"status": "PASS", "orders_submitted": 0}
    assert rollup["evidence_event_counts"]["shadow_book_test"] == 1
    assert rollup["lanes"]["e5"]["rows_seen"] == 2
    assert rollup["lanes"]["e5"]["unique_intents"] == 2
    assert rollup["lanes"]["e5"]["book_aware_fill_test_counts"] == {"BLOCKED": 1, "PASS": 1}
    assert rollup["lanes"]["e5"]["pricing_provenance_split"] == {"book_derived": 1, "direct_fallback": 1}
    assert rollup["lanes"]["e5"]["fallback_reason_histogram"] == {"direct_clob_after_primary_failure": 1}
    assert rollup["lanes"]["e5"]["would_have_filled_first_seen"] == 1
