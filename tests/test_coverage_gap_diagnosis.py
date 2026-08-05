import json

from scripts.report_coverage_gap_diagnosis import build_report


def test_coverage_gap_diagnosis_classifies_zero_submission_windows(tmp_path) -> None:
    start = 1_800_000_000
    guard_events = tmp_path / "guard_events.jsonl"
    rows = [
        {
            "market_slug": f"btc-updown-5m-{start + 300}",
            "window_start_s": start + 300,
            "wallet_eligible_orders": 4,
            "our_submits": 0,
            "our_fills": 0,
            "missed_active_window": True,
            "dominant_skip_reason_counts": {"window_time_gte_180s": 1},
        },
        {
            "market_slug": f"btc-updown-5m-{start + 600}",
            "window_start_s": start + 600,
            "wallet_eligible_orders": 3,
            "our_submits": 0,
            "our_fills": 0,
            "missed_active_window": True,
            "dominant_skip_reason_counts": {"inventory_best_ask_above_vwap_plus_buffer": 1},
        },
        {
            "market_slug": f"btc-updown-5m-{start + 900}",
            "window_start_s": start + 900,
            "wallet_eligible_orders": 2,
            "our_submits": 0,
            "our_fills": 0,
            "missed_active_window": True,
            "dominant_skip_reason_counts": {},
        },
        {
            "market_slug": f"btc-updown-5m-{start + 1200}",
            "window_start_s": start + 1200,
            "wallet_eligible_orders": 2,
            "our_submits": 0,
            "our_fills": 0,
            "missed_active_window": True,
            "dominant_skip_reason_counts": {"selected_candidate_failed_pass_gate": 1},
        },
        {
            "market_slug": f"btc-updown-5m-{start + 1500}",
            "window_start_s": start + 1500,
            "wallet_eligible_orders": 7,
            "our_submits": 1,
            "our_fills": 0,
            "dominant_skip_reason_counts": {"inventory_target_already_met": 1},
        },
    ]
    guard_events.write_text(json.dumps({"window_participation": {"window_rollups": rows}}) + "\n", encoding="utf-8")
    ledger = {"orders": [{"market_slug": f"btc-updown-5m-{start + 1500}", "status": "REJECTED"}]}

    report = build_report(
        ledger=ledger,
        guard_state={},
        history_state={},
        guard_events_path=guard_events,
        lookback_hours=0.5,
        end_ts=start + 1800,
    )

    assert report["summary"]["windows_total"] == 6
    assert report["summary"]["submitted_windows"] == 1
    assert report["summary"]["zero_submission_windows"] == 5
    assert report["summary"]["reason_class_counts"] == {
        "no-eligible-signal": 1,
        "selector-abstain": 1,
        "price/eligibility-filter": 1,
        "guard-reject": 1,
        "stale-flow-protected": 1,
    }
    by_slug = {row["market_slug"]: row["reason_class"] for row in report["rows"]}
    assert by_slug[f"btc-updown-5m-{start}"] == "no-eligible-signal"
    assert by_slug[f"btc-updown-5m-{start + 300}"] == "stale-flow-protected"
    assert by_slug[f"btc-updown-5m-{start + 600}"] == "price/eligibility-filter"
    assert by_slug[f"btc-updown-5m-{start + 900}"] == "guard-reject"
    assert by_slug[f"btc-updown-5m-{start + 1200}"] == "selector-abstain"
    assert f"btc-updown-5m-{start + 1500}" not in by_slug
    evidence_by_slug = {
        row["market_slug"]: row for row in report["rows"]
    }
    selector_row = evidence_by_slug[f"btc-updown-5m-{start + 1200}"]
    assert selector_row["abstaining_predicate"] == "selected_candidate_failed_pass_gate"
    assert selector_row["measured_input"] == {
        "predicate_count": 1,
        "wallet_eligible_orders": 2,
        "missed_active_window": True,
    }
    assert {
        row["predicate"]: row["windows_foreclosed"]
        for row in report["summary"]["abstaining_predicates_ranked"]
    } == {
        "no_eligible_signal": 1,
        "window_time_gte_180s": 1,
        "inventory_best_ask_above_vwap_plus_buffer": 1,
        "guard_reject_no_named_predicate": 1,
        "selected_candidate_failed_pass_gate": 1,
    }


def test_coverage_gap_diagnosis_deduplicates_repeated_guard_rollups(tmp_path) -> None:
    start = 1_800_000_000
    guard_events = tmp_path / "guard_events.jsonl"
    row = {
        "market_slug": f"btc-updown-5m-{start}",
        "window_start_s": start,
        "source_wallet": "0xabc",
        "outcomes": ["Up"],
        "wallet_eligible_orders": 3,
        "our_submits": 0,
        "our_fills": 0,
        "missed_active_window": True,
        "dominant_skip_reason_counts": {"window_time_gte_180s": 1},
    }
    guard_events.write_text(
        "\n".join(
            [
                json.dumps({"window_participation": {"recent_window_rollups": [row]}}),
                json.dumps({"window_participation": {"recent_window_rollups": [row]}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_report(
        ledger={"orders": []},
        guard_state={},
        history_state={},
        guard_events_path=guard_events,
        lookback_hours=1 / 12,
        end_ts=start + 300,
    )

    assert report["summary"]["zero_submission_windows"] == 1
    assert report["rows"][0]["wallet_eligible_orders"] == 3
    assert report["rows"][0]["reason_class"] == "stale-flow-protected"


def test_coverage_gap_diagnosis_uses_retained_history_for_aged_out_rollups(tmp_path) -> None:
    start = 1_800_000_000
    guard_events = tmp_path / "guard_events.jsonl"
    guard_events.write_text("", encoding="utf-8")
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    inactive_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    report = build_report(
        ledger={"orders": []},
        guard_state={
            "active_set_runtime": {
                "members": [
                    {"source_wallet": wallet, "candidate_id": "active", "enabled": True},
                    {"source_wallet": inactive_wallet, "candidate_id": "off", "enabled": False},
                ]
            }
        },
        history_state={
            "events": [
                {
                    "source_wallet": wallet,
                    "market_slug": f"btc-updown-5m-{start}",
                    "source": "rtds_activity",
                    "action": "BUY",
                    "price": 0.51,
                    "event_ts": start + 20,
                    "observed_ts": start + 21,
                    "transaction_hash": "0x1",
                },
                {
                    "source_wallet": inactive_wallet,
                    "market_slug": f"btc-updown-5m-{start + 300}",
                    "source": "rtds_activity",
                    "action": "BUY",
                    "price": 0.52,
                    "event_ts": start + 320,
                    "observed_ts": start + 321,
                    "transaction_hash": "0x2",
                },
            ]
        },
        guard_events_path=guard_events,
        lookback_hours=1 / 6,
        end_ts=start + 600,
    )

    by_slug = {row["market_slug"]: row for row in report["rows"]}
    first = by_slug[f"btc-updown-5m-{start}"]
    second = by_slug[f"btc-updown-5m-{start + 300}"]

    assert report["summary"]["history_derived_zero_submission_windows"] == 1
    assert report["summary"]["observed_selector_abstain_windows"] == 0
    assert report["summary"]["history_derived_selector_abstain_windows"] == 1
    assert report["summary"]["reason_class_counts"]["selector-abstain"] == 1
    assert report["summary"]["reason_class_counts"]["no-eligible-signal"] == 1
    assert first["reason_class"] == "selector-abstain"
    assert first["history_derived_signal"] is True
    assert first["history_source_wallets"] == [wallet]
    assert first["abstaining_predicate"] == "retained_history_not_selected"
    assert first["measured_input"] == {
        "history_signal_count": 1,
        "history_source_wallets": [wallet],
    }
    assert second["reason_class"] == "no-eligible-signal"
    assert second["history_derived_signal"] is False
