import json

from scripts.launch_member_native_policy_acceptance_uplift_shadow import build_launchd_payload
from scripts.report_member_native_policy_acceptance_uplift_shadow import build_report


START = "2026-07-23T16:30:00Z"
F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
ALT = "0x3048d65321be3497164cdfc2996f94f98a2e7537"


def _guard():
    return {
        "active_set_runtime": {
            "members": [
                {"source_wallet": F418, "candidate_id": "incumbent"},
                {"source_wallet": ALT, "candidate_id": "alternate"},
            ],
            "policy_by_wallet": {
                F418: {"policy_id": "p4", "max_price": 0.5},
                ALT: {"policy_id": "p1", "max_price": 0.4},
            },
        }
    }


def _poller():
    return {
        "generated_at": START,
        "fetch_meta": {
            F418: {
                "raw_rows": 10,
                "normalized_trade_events": 8,
                "policy_feedback": {"policy_compatible_fresh_buy_rows_le_30s": 2},
            },
            ALT: {
                "raw_rows": 20,
                "normalized_trade_events": 15,
                "policy_feedback": {"policy_compatible_fresh_buy_rows_le_30s": 5},
            },
        },
    }


def _routing(alt_windows=20, alt_pnl=0.2, incumbent_pnl=0.1):
    rows = [
        {
            "cycle_generated_at": "2026-07-23T16:31:00Z",
            "winning_source_wallet": F418,
            "market_slug": "btc-updown-5m-1",
            "window_start_s": 1,
            "winning_intent_id": "incumbent-1",
            "winning_policy_id": "p4",
            "expected_fee_usd": 0.0,
            "realized_paper_outcome": {
                "status": "RESOLVED",
                "paper_pnl_usd": incumbent_pnl,
            },
        }
    ]
    rows.extend(
        {
            "cycle_generated_at": "2026-07-23T16:31:00Z",
            "winning_source_wallet": ALT,
            "market_slug": f"btc-updown-5m-{index + 2}",
            "window_start_s": index + 2,
            "winning_intent_id": f"alternate-{index}",
            "winning_policy_id": "p1",
            "expected_fee_usd": 0.0,
            "realized_paper_outcome": {"status": "RESOLVED", "paper_pnl_usd": alt_pnl},
        }
        for index in range(alt_windows)
    )
    return {"rows": rows}


def test_report_passes_only_on_positive_incremental_closed_windows():
    report = build_report(
        _guard(),
        _poller(),
        _routing(),
        {"rolling_30m": {"policy_eligible_unique_intents": 28, "actual_accepted_orders": 5}},
        generated_at="2026-07-23T17:00:00Z",
        sample_started_at=START,
    )
    assert report["frozen_policy_binding_count"] == 2
    assert report["incremental"]["resolved_windows"] == 20
    assert report["incremental"]["post_fee_pnl_usd"] == 4.0
    assert report["gate"]["pass"] is True
    assert report["verdict"] == "PASS_MEMBER_NATIVE_POLICY_UPLIFT"
    assert report["live_mutation"] is False
    assert report["single_submitter_preserved"] is True


def test_report_accrues_when_sample_is_short_and_deduplicates_windows():
    routing = _routing(alt_windows=2)
    routing["rows"].append(dict(routing["rows"][-1]))
    routing["rows"].append(
        {
            "cycle_generated_at": "2026-07-23T16:29:59Z",
            "winning_source_wallet": ALT,
            "market_slug": "btc-updown-5m-old",
            "winning_intent_id": "old",
            "winning_policy_id": "p1",
            "expected_fee_usd": 0.0,
            "realized_paper_outcome": {"status": "RESOLVED", "paper_pnl_usd": 99.0},
        }
    )
    report = build_report(
        _guard(),
        _poller(),
        routing,
        {},
        generated_at="2026-07-23T17:00:00Z",
        sample_started_at=START,
    )
    assert report["incremental"]["resolved_windows"] == 2
    assert report["incremental"]["policy_compatible_fresh_buy_rows_le_30s"] == 5
    assert report["gate"]["pass"] is False
    assert report["verdict"] == "ACCRUE_MEMBER_NATIVE_MEASURED_WINDOWS"


def test_fees_turn_apparent_positive_cohort_negative_and_fail_closed():
    routing = _routing(alt_windows=20, alt_pnl=0.19)
    for row in routing["rows"][1:]:
        row["expected_fee_usd"] = 0.19
    report = build_report(
        _guard(),
        _poller(),
        routing,
        {},
        generated_at="2026-07-23T17:00:00Z",
        sample_started_at=START,
    )
    assert report["incremental"]["gross_pnl_usd"] == 3.8
    assert report["incremental"]["expected_fee_usd"] == 3.8
    assert report["incremental"]["post_fee_pnl_usd"] == 0.0
    assert report["gate"]["pass"] is False


def test_null_pnl_rows_do_not_satisfy_twenty_window_gate():
    routing = _routing(alt_windows=20, alt_pnl=None)
    routing["rows"][-1]["realized_paper_outcome"]["paper_pnl_usd"] = 0.19
    report = build_report(
        _guard(),
        _poller(),
        routing,
        {},
        generated_at="2026-07-23T17:00:00Z",
        sample_started_at=START,
    )
    assert report["incremental"]["measured_windows"] == 1
    assert report["exclusions"]["unmeasured_resolved_windows"] == 19
    assert report["gate"]["pass"] is False


def test_two_intents_in_one_window_are_aggregated_and_permutation_invariant():
    routing = _routing(alt_windows=1, alt_pnl=0.3)
    second = dict(routing["rows"][-1])
    second["winning_intent_id"] = "alternate-second"
    second["expected_fee_usd"] = 0.05
    second["realized_paper_outcome"] = {"status": "RESOLVED", "paper_pnl_usd": -0.1}
    routing["rows"].append(second)
    first = build_report(
        _guard(), _poller(), routing, {}, generated_at="A", sample_started_at=START
    )
    shuffled = build_report(
        _guard(),
        _poller(),
        {"rows": list(reversed(routing["rows"]))},
        {},
        generated_at="B",
        sample_started_at=START,
    )
    assert first["incremental"]["measured_windows"] == 1
    assert first["incremental"]["measured_intents"] == 2
    assert first["incremental"]["post_fee_pnl_usd"] == 0.15
    for report in (first, shuffled):
        report.pop("generated_at")
        report["cohort_state"].pop("last_generated_at")
    assert json.dumps(first, sort_keys=True) == json.dumps(shuffled, sort_keys=True)


def test_wrong_frozen_policy_invalidates_profitable_cohort():
    routing = _routing()
    routing["rows"][1]["winning_policy_id"] = "wrong-policy"
    report = build_report(
        _guard(), _poller(), routing, {}, generated_at="A", sample_started_at=START
    )
    assert report["sample_valid"] is False
    assert report["gate"]["pass"] is False
    assert report["verdict"] == "INVALID_MEMBER_NATIVE_COHORT"
    assert report["exclusions"]["policy_mismatch_rows"] == 1


def test_cohort_state_resumes_epoch_and_measured_count_is_monotone():
    first = build_report(
        _guard(),
        _poller(),
        _routing(alt_windows=1),
        {},
        generated_at="A",
        sample_started_at=START,
    )
    resumed = build_report(
        _guard(),
        _poller(),
        _routing(alt_windows=2),
        {},
        generated_at="B",
        sample_started_at="2099-01-01T00:00:00Z",
        cohort_state=first["cohort_state"],
    )
    assert resumed["cohort_id"] == first["cohort_id"]
    assert resumed["sample_started_at"] == START
    assert resumed["incremental"]["measured_windows"] == 2
    assert resumed["frozen_policy_snapshot_hash"] == first["frozen_policy_snapshot_hash"]


def test_launchd_payload_is_persistent_paper_only_reporter():
    payload = build_launchd_payload(
        python="/usr/bin/python3", stdout="/tmp/member.out", stderr="/tmp/member.err"
    )
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert "--watch" in payload["ProgramArguments"]
    assert "report_member_native_policy_acceptance_uplift_shadow.py" in payload["ProgramArguments"][1]
