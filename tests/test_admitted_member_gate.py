from __future__ import annotations

import scripts.report_admitted_member_gate as gate


def _member(candidate_id: str, wallet: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "source_wallet": wallet,
        "policy_id": "protection_refill_0.10_cap_8_auto_degrade_le_50",
        "status": "FABLE_0608_ADMITTED_STANDARD_POLICY",
        "max_price": 0.5,
    }


def test_admitted_member_gate_classifies_source_inactive_before_deadline() -> None:
    members = [
        _member("shadow_realtime_8bc176d95c", "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473"),
        _member("shadow_realtime_cdf7909833", "0xcdf79098337926ce014ac680d98cc9728439824e"),
    ]
    guard_events = [
        {
            "event": "wallet_copy_live_guard_cycle",
            "generated_at": "2026-07-07T06:24:00Z",
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "candidate": {
                "candidate_id": "shadow_realtime_8bc176d95c",
                "source_wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
            },
            "live_execution": {
                "status": "LIVE_ARMED_NO_FRESH_INTENTS",
                "fresh_candidate_intents": 0,
                "new_live_candidate_intents": 0,
                "orders_submitted": 0,
            },
            "participation_alerts": ["no_fresh_live_tradeable_intents"],
        },
        {
            "event": "wallet_copy_live_guard_cycle",
            "generated_at": "2026-07-07T06:24:01Z",
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "candidate": {
                "candidate_id": "shadow_realtime_cdf7909833",
                "source_wallet": "0xcdf79098337926ce014ac680d98cc9728439824e",
            },
            "live_execution": {
                "status": "LIVE_ARMED_NO_FRESH_INTENTS",
                "fresh_candidate_intents": 0,
                "new_live_candidate_intents": 0,
                "orders_submitted": 0,
            },
            "participation_alerts": ["no_fresh_live_tradeable_intents"],
        },
    ]
    source_reports = {
        "shadow_realtime_8bc176d95c": {
            "summary": {
                "btc5m_buy_rows_since": 22,
                "btc5m_buy_windows_since": 5,
                "source_active_rows": 0,
                "source_active_windows": 0,
                "policy_eligible_rows": 0,
                "policy_eligible_windows": 0,
            }
        },
        "shadow_realtime_cdf7909833": {"summary": {}},
    }

    report = gate.build_report(
        guard_events=guard_events,
        guard_state={},
        live_ledger_state={"orders": []},
        source_reports=source_reports,
        members=members,
        since_ts=gate._parse_ts_required("2026-07-07T06:23:00Z", label="since"),
        gate_deadline_ts=gate._parse_ts_required("2026-07-07T08:30:00Z", label="deadline"),
        generated_at="2026-07-07T07:00:00Z",
    )

    assert report["summary"]["gate_status"] == "PENDING_BEFORE_DEADLINE"
    assert report["summary"]["classification_counts"] == {"ADMITTED_SOURCE_INACTIVE": 2}
    first = report["members"][0]
    assert first["cycles_landed"] == 1
    assert first["fresh_intents"] == 0
    assert first["source_activity"]["outside_band_rows"] == 22


def test_admitted_member_gate_classifies_pipeline_source_activity_gap() -> None:
    members = [_member("shadow_realtime_8bc176d95c", "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473")]
    guard_events = [
        {
            "event": "wallet_copy_live_guard_cycle",
            "generated_at": "2026-07-07T08:31:00Z",
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "candidate_id": "shadow_realtime_8bc176d95c",
            "source_wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
            "live_execution": {
                "status": "LIVE_ARMED_NO_FRESH_INTENTS",
                "fresh_candidate_intents": 0,
                "new_live_candidate_intents": 0,
                "orders_submitted": 0,
            },
            "participation_alerts": ["no_fresh_live_tradeable_intents"],
        }
    ]
    source_reports = {
        "shadow_realtime_8bc176d95c": {
            "summary": {
                "btc5m_buy_rows_since": 3,
                "btc5m_buy_windows_since": 1,
                "source_active_rows": 3,
                "source_active_windows": 1,
                "policy_eligible_rows": 3,
                "policy_eligible_windows": 1,
            },
            "windows": [
                {
                    "market_slug": "btc-updown-5m-1783413000",
                    "first_event_iso": "2026-07-07T08:30:50Z",
                    "first_policy_event_iso": "2026-07-07T08:30:50Z",
                    "first_policy_received_iso": "2026-07-07T08:30:50Z",
                    "first_received_iso": "2026-07-07T08:30:50Z",
                    "last_event_iso": "2026-07-07T08:30:55Z",
                    "last_policy_event_iso": "2026-07-07T08:30:55Z",
                    "last_policy_received_iso": "2026-07-07T08:30:55Z",
                    "last_received_iso": "2026-07-07T08:30:55Z",
                    "rows": 3,
                    "le_max_price_rows": 3,
                }
            ],
        }
    }

    report = gate.build_report(
        guard_events=guard_events,
        guard_state={},
        live_ledger_state={"orders": []},
        source_reports=source_reports,
        members=members,
        since_ts=gate._parse_ts_required("2026-07-07T06:23:00Z", label="since"),
        gate_deadline_ts=gate._parse_ts_required("2026-07-07T08:30:00Z", label="deadline"),
        generated_at="2026-07-07T08:31:10Z",
    )

    assert report["summary"]["gate_status"] == "FAIL_PIPELINE_EVIDENCE"
    assert report["members"][0]["classification"] == "PIPELINE_NO_INTENTS_FROM_SOURCE_ACTIVITY"
    assert report["members"][0]["source_activity"]["policy_windows_with_cycle_fresh_overlap"] == 1
    assert report["members"][0]["source_activity"]["policy_windows_with_received_fresh_overlap"] == 1


def test_admitted_member_gate_separates_source_activity_from_cycle_overlap() -> None:
    members = [_member("shadow_realtime_8bc176d95c", "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473")]
    guard_events = [
        {
            "event": "wallet_copy_live_guard_cycle",
            "generated_at": "2026-07-07T08:31:40Z",
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "candidate_id": "shadow_realtime_8bc176d95c",
            "source_wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
            "live_execution": {
                "status": "LIVE_ARMED_NO_FRESH_INTENTS",
                "fresh_candidate_intents": 0,
                "new_live_candidate_intents": 0,
                "orders_submitted": 0,
            },
            "participation_alerts": ["no_fresh_live_tradeable_intents"],
        }
    ]
    source_reports = {
        "shadow_realtime_8bc176d95c": {
            "summary": {
                "btc5m_buy_rows_since": 3,
                "btc5m_buy_windows_since": 1,
                "source_active_rows": 3,
                "source_active_windows": 1,
                "policy_eligible_rows": 3,
                "policy_eligible_windows": 1,
            },
            "windows": [
                {
                    "market_slug": "btc-updown-5m-1783413000",
                    "first_event_iso": "2026-07-07T08:30:00Z",
                    "first_policy_event_iso": "2026-07-07T08:30:00Z",
                    "first_policy_received_iso": "2026-07-07T08:30:00Z",
                    "first_received_iso": "2026-07-07T08:30:00Z",
                    "last_event_iso": "2026-07-07T08:30:03Z",
                    "last_policy_event_iso": "2026-07-07T08:30:03Z",
                    "last_policy_received_iso": "2026-07-07T08:30:03Z",
                    "last_received_iso": "2026-07-07T08:30:03Z",
                    "rows": 3,
                    "le_max_price_rows": 3,
                }
            ],
        }
    }

    report = gate.build_report(
        guard_events=guard_events,
        guard_state={},
        live_ledger_state={"orders": []},
        source_reports=source_reports,
        members=members,
        since_ts=gate._parse_ts_required("2026-07-07T06:23:00Z", label="since"),
        gate_deadline_ts=gate._parse_ts_required("2026-07-07T08:30:00Z", label="deadline"),
        generated_at="2026-07-07T08:31:50Z",
    )

    assert report["summary"]["gate_status"] == "FAIL_PIPELINE_EVIDENCE"
    assert report["members"][0]["classification"] == "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP"
    assert report["members"][0]["source_activity"]["policy_windows_with_cycle_fresh_overlap"] == 0


def test_admitted_member_gate_uses_received_time_for_fresh_overlap() -> None:
    members = [_member("shadow_realtime_8bc176d95c", "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473")]
    guard_events = [
        {
            "event": "wallet_copy_live_guard_cycle",
            "generated_at": "2026-07-07T08:31:00Z",
            "cycle_outcome": "LIVE_GUARD_RUNNING",
            "candidate_id": "shadow_realtime_8bc176d95c",
            "source_wallet": "0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473",
            "live_execution": {
                "status": "LIVE_ARMED_NO_FRESH_INTENTS",
                "fresh_candidate_intents": 0,
                "new_live_candidate_intents": 0,
                "orders_submitted": 0,
            },
            "participation_alerts": ["no_fresh_live_tradeable_intents"],
        }
    ]
    source_reports = {
        "shadow_realtime_8bc176d95c": {
            "summary": {
                "btc5m_buy_rows_since": 1,
                "btc5m_buy_windows_since": 1,
                "source_active_rows": 1,
                "source_active_windows": 1,
                "policy_eligible_rows": 1,
                "policy_eligible_windows": 1,
            },
            "windows": [
                {
                    "market_slug": "btc-updown-5m-1783413000",
                    "first_event_iso": "2026-07-07T08:30:50Z",
                    "first_policy_event_iso": "2026-07-07T08:30:50Z",
                    "first_policy_received_iso": "2026-07-07T08:31:05Z",
                    "first_received_iso": "2026-07-07T08:31:05Z",
                    "last_event_iso": "2026-07-07T08:30:50Z",
                    "last_policy_event_iso": "2026-07-07T08:30:50Z",
                    "last_policy_received_iso": "2026-07-07T08:31:05Z",
                    "last_received_iso": "2026-07-07T08:31:05Z",
                    "rows": 1,
                    "le_max_price_rows": 1,
                }
            ],
        }
    }

    report = gate.build_report(
        guard_events=guard_events,
        guard_state={},
        live_ledger_state={"orders": []},
        source_reports=source_reports,
        members=members,
        since_ts=gate._parse_ts_required("2026-07-07T06:23:00Z", label="since"),
        gate_deadline_ts=gate._parse_ts_required("2026-07-07T08:30:00Z", label="deadline"),
        generated_at="2026-07-07T08:31:10Z",
    )

    activity = report["members"][0]["source_activity"]
    assert activity["policy_windows_with_event_fresh_overlap"] == 1
    assert activity["policy_windows_with_received_fresh_overlap"] == 0
    assert report["members"][0]["classification"] == "SOURCE_ACTIVE_BUT_NO_FRESH_CYCLE_OVERLAP"
