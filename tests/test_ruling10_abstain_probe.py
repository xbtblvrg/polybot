import json
from pathlib import Path

from scripts.report_ruling10_abstain_probe import TargetMember, build_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_ruling10_probe_counts_skip_reasons_and_intent_ids(tmp_path: Path) -> None:
    probe_path = tmp_path / "data/research/probe_4013.json"
    _write_json(
        probe_path,
        {
            "candidate_id": "runtime_auto_degrade_40138697bf",
            "generated_at": "2026-07-17T12:00:01Z",
            "candidate_intent_summary": {
                "source_wallet": "0x40138697bf1a0d655593f3be6237d60c1dc7ab35",
                "history_events": 11,
                "candidate_build_events_filtered": 11,
                "fresh_candidate_intents": 0,
                "live_event_prefilter": {
                    "skip_counts": {"market_closed_now": 4, "inventory_late_window_guard": 2},
                    "inventory_window_participation": [
                        {
                            "market_slug": "btc-updown-5m-1",
                            "dominant_skip_reason": "inventory_late_window_guard",
                            "wallet_eligible_orders": 2,
                            "our_submits": 0,
                            "our_fills": 0,
                            "intent_id": "ci_probe",
                            "status": "SKIPPED",
                        }
                    ],
                },
            },
        },
    )
    _write_json(
        tmp_path / "data/research/live_state.json",
        {
            "orders": [
                {
                    "submitted_at": "2026-07-17T11:00:00Z",
                    "source_wallet": "0xf418d3a1a941292f9c8707d62a14980c5beb95a3",
                    "status": "FILLED",
                }
            ]
        },
    )
    _write_json(
        tmp_path / "data/research/guard_state.json",
        {
            "generated_at": "2026-07-17T12:00:02Z",
            "pid": 123,
            "guard_loop_profile": {"cycle_started_at": "2026-07-17T12:00:00Z", "cycle_duration_s": 9.5},
        },
    )
    _write_json(
        tmp_path / "data/research/wallet_copy_daily_scorecard_2026-07-17.json",
        {
            "generated_at": "2026-07-17T12:00:03Z",
            "canonical_pnl_truth": {"by_day": {"2026-07-17": {"fills": 53, "pnl_usd": 24.9}}},
        },
    )

    report = build_report(
        root=tmp_path,
        day="2026-07-17",
        live_state_path="data/research/live_state.json",
        guard_state_path="data/research/guard_state.json",
        targets=(
            TargetMember(
                label="d60c",
                wallet="0x40138697bf1a0d655593f3be6237d60c1dc7ab35",
                candidate_id="runtime_auto_degrade_40138697bf",
                probe_path="data/research/probe_4013.json",
            ),
        ),
    )

    member = report["members"][0]
    assert member["due_for_ruling10"] is True
    assert member["top_abstain_reasons"] == [
        {"reason": "market_closed_now", "count": 4},
        {"reason": "inventory_late_window_guard", "count": 2},
    ]
    assert member["intent_id_report"] == "ci_probe"
    assert member["guard_stamp"]["live_guard_pid"] == 123
    assert report["summary"]["due_labels"] == ["d60c"]


def test_ruling10_probe_classifies_zero_event_member_as_source_quiet(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "data/research/probe_32de.json",
        {
            "candidate_id": "runtime_auto_degrade_32de91fa20",
            "generated_at": "2026-07-17T12:00:01Z",
            "candidate_intent_summary": {
                "source_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
                "history_events": 0,
                "candidate_build_events_filtered": 0,
                "fresh_candidate_intents": 0,
                "live_event_prefilter": {"skip_counts": {}, "inventory_window_participation": []},
            },
        },
    )
    _write_json(
        tmp_path / "data/research/live_state.json",
        {
            "orders": [
                {
                    "submitted_at": "2026-07-17T11:00:00Z",
                    "source_wallet": "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
                    "status": "REJECTED",
                }
            ]
        },
    )
    _write_json(tmp_path / "data/research/guard_state.json", {"generated_at": "2026-07-17T12:00:02Z"})
    _write_json(tmp_path / "data/research/wallet_copy_daily_scorecard_2026-07-17.json", {})

    report = build_report(
        root=tmp_path,
        day="2026-07-17",
        live_state_path="data/research/live_state.json",
        guard_state_path="data/research/guard_state.json",
        targets=(
            TargetMember(
                label="32de",
                wallet="0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
                candidate_id="runtime_auto_degrade_32de91fa20",
                probe_path="data/research/probe_32de.json",
            ),
        ),
    )

    member = report["members"][0]
    assert member["due_for_ruling10"] is True
    assert member["day_live_orders"] == 1
    assert member["day_filled_orders"] == 0
    assert member["top_abstain_reasons"] == [{"reason": "source_quiet_no_candidate_events", "count": 1}]
    assert member["intent_id_report"] is None


def test_ruling10_probe_omits_member_with_day_fill_from_due_list(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "data/research/probe_e88d.json",
        {
            "candidate_id": "market_cohort_alive_41206ad03db1",
            "generated_at": "2026-07-17T12:00:01Z",
            "candidate_intent_summary": {
                "source_wallet": "0xe88db6a8c559410a627a528264a441206ad03db1",
                "history_events": 0,
                "live_event_prefilter": {"skip_counts": {}, "inventory_window_participation": []},
            },
        },
    )
    _write_json(
        tmp_path / "data/research/live_state.json",
        {
            "orders": [
                {
                    "submitted_at": "2026-07-17T10:00:00Z",
                    "source_wallet": "0xe88db6a8c559410a627a528264a441206ad03db1",
                    "status": "FILLED",
                }
            ]
        },
    )
    _write_json(tmp_path / "data/research/guard_state.json", {})
    _write_json(tmp_path / "data/research/wallet_copy_daily_scorecard_2026-07-17.json", {})

    report = build_report(
        root=tmp_path,
        day="2026-07-17",
        live_state_path="data/research/live_state.json",
        guard_state_path="data/research/guard_state.json",
        targets=(
            TargetMember(
                label="e88d",
                wallet="0xe88db6a8c559410a627a528264a441206ad03db1",
                candidate_id="market_cohort_alive_41206ad03db1",
                probe_path="data/research/probe_e88d.json",
            ),
        ),
    )

    assert report["members"][0]["due_for_ruling10"] is False
    assert report["summary"]["due_labels"] == []
