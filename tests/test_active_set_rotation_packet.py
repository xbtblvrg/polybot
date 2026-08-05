from __future__ import annotations

from scripts.report_active_set_rotation_packet import build_packet


F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
A689 = "0xa6896d11f76dfa2820662c1f441496f51553559b"
C539 = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"
SELECTED = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"


def _event(wallet: str, event_ts: float, observed_ts: float) -> dict:
    return {
        "source_wallet": wallet,
        "event_ts": event_ts,
        "observed_ts": observed_ts,
        "action": "BUY",
        "asset": "BTC",
        "duration": "5m",
        "market_slug": f"btc-updown-5m-{int(event_ts // 300) * 300}",
    }


def test_active_set_rotation_packet_ranks_by_fresh_flow_then_latency_then_edge() -> None:
    guard = {
        "active_set_runtime": {
            "selected_member": {
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "source_wallet": SELECTED,
            },
            "members": [
                {"candidate_id": "c539", "source_wallet": C539, "policy_id": "p-c539"},
                {"candidate_id": "f418", "source_wallet": F418, "policy_id": "p-f418"},
                {"candidate_id": "a689", "source_wallet": A689, "policy_id": "p-a689"},
                {"candidate_id": "selected", "source_wallet": SELECTED, "policy_id": "p-selected"},
            ],
        },
        "active_set_rtds_premerge": {
            "rows": [
                {"source_wallet": SELECTED, "new_matching_events": 0, "latest_event_ts": 1_700_000_100.0},
                {"source_wallet": F418, "new_matching_events": 3, "retained_matching_rows": 8, "rtds_catchup_lag_s": 2.0},
                {"source_wallet": A689, "new_matching_events": 1, "retained_matching_rows": 3, "rtds_catchup_lag_s": 0.5},
                {"source_wallet": C539, "new_matching_events": 0, "retained_matching_rows": 2, "rtds_catchup_lag_s": 0.1},
            ]
        },
    }
    history = {
        "events": [
            *[_event(F418, 1_700_000_000.0 + idx, 1_700_000_001.0 + idx) for idx in range(4)],
            *[_event(A689, 1_700_000_010.0 + idx, 1_700_000_010.2 + idx) for idx in range(2)],
            _event(C539, 1_700_000_020.0, 1_700_000_020.1),
            _event(SELECTED, 1_700_000_100.0, 1_700_000_100.5),
        ]
    }
    routing = {
        "summary": {
            "fee_gate_calibration_retained": {
                "by_member": {
                    F418: {"post_fee_pnl_usd": -2.0, "measured_unique_windows": 10},
                    A689: {"post_fee_pnl_usd": 5.0, "measured_unique_windows": 4},
                }
            }
        }
    }

    packet = build_packet(
        guard=guard,
        history=history,
        routing=routing,
        ready_shadow={"lanes": [{ "wallet": C539, "paper_pnl_usd": 1.25 }]},
        candidate_wallets=[F418, A689, C539],
        now_iso="2023-11-14T22:20:00Z",
        quiet_anchor_ts=1_700_000_000.0,
    )

    assert packet["status"] == "PRESTAGED_NO_LIVE_CHANGE"
    assert packet["live_path_mutated"] is False
    assert packet["presumptive_target"] == F418
    assert [row["source_wallet"] for row in packet["ranked_candidates"]] == [F418, A689, C539]
    assert packet["ranked_candidates"][0]["fresh_matching_events_4h"] == 4
    assert packet["ranked_candidates"][2]["routing_shadow_edge_status"] == "MISSING_ROUTING_SHADOW_RETAINED"
    assert packet["quiet_clock"]["anchor_ts"] == 1_700_000_000.0
    assert packet["quiet_clock"]["fires_now"] is False
    assert packet["selected_premerge_new_matching_events"] == 0


def test_active_set_rotation_packet_does_not_reset_on_selected_chatter() -> None:
    guard = {
        "active_set_runtime": {
            "selected_member": {
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "source_wallet": SELECTED,
            },
            "members": [
                {"candidate_id": "f418", "source_wallet": F418, "policy_id": "p-f418"},
                {"candidate_id": "selected", "source_wallet": SELECTED, "policy_id": "p-selected"},
            ],
        },
        "active_set_rtds_premerge": {
            "rows": [
                {"source_wallet": SELECTED, "new_matching_events": 1, "latest_event_ts": 1_700_000_500.0},
                {"source_wallet": F418, "new_matching_events": 3, "retained_matching_rows": 8},
            ]
        },
    }
    history = {"events": [_event(SELECTED, 1_700_000_500.0, 1_700_000_500.4), _event(F418, 1_700_000_100.0, 1_700_000_101.0)]}

    first = build_packet(
        guard=guard,
        history=history,
        routing={},
        ready_shadow={},
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:25:00Z",
        quiet_anchor_ts=1_700_000_000.0,
    )

    guard["active_set_rtds_premerge"]["rows"][0]["new_matching_events"] = 0
    second = build_packet(
        guard=guard,
        history=history,
        routing={},
        ready_shadow={},
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:26:00Z",
        quiet_anchor_ts=1_700_000_000.0,
        prior_packet=first,
    )

    assert first["selected_premerge_new_matching_events"] == 1
    assert first["quiet_clock"]["ignored_selected_premerge_new_matching_events"] == 1
    assert first["quiet_clock"]["anchor_ts"] == 1_700_000_000.0
    assert first["quiet_clock"]["selected_submit_attempt_ts"] is None
    assert second["selected_premerge_new_matching_events"] == 0
    assert second["quiet_clock"]["anchor_ts"] == 1_700_000_000.0
    assert second["quiet_clock"]["earliest_fire_ts"] == 1_700_014_400.0


def test_active_set_rotation_packet_uses_selected_submit_attempt_anchor() -> None:
    guard = {
        "active_set_runtime": {
            "selected_member": {
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "source_wallet": SELECTED,
            },
            "members": [
                {"candidate_id": "f418", "source_wallet": F418, "policy_id": "p-f418"},
                {"candidate_id": "selected", "source_wallet": SELECTED, "policy_id": "p-selected"},
            ],
        },
        "active_set_rtds_premerge": {
            "rows": [
                {"source_wallet": SELECTED, "new_matching_events": 3, "latest_event_ts": 1_700_000_700.0},
                {"source_wallet": F418, "new_matching_events": 3, "retained_matching_rows": 8},
            ]
        },
    }
    live_execution = {
        "orders": [
            {
                "source_wallet": SELECTED,
                "status": "submitted",
                "final_status": "FILLED",
                "submitted_at": "2023-11-14T22:21:40Z",
                "updated_at": "2023-11-14T22:21:41Z",
                "order_id": "0xabc",
                "intent_id": "ci_selected",
            },
            {
                "source_wallet": F418,
                "status": "submitted",
                "updated_at": "2023-11-14T22:30:00Z",
            },
        ]
    }

    first = build_packet(
        guard=guard,
        history={"events": [_event(SELECTED, 1_700_000_700.0, 1_700_000_700.2)]},
        routing={},
        ready_shadow={},
        live_execution=live_execution,
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:35:00Z",
        quiet_anchor_ts=1_700_000_000.0,
    )
    second = build_packet(
        guard=guard,
        history={"events": []},
        routing={},
        ready_shadow={},
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:36:00Z",
        quiet_anchor_ts=1_700_000_000.0,
        prior_packet=first,
    )

    assert first["quiet_clock"]["anchor_ts"] == 1_700_000_501.0
    assert first["quiet_clock"]["anchor_source"] == "selected_submit_attempt"
    assert first["quiet_clock"]["selected_submit_attempt"]["order_id"] == "0xabc"
    assert second["quiet_clock"]["anchor_ts"] == 1_700_000_501.0
    assert second["quiet_clock"]["anchor_source"] == "prior_same_selected_submit_predicate_anchor"
    assert second["quiet_clock"]["selected_submit_attempt"]["order_id"] == "0xabc"
    assert second["quiet_clock"]["earliest_fire_ts"] == 1_700_014_901.0


def test_active_set_rotation_packet_rejects_contaminated_prior_anchor_without_submit_evidence() -> None:
    guard = {
        "active_set_runtime": {
            "selected_member": {
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "source_wallet": SELECTED,
            },
            "members": [
                {"candidate_id": "f418", "source_wallet": F418, "policy_id": "p-f418"},
                {"candidate_id": "selected", "source_wallet": SELECTED, "policy_id": "p-selected"},
            ],
        },
        "active_set_rtds_premerge": {
            "rows": [
                {"source_wallet": SELECTED, "new_matching_events": 5, "latest_event_ts": 1_700_000_900.0},
                {"source_wallet": F418, "new_matching_events": 3, "retained_matching_rows": 8},
            ]
        },
    }
    live_execution = {
        "orders": [
            {
                "source_wallet": SELECTED,
                "status": "submitted",
                "submitted_at": "2023-11-14T22:21:40Z",
                "updated_at": "2023-11-14T22:21:41Z",
                "order_id": "0xabc",
            }
        ]
    }
    contaminated_prior = {
        "selected_wallet": SELECTED,
        "quiet_clock": {
            "reset_predicate": "selected_submit_eligible_copyintent",
            "anchor_floor_ts": 1_700_000_000.0,
            "anchor_ts": 1_700_000_900.0,
            "selected_submit_attempt_ts": None,
            "selected_submit_attempt": None,
        },
    }

    packet = build_packet(
        guard=guard,
        history={"events": [_event(SELECTED, 1_700_000_900.0, 1_700_000_900.2)]},
        routing={},
        ready_shadow={},
        live_execution=live_execution,
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:35:00Z",
        quiet_anchor_ts=1_700_000_000.0,
        prior_packet=contaminated_prior,
    )

    assert packet["quiet_clock"]["anchor_ts"] == 1_700_000_501.0
    assert packet["quiet_clock"]["anchor_source"] == "selected_submit_attempt"
    assert packet["quiet_clock"]["selected_submit_attempt"]["order_id"] == "0xabc"


def test_active_set_rotation_packet_prefers_live_execution_current_member_over_stale_guard_selection() -> None:
    guard = {
        "active_set_runtime": {
            "selected_member": {
                "candidate_id": "runtime_auto_degrade_32de91fa20",
                "source_wallet": SELECTED,
            },
            "members": [
                {"candidate_id": "stale-selected", "source_wallet": SELECTED, "policy_id": "p-selected"},
            ],
        },
        "active_set_rtds_premerge": {
            "rows": [
                {"source_wallet": SELECTED, "new_matching_events": 5, "latest_event_ts": 1_700_000_900.0},
                {"source_wallet": F418, "new_matching_events": 3, "retained_matching_rows": 8},
            ]
        },
    }
    live_execution = {
        "runtime_permission": {
            "details": {
                "active_set": {
                    "members": [
                        {
                            "candidate_id": "runtime_auto_degrade_f418d3a1a9",
                            "is_current_cycle_member": True,
                            "source_wallet": F418,
                            "policy_id": "p-f418",
                        }
                    ]
                }
            }
        },
        "orders": [
            {
                "source_wallet": F418,
                "status": "submitted",
                "final_status": "FILLED",
                "submitted_at": "2023-11-14T22:31:40Z",
                "order_id": "0xf418",
            },
            {
                "source_wallet": SELECTED,
                "status": "submitted",
                "final_status": "FILLED",
                "submitted_at": "2023-11-14T22:21:40Z",
                "order_id": "0x32de",
            },
        ],
    }
    prior_packet = {
        "selected_wallet": SELECTED,
        "quiet_clock": {
            "reset_predicate": "selected_submit_eligible_copyintent",
            "anchor_floor_ts": 1_700_000_000.0,
            "anchor_ts": 1_700_000_900.0,
            "selected_submit_attempt_ts": 1_700_000_500.0,
        },
    }

    packet = build_packet(
        guard=guard,
        history={"events": [_event(F418, 1_700_000_700.0, 1_700_000_700.2)]},
        routing={},
        ready_shadow={},
        live_execution=live_execution,
        candidate_wallets=[F418],
        now_iso="2023-11-14T22:35:00Z",
        quiet_anchor_ts=1_700_000_000.0,
        prior_packet=prior_packet,
    )

    assert packet["selected_wallet"] == F418
    assert packet["selected_candidate_id"] == "runtime_auto_degrade_f418d3a1a9"
    assert packet["quiet_clock"]["anchor_source"] == "selected_submit_attempt"
    assert packet["quiet_clock"]["selected_submit_attempt"]["order_id"] == "0xf418"
    assert packet["quiet_clock"]["anchor_ts"] == 1_700_001_100.0
