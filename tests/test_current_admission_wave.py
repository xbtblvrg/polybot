from scripts.report_current_admission_wave import build_wave


def test_current_wave_selects_only_qualified_and_emits_cycle_counters() -> None:
    wallet = "0x" + "a" * 40
    wave = build_wave(
        queue={
            "generated_at": "2026-07-24T03:00:00Z",
            "ranked_members": [
                {
                    "wallet": wallet,
                    "queue_rank": 1,
                    "clearance_ready": True,
                    "ready_for_live": True,
                    "external_liveness_status": "PASS",
                    "fresh_own_source_buy_rows_30m": 7,
                },
                {
                    "wallet": "0x" + "b" * 40,
                    "clearance_ready": False,
                    "ready_for_live": True,
                    "external_liveness_status": "PASS",
                },
            ],
        },
        guard={
            "generated_at": "2026-07-24T03:00:30Z",
            "active_set_runtime": {"members": [{"source_wallet": wallet}]},
        },
        ledger={
            "orders": [
                {
                    "intent_id": "ci_1",
                    "order_id": "order_1",
                    "final_status": "FILLED",
                    "trade_decision": {"wallet_copy": {"source_wallet": wallet}},
                }
            ]
        },
        generated_at="2026-07-24T03:01:00Z",
    )

    assert wave["status"] == "FRESH_OUTPUT_WAVE_ACTIVE"
    assert wave["picked_count"] == 1
    assert wave["runtime_loaded_count"] == 1
    assert wave["own_source_rows_30m"] == 7
    assert wave["intent_count"] == wave["submit_count"] == wave["fill_count"] == 1


def test_current_wave_freshness_deadman_fails_closed() -> None:
    wave = build_wave(
        queue={"generated_at": "2026-07-24T02:00:00Z", "ranked_members": []},
        guard={"generated_at": "2026-07-24T03:00:00Z"},
        ledger={},
        generated_at="2026-07-24T03:01:00Z",
    )

    assert wave["status"] == "STALE_INPUT_FAIL_CLOSED"
    assert wave["input_freshness"]["firing"] is True
    assert wave["input_freshness"]["status"] == "STALE_INPUT_FAIL_CLOSED"
    assert wave["freshness_deadman"]["status"] == "CLEAR"


def test_current_wave_counts_only_orders_created_since_previous_cycle() -> None:
    wallet = "0x" + "a" * 40
    common = {
        "trade_decision": {"wallet_copy": {"source_wallet": wallet}},
        "final_status": "SUBMITTED",
    }
    wave = build_wave(
        queue={
            "generated_at": "2026-07-24T03:01:30Z",
            "ranked_members": [
                {
                    "wallet": wallet,
                    "clearance_ready": True,
                    "ready_for_live": True,
                    "external_liveness_status": "PASS",
                }
            ],
        },
        guard={"generated_at": "2026-07-24T03:01:30Z", "active_set_runtime": {"members": []}},
        ledger={
            "orders": [
                {**common, "intent_id": "old", "submitted_at": "2026-07-24T03:00:30Z"},
                {**common, "intent_id": "new", "submitted_at": "2026-07-24T03:01:30Z"},
            ]
        },
        previous_wave={"generated_at": "2026-07-24T03:01:00Z"},
        generated_at="2026-07-24T03:02:00Z",
    )

    assert wave["intent_count"] == 1
    assert wave["members"][0]["latest_intent_id"] == "new"


def test_current_wave_starts_927f_clock_only_on_first_real_eligible_intent() -> None:
    tracker = {
        "generated_at": "2026-07-24T03:50:00Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "summary": {
            "new_unique_buy_copy_intents": 1,
            "profit_policy": {"policy": {"policy_id": "exact-policy"}},
            "current_poll_diagnostics": {
                "current_poll_ladder": {
                    "raw_rows": 4,
                    "profit_policy_buy_rows": 2,
                    "paper_orders": 1,
                }
            },
        },
        "config": {"max_copyability_event_age_s": 10.0},
        "last_moves": [
            {
                "copyability": {"accepted": True},
                "wallet_event": {
                    "event_id": "event-927f",
                    "source_fingerprint": "fingerprint-927f",
                    "transaction_hash": "tx-927f",
                    "condition_id": "condition-927f",
                    "token_id": "token-927f",
                    "action": "BUY",
                    "event_ts": 1784866703.0,
                    "observed_ts": 1784866704.0,
                    "source": "rtds_activity",
                },
                "copy_efficiency": {
                    "profit_policy_accepted": True,
                    "intent_id": "intent-927f",
                    "event_age_s": 1.0,
                    "policy_id": "exact-policy",
                },
            }
        ],
    }
    wave = build_wave(
        queue={"generated_at": "2026-07-24T03:50:00Z", "ranked_members": []},
        guard={"generated_at": "2026-07-24T03:50:00Z"},
        ledger={},
        forward_927f=tracker,
        generated_at="2026-07-24T03:50:01Z",
    )
    forward = wave["forward_927f"]
    assert forward["clock_start"] == "2026-07-24T03:50:01Z"
    assert forward["funnel"]["raw_rows"] == 4
    assert forward["funnel"]["eligible_copyintents"] == 1
    assert forward["funnel"]["would_submit"] == 1

    preserved = build_wave(
        queue={"generated_at": "2026-07-24T03:51:00Z", "ranked_members": []},
        guard={"generated_at": "2026-07-24T03:51:00Z"},
        ledger={},
        previous_wave=wave,
        forward_927f={"summary": {"new_unique_buy_copy_intents": 0}},
        generated_at="2026-07-24T03:51:01Z",
    )
    assert preserved["forward_927f"]["clock_start"] == "2026-07-24T03:50:01Z"


def test_two_forward_seat_clocks_and_bindings_are_independent() -> None:
    eligible_move = {
        "copyability": {"accepted": True},
        "wallet_event": {
            "event_id": "a689-event",
            "source_fingerprint": "a689-fingerprint",
            "transaction_hash": "a689-tx",
            "condition_id": "a689-condition",
            "token_id": "a689-token",
            "action": "BUY",
            "event_ts": 1784866800.0,
            "observed_ts": 1784866801.0,
            "source": "rtds_activity",
        },
        "copy_efficiency": {
            "profit_policy_accepted": True,
            "intent_id": "a689-intent",
            "source_event_id": "a689-event",
            "event_age_s": 1.0,
            "copyability_details": {"wallet_data_api_source": "activity:user"},
        },
    }
    a689 = {
        "generated_at": "2026-07-24T04:20:00Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "last_moves": [eligible_move],
        "summary": {
            "new_unique_buy_copy_intents": 1,
            "profit_policy": {"policy": {"policy_id": "exact-policy"}},
            "current_poll_diagnostics": {"current_poll_ladder": {"raw_rows": 3, "paper_orders": 1}},
        },
    }
    wave = build_wave(
        queue={"generated_at": "2026-07-24T04:20:00Z", "ranked_members": []},
        guard={"generated_at": "2026-07-24T04:20:00Z"},
        ledger={},
        forward_927f={"summary": {"new_unique_buy_copy_intents": 0}},
        forward_a689=a689,
        generated_at="2026-07-24T04:20:01Z",
    )
    assert wave["forward_seats"]["927f"]["clock_start"] is None
    assert wave["forward_seats"]["a689"]["clock_start"] == "2026-07-24T04:20:01Z"
    assert wave["forward_seats"]["a689"]["source_binding"]["source_event_id"] == "a689-event"


def test_forward_clock_fails_closed_on_unbound_eligible_counter() -> None:
    wave = build_wave(
        queue={"generated_at": "2026-07-24T04:20:00Z", "ranked_members": []},
        guard={"generated_at": "2026-07-24T04:20:00Z"},
        ledger={},
        forward_927f={"summary": {"new_unique_buy_copy_intents": 7}},
        generated_at="2026-07-24T04:20:01Z",
    )

    seat = wave["forward_seats"]["927f"]
    assert seat["clock_start"] is None
    assert seat["source_binding"] is None
    assert seat["clock_binding_status"] == "FAIL_CLOSED_NO_EXPLICIT_ACCEPTED_EVENT"
