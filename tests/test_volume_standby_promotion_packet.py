from scripts.report_volume_standby_promotion_packet import build_packet


def test_packet_preserves_exact_policy_and_fails_closed_on_liveness() -> None:
    wallet = "0x13e0"
    packet = build_packet(
        {
            "generated_at": "2026-07-22T19:00:00Z",
            "lanes": [
                {
                    "wallet": wallet,
                    "paper_orders": 596,
                    "copyable_buy_events": 279,
                    "resolved_paper_fills": 276,
                    "paper_pnl_usd": 177.809634,
                    "paper_policy_id": "exact",
                    "copyintent_parity_capture": {"policy_id": "exact", "live_submit_disabled": True},
                    "source_liveness": {"status": "STALE"},
                    "live_canary_packet_preconditions": {
                        "fresh_external_btc5m_lt_24h": False,
                        "ready_shadow_full_utc_day": True,
                    },
                }
            ],
        },
        wallet=wallet,
    )

    assert packet["exact_policy"]["policy_match"] is True
    assert packet["funnel"]["copyable_buy_events"] == 279
    assert packet["funnel"]["resolved_paper_fills"] == 276
    assert packet["ev"]["resolved_paper_pnl_usd"] == 177.809634
    assert packet["ev"]["roi_pct"] is None
    assert packet["promotion_gate"]["status"] == "HARD_FAIL"
    assert packet["promotion_gate"]["decision"] == "NO_LIVE_PROMOTION"
    assert packet["live_mutation_allowed"] is False


def _due_state(*, preconditions: dict[str, bool]) -> dict:
    return {
        "generated_at": "2026-07-24T01:39:00Z",
        "lanes": [
            {
                "wallet": "0x13e0",
                "paper_orders": 596,
                "copyable_buy_events": 279,
                "resolved_paper_fills": 276,
                "paper_pnl_usd": 177.809634,
                "paper_policy_id": "exact",
                "copyintent_parity_capture": {"policy_id": "exact", "live_submit_disabled": True},
                "copyintent_parity_capture_armed": True,
                "paper_canary_enrolled_at": "2026-07-21T01:38:33Z",
                "ready_shadow_full_utc_day": True,
                "promotion_copyable_buy_gate": 20,
                "live_canary_packet_preconditions": preconditions,
                "live_canary_precondition_evidence": {
                    "fading": {"generated_at": "2026-07-24T01:38:50Z", "classification": "CONTINUOUS"},
                    "external_liveness": {"probe_observed_at": "2026-07-24T01:38:50Z"},
                    "defense": {
                        "scorecard_generated_at": "2026-07-24T01:38:50Z",
                        "utc_release_due": True,
                    },
                },
            }
        ],
    }


def _clearance() -> dict:
    return {
        "wallet": "0x13e0",
        "exact_policy": {"policy_id": "exact"},
        "exact_policy_post_fee_shadow": {
            "pre_fee_pnl_usd": 177.809634,
            "post_fee_pnl_usd": 167.329126,
            "expected_fee_usd": 10.480508,
            "resolved_orders": 276,
        },
    }


def test_volume_clock_prederives_fable_branch_but_never_promotes_early() -> None:
    packet = build_packet(
        _due_state(preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": True, "fading_clear": True, "defense_not_in_triggered_rung": True}),
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-23T02:00:00Z",
    )

    assert packet["decision_clock"]["due"] is False
    assert packet["decision"] == "FRESH_EXPIRY_READ_REQUIRED"
    assert packet["prederived_decision_branches"]["current_branch"] == "ACCRUE_PAPER_ONLY_NO_LIVE_MUTATION"
    assert packet["live_mutation_allowed"] is False


def test_due_volume_clock_requires_fresh_fable_decision_when_all_inputs_pass() -> None:
    packet = build_packet(
        _due_state(preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": True, "fading_clear": True, "defense_not_in_triggered_rung": True}),
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-24T01:39:00Z",
    )

    assert packet["decision_clock"]["due"] is True
    assert packet["divergence_review"]["status"] == "CLEAR"
    assert packet["fee_aware_economics"]["post_fee_positive"] is True
    assert packet["decision"] == "FABLE_PROMOTION_DECISION_REQUIRED"
    assert packet["evidence_gate_pass"] is True


def test_due_volume_clock_parks_when_any_mechanical_input_fails() -> None:
    packet = build_packet(
        _due_state(preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": False, "fading_clear": True, "defense_not_in_triggered_rung": True}),
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-24T01:39:00Z",
    )

    assert packet["decision"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    assert packet["prederived_decision_branches"]["current_branch"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    assert packet["evidence_gate_pass"] is False


def test_due_volume_clock_defers_instead_of_false_parking_on_stale_inputs() -> None:
    state = _due_state(
        preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": False, "fading_clear": False, "defense_not_in_triggered_rung": True}
    )
    state["lanes"][0]["live_canary_precondition_evidence"]["fading"]["generated_at"] = "2026-07-23T01:00:00Z"
    packet = build_packet(
        state,
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-24T01:39:00Z",
    )

    assert packet["precondition_input_freshness"]["status"] == "STALE_OR_MISSING"
    assert packet["decision"] == "FRESH_PRECONDITION_INPUTS_REQUIRED"
    assert packet["prederived_decision_branches"]["current_branch"] == "DEFER_VOLUME_DECISION_FRESH_INPUT_REQUIRED"
    assert packet["evidence_gate_pass"] is False


def test_due_volume_clock_defers_when_fresh_temporal_artifact_omits_wallet() -> None:
    state = _due_state(
        preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": True, "fading_clear": False, "defense_not_in_triggered_rung": True}
    )
    state["lanes"][0]["live_canary_precondition_evidence"]["fading"]["classification"] = None
    packet = build_packet(
        state,
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-24T01:39:00Z",
    )

    freshness = packet["precondition_input_freshness"]
    assert freshness["ages_h"]["temporal_profitability"] < 1.0
    assert freshness["checks"]["temporal_wallet_classification_present"] is False
    assert packet["decision"] == "FRESH_PRECONDITION_INPUTS_REQUIRED"
    assert packet["prederived_decision_branches"]["current_branch"] == "DEFER_VOLUME_DECISION_FRESH_INPUT_REQUIRED"


def test_due_volume_clock_genuine_fading_classification_hard_fails_to_park() -> None:
    state = _due_state(
        preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": True, "fading_clear": False, "defense_not_in_triggered_rung": True}
    )
    state["lanes"][0]["live_canary_precondition_evidence"]["fading"]["classification"] = "FADING"
    packet = build_packet(
        state,
        wallet="0x13e0",
        clearance_packet=_clearance(),
        generated_at="2026-07-24T01:39:00Z",
    )

    assert packet["precondition_input_freshness"]["status"] == "PASS"
    assert packet["decision"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"


def test_terminal_volume_park_persists_across_fresh_all_pass_refresh() -> None:
    packet = build_packet(
        _due_state(preconditions={"hot_standby_ready": True, "fresh_external_btc5m_lt_24h": True, "fading_clear": True, "defense_not_in_triggered_rung": True}),
        wallet="0x13e0",
        clearance_packet=_clearance(),
        previous_packet={"terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY"},
        generated_at="2026-07-24T01:39:00Z",
    )

    assert packet["evidence_gate_pass"] is True
    assert packet["decision"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    assert packet["terminal_decision"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    assert packet["terminal_decision_monotone"] is True
    assert packet["prederived_decision_branches"]["current_branch"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
