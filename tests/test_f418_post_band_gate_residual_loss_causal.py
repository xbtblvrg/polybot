from scripts.report_f418_post_band_gate_residual_loss_causal import build_report


def _resolution(slug: str, winner: str) -> dict:
    return {"market_slug": slug, "winning_outcome": winner}


def test_causal_shadow_separates_accepted_and_denied_and_builds_cells():
    accepted_slug = "btc-updown-5m-1784843100"
    denied_slug = "btc-updown-5m-1784843400"
    events = [
        {
            "event": "wallet_copy_live_lifecycle",
            "status": "LIVE_FILLED",
            "intent_id": "ci_fill",
            "ts": "2026-07-23T21:45:55Z",
            "payload": {
                "execution_role": "taker",
                "response_fill_price": 0.21,
                "response_fill_size_shares": 5.0,
                "response_filled_size_usd": 1.05,
                "wallet_copy_execute_live_profile": {
                    "market_slug": accepted_slug,
                    "condition_id": "0xaccepted",
                    "outcome": "Down",
                },
                "wallet_copy_latency_budget": {
                    "source_fill_block_ts": 1784843149.0,
                    "intent_built_ts": 1784843153.0,
                },
            },
        },
        {
            "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
            "intent_id": "ci_deny",
            "ts": "2026-07-23T21:50:17Z",
            "market_slug": denied_slug,
            "condition_id": "0xdenied",
            "outcome": "Up",
            "limit_price": 0.5,
            "shares": 2.0,
            "copy_size_usd": 1.0,
            "expected_fee_usd": 0.035,
        },
    ]
    metadata = [
        {
            "generated_at": "2026-07-23T21:50:18Z",
            "sample": {
                "intent_id": "ci_deny",
                "event_age_s": 8.7,
                "event_ts": 1784843408.0,
                "market_slug": denied_slug,
            },
        }
    ]
    report = build_report(
        event_rows=events,
        resolution_rows=[_resolution(accepted_slug, "down"), _resolution(denied_slug, "down")],
        metadata_rows=metadata,
        generated_at="2026-07-23T22:00:00Z",
        min_resolved_windows=2,
    )

    assert report["status"] == "HOLDOUT_READY"
    assert report["gate"]["resolved_accepted_or_denied_windows"] == 2
    assert report["cohorts"]["accepted_live"]["wins"] == 1
    assert report["cohorts"]["denied_counterfactual"]["losses"] == 1
    denied = next(row for row in report["rows"] if row["cohort"] == "denied_counterfactual")
    assert denied["price_cell"] == "0.50_0.60"
    assert denied["source_age_cell"] == "5_10"
    assert denied["window_offset_cell"] == "lt_30"
    assert denied["fak_outcome_cell"] == "DENIED_BEFORE_FAK"


def test_causal_shadow_ignores_pre_activation_and_maker_fallback_duplicate():
    slug = "btc-updown-5m-1784843100"
    base = {
        "event": "wallet_copy_live_lifecycle",
        "intent_id": "ci_same",
        "payload": {
            "response_fill_price": 0.3,
            "response_fill_size_shares": 3.0,
            "response_filled_size_usd": 0.9,
            "wallet_copy_execute_live_profile": {
                "market_slug": slug,
                "condition_id": "0xcondition",
                "outcome": "Up",
            },
        },
    }
    pre = {**base, "status": "LIVE_FILLED", "ts": "2026-07-23T21:41:59Z"}
    maker = {
        **base,
        "status": "LIVE_REJECTED",
        "ts": "2026-07-23T21:45:00Z",
        "payload": {**base["payload"], "execution_role": "maker"},
    }
    report = build_report(
        event_rows=[pre, maker],
        resolution_rows=[_resolution(slug, "up")],
        generated_at="2026-07-23T22:00:00Z",
    )

    assert report["status"] == "ACCRUING"
    assert report["rows"] == []
    assert report["cohorts"]["accepted_live"]["resolved_rows"] == 0
