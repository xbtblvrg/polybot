from scripts import report_focused_candidate_p1_packet as report


WALLET = "0x8a47951a3cefcc98dc8b41eb438d1c49249872ef"


def test_focused_packet_fails_sign_flip_and_concentration() -> None:
    packets = {"packets": [{"wallet": WALLET, "paper_pnl_usd": 4.0, "resolved_copyable_events": 1}]}
    history = {
        "replay": {"stop_reason": "lookback_cutoff_reached", "raw_rows_seen": 3},
        "events": [
            {"action": "BUY", "transaction_hash": "a", "market_slug": "m1", "outcome": "UP", "price": 0.5, "size": 2, "event_ts": 1},
            {"action": "BUY", "transaction_hash": "b", "market_slug": "m2", "outcome": "DOWN", "price": 0.5, "size": 4, "event_ts": 2},
            {"action": "BUY", "transaction_hash": "c", "market_slug": "m3", "outcome": "UP", "price": 0.5, "size": 2, "event_ts": 3},
        ],
    }
    temporal = {
        "wallets": [{
            "wallet": WALLET,
            "classification": "BAND-SPECIALIST",
            "slice_labels": {"all": {"label": "PROVEN-NEGATIVE"}},
            "profitable_hour_bands": ["weekend:22"],
            "regime_hour_profiles": {"weekend": {"22": {"roi_pct": 10.0}}},
        }]
    }
    live = {"orders": [{"final_status": "FILLED", "source_wallet": WALLET, "submitted_at": "2026-07-18T22:00:00Z", "source_intent": {"policy_id": "p"}}]}
    result = report.build_report(
        wallet=WALLET,
        packets=packets,
        history=history,
        temporal=temporal,
        live_ledger=live,
        winners={"m1": "UP", "m2": "UP", "m3": "UP"},
    )
    assert result["history_depth"]["status"] == "COMPLETE_TO_PREREGISTERED_LOOKBACK"
    assert result["old_vs_new"]["new_deep_pnl_usd"] == 0.0
    assert result["old_vs_new"]["sign_flipped"] is True
    assert result["temporal_hour_match"]["status"] == "FAIL"
    assert result["concentration"]["top1_positive_pnl_share_pct"] == 50.0
    assert result["decision"]["rotation_eligible"] is False


def test_runtime_lane_uses_latest_filled_wallet() -> None:
    live = {"orders": [
        {"final_status": "FILLED", "source_wallet": "0x" + "1" * 40, "submitted_at": "2026-07-18T20:00:00Z"},
        {"final_status": "FILLED", "source_wallet": "0x" + "2" * 40, "submitted_at": "2026-07-19T22:00:00Z", "source_intent": {"policy_id": "latest"}},
    ]}
    runtime = report._runtime_lane_hours(live)
    assert runtime["wallet"] == "0x" + "2" * 40
    assert runtime["policy_id"] == "latest"
    assert runtime["cells"] == {"weekend:22": 1}


def test_passing_packet_is_pending_fable_audit() -> None:
    packets = {"packets": [{"wallet": WALLET, "paper_pnl_usd": 0.5, "resolved_copyable_events": 1}]}
    history = {
        "replay": {"stop_reason": "short_page"},
        "events": [
            {"action": "BUY", "transaction_hash": "a", "market_slug": "m1", "outcome": "UP", "price": 0.5, "size": 2, "event_ts": 1},
            {"action": "BUY", "transaction_hash": "b", "market_slug": "m2", "outcome": "UP", "price": 0.5, "size": 2, "event_ts": 2},
            {"action": "BUY", "transaction_hash": "c", "market_slug": "m3", "outcome": "UP", "price": 0.5, "size": 2, "event_ts": 3},
        ],
    }
    temporal = {"wallets": [{
        "wallet": WALLET,
        "classification": "CONTINUOUS",
        "slice_labels": {"all": {"label": "PROVEN-POSITIVE"}},
        "profitable_hour_bands": ["weekend:22"],
        "regime_hour_profiles": {"weekend": {"22": {"roi_pct": 10.0}}},
    }]}
    live = {"orders": [{"final_status": "FILLED", "source_wallet": WALLET, "submitted_at": "2026-07-18T22:00:00Z"}]}
    result = report.build_report(
        wallet=WALLET,
        packets=packets,
        history=history,
        temporal=temporal,
        live_ledger=live,
        winners={"m1": "UP", "m2": "UP", "m3": "UP"},
    )
    assert result["decision"]["verdict"] == "PROBE_READY_PENDING_FABLE_AUDIT"
    assert result["decision"]["live_mutation"] is False
