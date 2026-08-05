from scripts.report_shadow_arming import build_shadow_summary


def _polygon_row(tx: str, *, wallet: str, received_at_s: float, block_ts: float) -> dict:
    return {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "transaction_hash": tx,
        "received_at_s": received_at_s,
        "block_ts": block_ts,
        "selected_wallet": wallet,
        "registry_wallets": [wallet],
        "decoded": {"side": "BUY"},
    }


def _dataapi_row(tx: str, *, wallet: str, captured_at_s: float, started_at_s: float = 90.0) -> dict:
    return {
        "event": "dataapi_first_seen",
        "wallet": wallet,
        "transactionHash": tx,
        "captured_at_s": captured_at_s,
        "poller_started_at_s": started_at_s,
        "poll_interval_s": 5.0,
        "backfill": False,
        "side": "BUY",
    }


def test_shadow_arming_reports_measurement_only_lead_and_gate() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    summary = build_shadow_summary(
        [_polygon_row("0xabc", wallet=wallet, received_at_s=101.0, block_ts=100.0)],
        [_dataapi_row("0xabc", wallet=wallet, captured_at_s=120.0)],
        sample_limit=10,
        decision_overheads_s=[0.0],
    )
    variant = summary["variants"]["overhead_0_0s"]

    assert summary["copyintents_created"] == 0
    assert summary["orders_submitted"] == 0
    assert summary["matched_count"] == 1
    assert variant["arming_advantage"]["p50_s"] == 19.0
    assert variant["ws_freshness_pass_count"] == 1
    assert variant["dataapi_freshness_pass_count"] == 1
    assert variant["ws_2s_budget_pass_count"] == 1
    assert variant["dataapi_2s_budget_pass_count"] == 0


def test_shadow_arming_excludes_backfill_from_acceptance() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    summary = build_shadow_summary(
        [_polygon_row("0xdef", wallet=wallet, received_at_s=91.0, block_ts=91.0)],
        [_dataapi_row("0xdef", wallet=wallet, captured_at_s=92.0, started_at_s=90.0)],
        sample_limit=10,
        decision_overheads_s=[0.0],
    )

    assert summary["matched_count"] == 0
    assert summary["backfill_excluded_count"] == 1


def test_shadow_arming_uses_earliest_ws_detection_for_duplicate_tx() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    summary = build_shadow_summary(
        [
            _polygon_row("0xbeef", wallet=wallet, received_at_s=130.0, block_ts=120.0),
            _polygon_row("0xbeef", wallet=wallet, received_at_s=121.0, block_ts=120.0),
        ],
        [_dataapi_row("0xbeef", wallet=wallet, captured_at_s=150.0)],
        sample_limit=10,
        decision_overheads_s=[0.0],
    )
    variant = summary["variants"]["overhead_0_0s"]

    assert summary["matched_count"] == 1
    assert variant["sample_rows"][0]["ws_armable_ts"] == 121.0
    assert variant["arming_advantage"]["p50_s"] == 29.0


def test_shadow_arming_decision_overhead_shifts_age_and_advantage() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    summary = build_shadow_summary(
        [_polygon_row("0xcafe", wallet=wallet, received_at_s=101.0, block_ts=100.0)],
        [_dataapi_row("0xcafe", wallet=wallet, captured_at_s=120.0)],
        sample_limit=10,
        decision_overheads_s=[0.25],
    )
    variant = summary["variants"]["overhead_0_25s"]

    assert variant["ws_age_at_arm"]["p50_s"] == 1.25
    assert variant["arming_advantage"]["p50_s"] == 18.75


def test_shadow_arming_uses_mission_30s_freshness_boundary() -> None:
    wallet = "0x5555555555555555555555555555555555555555"
    summary = build_shadow_summary(
        [
            _polygon_row("0xpass", wallet=wallet, received_at_s=129.9, block_ts=100.0),
            _polygon_row("0xfail", wallet=wallet, received_at_s=130.1, block_ts=100.0),
        ],
        [
            _dataapi_row("0xpass", wallet=wallet, captured_at_s=131.0),
            _dataapi_row("0xfail", wallet=wallet, captured_at_s=132.0),
        ],
        sample_limit=10,
        decision_overheads_s=[0.0],
    )
    variant = summary["variants"]["overhead_0_0s"]

    assert summary["max_event_age_s"] == 30.0
    assert variant["ws_freshness_pass_count"] == 1
    assert variant["dataapi_freshness_pass_count"] == 0
