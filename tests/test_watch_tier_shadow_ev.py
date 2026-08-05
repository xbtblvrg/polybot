from __future__ import annotations

from scripts.report_watch_tier_shadow_ev import build_report


def test_watch_tier_shadow_ev_scores_only_gated_inband_buys() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1700000000",
                "condition_id": "0xabc",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0,
                "transaction_hash": "0x1",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1700000300",
                "condition_id": "0xdef",
                "outcome": "Down",
                "price": 0.6,
                "event_ts": 1700000320.0,
                "transaction_hash": "0x2",
            },
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1700000600",
                "condition_id": "0xghi",
                "outcome": "Down",
                "price": 0.4,
                "event_ts": 1700000700.0,
                "transaction_hash": "0x3",
            },
        ]
    }
    resolutions = {
        "0xabc": {
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1700000000",
            "direction": "UP",
            "expiry_unix_ts": 1700000300,
        }
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[wallet],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=0.0,
    )

    row = report["wallets"][0]
    assert report["summary"]["eligible_signals"] == 1
    assert row["resolved_signals"] == 1
    assert row["wins"] == 1
    assert row["pnl_usd"] == 1.5
    assert row["roi_pct"] == 150.0
    assert row["readmission_consideration_eligible"] is True
    assert report["skipped_counts"]["outside_price_band"] == 1
    assert report["skipped_counts"]["entry_offset_gte_max"] == 1


def test_watch_tier_shadow_ev_due_excludes_laned_and_denied_wallets() -> None:
    laned = "0x1111111111111111111111111111111111111111"
    denied = "0x2222222222222222222222222222222222222222"
    pending = "0x3333333333333333333333333333333333333333"
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": f"btc-updown-5m-{1700000000 + idx * 300}",
                "condition_id": f"0x{idx}",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0 + idx * 300,
                "transaction_hash": f"0xhash{idx}",
            }
            for idx, wallet in enumerate([laned, denied, pending])
        ]
    }
    resolutions = {
        f"0x{idx}": {
            "condition_id": f"0x{idx}",
            "market_slug": f"btc-updown-5m-{1700000000 + idx * 300}",
            "direction": "UP",
            "expiry_unix_ts": 1700000300 + idx * 300,
        }
        for idx in range(3)
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[laned, denied, pending],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=0.0,
        ready_shadow_state={"lanes": [{"wallet": laned}]},
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": denied,
                    "ruling": "DENIED",
                    "ruling_id": "test-denial",
                    "resolved_signals": 1,
                    "roi_pct": 150.0,
                    "min_roi_pct": 200.0,
                }
            ]
        },
    )

    rows = {row["source_wallet"]: row for row in report["wallets"]}
    assert report["summary"]["wallets_due"] == [pending]
    assert report["summary"]["readmission_ruling_due"] == 1
    assert report["summary"]["wallets_already_laned"] == [laned]
    assert report["summary"]["wallets_ruled_denied"] == [denied]
    assert rows[laned]["status"] == "READMISSION_ALREADY_LANED"
    assert rows[denied]["status"] == "READMISSION_DENIED"
    assert rows[pending]["status"] == "READMISSION_RULING_DUE"


def test_watch_tier_denial_persists_when_stats_drift_but_roi_stays_below_bar() -> None:
    denied = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "source_wallet": denied,
                "action": "BUY",
                "market_slug": f"btc-updown-5m-{1700000000 + idx * 300}",
                "condition_id": f"0x{idx}",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0 + idx * 300,
                "transaction_hash": f"0xhash{idx}",
            }
            for idx in range(2)
        ]
    }
    resolutions = {
        "0x0": {
            "condition_id": "0x0",
            "market_slug": "btc-updown-5m-1700000000",
            "direction": "DOWN",
            "expiry_unix_ts": 1700000300,
        },
        "0x1": {
            "condition_id": "0x1",
            "market_slug": "btc-updown-5m-1700000300",
            "direction": "DOWN",
            "expiry_unix_ts": 1700000600,
        },
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[denied],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=13.7,
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": denied,
                    "ruling": "DENIED",
                    "ruling_id": "test-denial",
                    "resolved_signals": 1,
                    "roi_pct": -100.0,
                    "min_roi_pct": 13.7,
                }
            ]
        },
    )

    row = report["wallets"][0]
    assert row["resolved_signals"] == 2
    assert row["roi_pct"] == -100.0
    assert row["status"] == "READMISSION_DENIED"
    assert report["summary"]["readmission_ruling_due"] == 0
    assert report["summary"]["wallets_ruled_denied"] == [denied]


def test_watch_tier_denial_reopens_when_roi_crosses_readmission_bar() -> None:
    denied = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "source_wallet": denied,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1700000000",
                "condition_id": "0xabc",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0,
                "transaction_hash": "0xhash",
            }
        ]
    }
    resolutions = {
        "0xabc": {
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1700000000",
            "direction": "UP",
            "expiry_unix_ts": 1700000300,
        }
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[denied],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=13.7,
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": denied,
                    "ruling": "DENIED",
                    "ruling_id": "test-denial",
                    "resolved_signals": 1,
                    "roi_pct": 0.0,
                    "min_roi_pct": 13.7,
                }
            ]
        },
    )

    row = report["wallets"][0]
    assert row["resolved_signals"] == 1
    assert row["roi_pct"] == 150.0
    assert row["status"] == "READMISSION_RULING_DUE"
    assert report["summary"]["readmission_ruling_due"] == 1
    assert report["summary"]["wallets_due"] == [denied]


def test_watch_tier_measurement_only_admission_clears_due_without_lane() -> None:
    wallet = "0x9999999999999999999999999999999999999999"
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": "btc-updown-5m-1700000000",
                "condition_id": "0xabc",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0,
                "transaction_hash": "0xadmit",
            }
        ]
    }
    resolutions = {
        "0xabc": {
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1700000000",
            "direction": "UP",
            "expiry_unix_ts": 1700000300,
        }
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[wallet],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=13.7,
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": wallet,
                    "ruling": "ADMITTED_MEASUREMENT_ONLY",
                    "ruling_id": "test-admit",
                    "resolved_signals": 1,
                    "roi_pct": 150.0,
                    "min_roi_pct": 13.7,
                }
            ]
        },
    )

    row = report["wallets"][0]
    assert row["status"] == "READMISSION_ADMITTED_MEASUREMENT_ONLY"
    assert report["summary"]["readmission_ruling_due"] == 0
    assert report["summary"]["wallets_due"] == []
    assert report["summary"]["wallets_admitted_by_ruling"] == [wallet]


def test_watch_tier_revocation_overrides_already_laned_when_roi_falls_below_bar() -> None:
    wallet = "0x9999999999999999999999999999999999999999"
    history = {
        "events": [
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": f"btc-updown-5m-{1700000000 + idx * 300}",
                "condition_id": f"0x{idx}",
                "outcome": "Up",
                "price": 0.4,
                "event_ts": 1700000020.0 + idx * 300,
                "transaction_hash": f"0xhash{idx}",
            }
            for idx in range(2)
        ]
    }
    resolutions = {
        f"0x{idx}": {
            "condition_id": f"0x{idx}",
            "market_slug": f"btc-updown-5m-{1700000000 + idx * 300}",
            "direction": "DOWN",
            "expiry_unix_ts": 1700000300 + idx * 300,
        }
        for idx in range(2)
    }

    report = build_report(
        history_state=history,
        resolutions=resolutions,
        configured_wallets=[wallet],
        max_entry_offset_s=60.0,
        min_price=0.25,
        max_price=0.50,
        order_usd=1.0,
        min_resolved_signals=1,
        min_roi_pct=13.7,
        ready_shadow_state={"lanes": [{"wallet": wallet}]},
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": wallet,
                    "ruling": "ADMISSION_REVOKED_PRE_LANE",
                    "ruling_id": "test-revoked",
                    "min_roi_pct": 13.7,
                }
            ]
        },
    )

    row = report["wallets"][0]
    assert row["status"] == "ADMISSION_REVOKED_PRE_LANE"
    assert row["readmission_ruling_state"] == "ADMISSION_REVOKED_PRE_LANE"
    assert report["summary"]["admission_revoked_or_suspended_by_ruling"] == 1
    assert report["summary"]["wallets_revoked_or_suspended"] == [wallet]
    assert report["summary"]["wallets_already_laned"] == []
