from __future__ import annotations

from scripts.build_non_active_wallet_signal_census import build_report


def _rtds_row(wallet: str, start: int, offset: int, *, side: str = "BUY", price: float = 0.4) -> dict:
    return {
        "event": "rtds_trade_event",
        "source_wallet": wallet,
        "market_slug": f"btc-updown-5m-{start}",
        "condition_id": f"cond-{start}",
        "outcome": "Up",
        "side": side,
        "price": price,
        "size": 10.0,
        "event_ts": float(start + offset),
        "received_at_s": float(start + offset) + 0.2,
        "asset": f"asset-{start}",
        "transaction_hash": f"0x{start:x}{offset:x}",
    }


def test_non_active_signal_census_counts_tracked_wallets_in_no_signal_windows() -> None:
    candidate = "0x1111111111111111111111111111111111111111"
    active = "0x2222222222222222222222222222222222222222"
    untracked = "0x3333333333333333333333333333333333333333"
    no_signal_start = 1_783_296_300
    traded_start = 1_783_296_600

    opportunity_census = {
        "rows": [
            {
                "classification": "NO_SOURCE_SIGNAL",
                "market_slug": f"btc-updown-5m-{no_signal_start}",
                "window_start_s": no_signal_start,
            },
            {
                "classification": "TRADED",
                "market_slug": f"btc-updown-5m-{traded_start}",
                "window_start_s": traded_start,
            },
        ]
    }
    registry = {
        "wallets": [
            {
                "address": candidate,
                "enabled": True,
                "name": "candidate",
                "notes": "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #5 pnl=42.0 vol=1000",
            },
            {"address": active, "enabled": True, "name": "active"},
        ]
    }
    history_state = {
        "wallet_results": [
            {"wallet": {"address": candidate}, "events": 20, "copy_intents": 7, "latest_event_ts": no_signal_start + 260},
        ]
    }
    alpha_report = {
        "execution_profiles": {
            "profiles_by_wallet": {
                candidate: {
                    "eligible": True,
                    "status": "PASS",
                    "copyable_rate_pct": 80.0,
                    "fill_sample": 40,
                    "eligible_move_slice_count": 2,
                    "mean_edge": 0.03,
                    "median_edge": 0.02,
                }
            }
        }
    }
    guard_state = {"active_set": {"members": [{"source_wallet": active}]}}

    report = build_report(
        rtds_rows=[
            _rtds_row(candidate, no_signal_start, 250),
            _rtds_row(candidate, no_signal_start, 270),
            _rtds_row(candidate, traded_start, 250),
            _rtds_row(active, no_signal_start, 250),
            _rtds_row(untracked, no_signal_start, 250),
            _rtds_row(candidate, no_signal_start, 280, side="SELL"),
        ],
        opportunity_census=opportunity_census,
        registry=registry,
        history_state=history_state,
        alpha_report=alpha_report,
        guard_state=guard_state,
        top_n=5,
    )

    assert report["summary"]["no_source_signal_windows"] == 1
    assert report["summary"]["non_active_wallets_seen"] == 1
    assert report["summary"]["candidates_meeting_existing_bar_hint"] == 1
    assert report["summary"]["diagnostics"]["outside_no_signal_window"] == 1
    assert report["summary"]["diagnostics"]["active_set_wallet"] == 1
    assert report["summary"]["diagnostics"]["untracked_wallet"] == 1
    assert report["summary"]["diagnostics"]["non_buy_trade"] == 1

    row = report["top_candidates"][0]
    assert row["wallet"] == candidate
    assert row["source_active_windows_in_empty_set"] == 1
    assert row["buy_events_in_empty_set"] == 2
    assert row["source_usd_in_empty_set"] == 8.0
    assert row["resolved_pnl"] == 42.0
    assert row["history_copy_intents"] == 7
    assert row["alpha_eligible"] is True
    assert row["meets_existing_bar_hint"] is True
