from scripts.report_82c8_resolved_tape_gap import WALLET, build_report


def test_gap_closed_but_two_window_sample_remains_refused() -> None:
    report = build_report(
        temporal={
            "generated_at": "2026-07-31T03:27:26Z",
            "wallets": [
                {
                    "wallet": WALLET,
                    "recent": {
                        "latest_event_age_h": 0.164,
                        "latest_event_ts": "2026-07-31T03:17:08Z",
                        "resolved_trades": 50,
                        "unique_windows": 2,
                        "pnl_usd": 2.857188,
                        "roi_pct": 0.716017,
                    },
                }
            ],
        },
        liveness={
            "rows": [
                {
                    "wallet": WALLET,
                    "address_selection": {"last_trade_age_h": 0.001954},
                }
            ]
        },
        replay={
            "results": [
                {"wallet": WALLET, "normalized_btc5m_buy_events": 2000}
            ]
        },
        overlay={"members": [{"source_wallet": WALLET, "enabled": True}]},
    )

    assert report["checks"] == {
        "resolved_tape_gap_closed_inside_24h": True,
        "recent_distinct_windows_gte_5": False,
        "recent_pnl_positive": True,
    }
    assert report["decision"] == "GAP_CLOSED_SAMPLE_DIVERSITY_PENDING_REFUSE"
    assert report["overlay_enabled"] is True
    assert report["live_admission_authority"] is False
