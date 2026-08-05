from scripts.report_routing_disambiguation import build_report


TARGET = "0x4d8bc628487bbc9931b4d039e6a7529b8ae1a00d"
OTHER = "0x5e4aa0f176014729f5168e821ce614484fbebe6b"


def _signal_supply(start: int) -> dict:
    return {
        "rows": [
            {
                "market_slug": f"btc-updown-5m-{start}",
                "window_start_s": start,
                "classification": "sources_traded_but_unobserved",
                "active_wallets_with_remote_trades": [TARGET],
            },
            {
                "market_slug": f"btc-updown-5m-{start + 300}",
                "window_start_s": start + 300,
                "classification": "sources_traded_but_unobserved",
                "active_wallets_with_remote_trades": [TARGET],
            },
        ]
    }


def _guard_state(*, include_premerge: bool = True) -> dict:
    state = {
        "active_set_runtime": {
            "selection_mode": "last_successful_nondenied_member_priority_single_guard",
            "fresh_runtime_member_selection": {"selected_wallet": OTHER},
            "members": [
                {
                    "source_wallet": TARGET,
                    "candidate_id": "runtime_auto_degrade_4d8bc62848",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "enabled": True,
                },
                {
                    "source_wallet": OTHER,
                    "candidate_id": "leaderboard_crypto_5e4aa0f176",
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "enabled": True,
                },
            ],
        }
    }
    if include_premerge:
        state["active_set_rtds_premerge"] = {"rows": [{"source_wallet": TARGET}]}
    return state


def test_routing_disambiguation_identifies_signal_not_selected() -> None:
    start = 1_800_000_000
    report = build_report(
        signal_supply=_signal_supply(start),
        guard_state=_guard_state(),
        history_state={
            "events": [
                {
                    "source_wallet": TARGET,
                    "market_slug": f"btc-updown-5m-{start}",
                    "action": "BUY",
                    "source": "rtds_activity",
                },
                {
                    "source_wallet": TARGET,
                    "market_slug": f"btc-updown-5m-{start + 300}",
                    "action": "BUY",
                    "source": "rtds_activity",
                },
            ],
            "copy_intents": [],
        },
        live_ledger={"orders": []},
        realtime_shadow={"events": []},
        realtime_watch_state={"passive_wallets": []},
        guard_shadow={"rows": []},
        target_wallet=TARGET,
        sample_size=20,
    )

    assert report["summary"]["sampled_windows"] == 2
    assert report["summary"]["target_wallet_active_runtime_member"] is True
    assert report["summary"]["target_wallet_active_rtds_watch"] is True
    assert report["summary"]["selected_wallet_at_report_time"] == OTHER
    assert report["summary"]["class_counts"] == {"signal-emitted-but-not-selected": 2}
    assert {row["classification"] for row in report["rows"]} == {"signal-emitted-but-not-selected"}


def test_routing_disambiguation_identifies_unwatched_source_set() -> None:
    start = 1_800_000_000
    report = build_report(
        signal_supply=_signal_supply(start),
        guard_state=_guard_state(include_premerge=False),
        history_state={"events": [], "copy_intents": []},
        live_ledger={"orders": []},
        realtime_shadow={"events": []},
        realtime_watch_state={"passive_wallets": []},
        guard_shadow={"rows": []},
        target_wallet=TARGET,
        sample_size=1,
    )

    assert report["summary"]["sampled_windows"] == 1
    assert report["summary"]["class_counts"] == {"wallet-not-in-watched-source-set": 1}
    assert report["rows"][0]["runtime_member_enabled"] is True
