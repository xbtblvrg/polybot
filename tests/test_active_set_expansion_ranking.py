from scripts.report_active_set_expansion_ranking import build_report


def _candidate(wallet: str, *, pnl: float = 1.0, resolved: int = 12) -> dict:
    return {
        "wallet": wallet,
        "candidate_id": f"candidate_{wallet[-6:]}",
        "paper_replay": {
            "candidate_clob_backed_resolved_orders": resolved,
            "candidate_clob_backed_orders": resolved,
            "copyable_buy_events": resolved,
            "paper_orders": resolved,
            "paper_pnl_usd": pnl,
            "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
        },
    }


def test_active_set_expansion_ranking_keeps_ready_backlog_when_set_at_max() -> None:
    active_wallets = [
        f"0x{'%040x' % index}" for index in range(1, 9)
    ]
    ready_wallet = "0x9999999999999999999999999999999999999999"
    report = build_report(
        full_pool_replay={"candidates": [_candidate(ready_wallet, pnl=4.25, resolved=19)]},
        member_bar_replay={"candidates": []},
        slow_market_ranking={"ranked_wallets": []},
        slow_market_measurement={"summary": {"paper_pnl_usd": -1.0, "copyable_buy_events": 0}},
        slow_market_status={"status": "PASS", "next_action": "continue_paper"},
        live_guard_state={
            "active_set": {
                "target_member_count_max": 8,
                "members": [{"source_wallet": wallet} for wallet in active_wallets],
            }
        },
        abandoned_payload=[],
        limit=10,
    )

    summary = report["summary"]
    assert summary["active_wallets"] == 8
    assert summary["open_active_slots"] == 0
    assert summary["promotion_ready_backlog_count"] == 1
    assert summary["promotions_allowed"] == 0
    assert summary["decision"] == "ACTIVE_SET_AT_MAX_RANKING_ONLY"
    assert report["ready_candidates"] == []
    assert report["ready_backlog_candidates"][0]["wallet"] == ready_wallet


def test_active_set_expansion_ranking_allows_only_open_slots() -> None:
    active_wallets = [
        f"0x{'%040x' % index}" for index in range(1, 8)
    ]
    ready_wallets = [
        "0x9999999999999999999999999999999999999999",
        "0x8888888888888888888888888888888888888888",
    ]
    report = build_report(
        full_pool_replay={"candidates": [_candidate(ready_wallets[0]), _candidate(ready_wallets[1], pnl=2.0)]},
        member_bar_replay={"candidates": []},
        slow_market_ranking={"ranked_wallets": []},
        slow_market_measurement={"summary": {}},
        slow_market_status={},
        live_guard_state={
            "active_set": {
                "target_member_count_max": 8,
                "members": [{"source_wallet": wallet} for wallet in active_wallets],
            }
        },
        abandoned_payload=[],
        limit=10,
    )

    summary = report["summary"]
    assert summary["open_active_slots"] == 1
    assert summary["promotion_ready_backlog_count"] == 2
    assert summary["promotions_allowed"] == 1
    assert summary["decision"] == "PROMOTE"
    assert len(report["ready_candidates"]) == 1
