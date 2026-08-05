from scripts.report_order127_seat_distance import TARGETS, build_packet


def test_order127_measures_all_three_without_admission_authority() -> None:
    frontier = {
        "candidate_count": 3,
        "eligible_count": 0,
        "nearest_frontier": [
            {"wallet": TARGETS[0], "active_temporal": {"pnl_usd": -10, "roi_pct": -2, "resolved_trades": 100}, "evidence_deficits": ["temporal"]},
            {"wallet": TARGETS[1], "direct_source": {"attempts": 20, "copyable": 0}, "evidence_deficits": ["f2"]},
            {"wallet": TARGETS[2], "standby_exclusion": {"permanent_park": True, "park_basis": {"observed_marginal_post_fee_usd_per_fill": -0.309917}}, "evidence_deficits": ["park"]},
        ],
    }
    temporal = {"wallets": [{"wallet": TARGETS[1], "recent": {"pnl_usd": -5, "roi_pct": -1, "resolved_trades": 50}}]}
    packet = build_packet(frontier, temporal)
    assert packet["verdict"] == "NO_WALLET_WITHIN_REACH_UNDER_UNCHANGED_BARS"
    assert packet["admission_authority"] is False
    assert packet["rows"][1]["distances"][0]["copyable_rows_gap"] == 1
    assert packet["rows"][2]["distances"][0]["authority_change_required"] is True
