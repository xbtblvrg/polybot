from scripts.report_order128_fastest_lawful_path import build_packet, candidate_evidence


def test_candidate_evidence_falls_back_to_source_drought_when_actuator_is_empty() -> None:
    packet = {"rows": [{"wallet": "0x" + "3" * 40}]}
    deadman = {"policy_choke": {"actuator": {"candidate_evidence": {}}, "source_roster_drought": {"candidate_evidence": packet}}}
    assert candidate_evidence(deadman) is packet


def test_soft_dual_path_beats_structural_wall_and_binds() -> None:
    rows = [
        {"wallet": "0x" + "1" * 40, "wide_policy_fingerprint": "soft", "checks": {"f2_fresh_rows_and_own_policy_copyable": False, "f3_not_enabled_or_cooloff_or_fading": False, "f1_walk_forward_admissible": False, "own_evidenced_policy_available": True, "active_temporal_not_proven_negative": True, "active_temporal_regime_cell_measured": True, "both_resolved_halves_positive": True, "f1_concentration_admissible": True, "f1_measured_positive_regime_cell": True, "f1_venue_reachable_admissible": True}, "evidence_deficits": ["f2_fresh_rows_and_own_policy_copyable", "f3_not_enabled_or_cooloff_or_fading", "f1_walk_forward_admissible"], "direct_source": {"attempts": 50, "copyable": 0}, "regime_evidence": {"pnl_usd": 10}},
        {"wallet": "0x" + "2" * 40, "wide_policy_fingerprint": "park", "checks": {"own_evidenced_policy_available": True}, "evidence_deficits": ["not_terminal_park_red_clock_or_measured_loser"], "standby_exclusion": {"permanent_park": True}},
    ]
    temporal = {"wallets": [{"wallet": rows[0]["wallet"], "recent": {"pnl_usd": -5, "stake_usd": 1000, "resolved_trades": 50}}]}
    packet = build_packet({"rows": rows, "eligible_count": 0}, temporal, {"gen2": {"residual_to_200": 150}}, 0.0)
    assert packet["verdict"] == "REBIND_SOLE_PAPER_FOCUS"
    assert packet["binding_action"]["wide_policy_fingerprint"] == "soft"
    assert packet["binding_action"]["eta_is_lower_bound"] is True
    assert packet["top_soft_clock"]["eta_completeness"] == "PARTIAL"
    assert packet["top_soft_clock"]["unmodeled_deficits"] == ["f1_walk_forward_admissible"]
    assert any(row["partition"] == "STRUCTURAL_WALL" for row in packet["top_10"])


def test_top_10_contains_distinct_wallets_but_ranked_rows_retain_duplicates() -> None:
    base = {
        "checks": {
            "own_evidenced_policy_available": True,
            "active_temporal_not_proven_negative": True,
            "active_temporal_regime_cell_measured": True,
            "both_resolved_halves_positive": True,
            "f1_concentration_admissible": True,
            "f1_measured_positive_regime_cell": True,
            "f1_venue_reachable_admissible": True,
            "f1_walk_forward_admissible": True,
            "f2_fresh_rows_and_own_policy_copyable": True,
            "f3_not_enabled_or_cooloff_or_fading": True,
        },
        "evidence_deficits": [],
    }
    rows = []
    for index in range(11):
        wallet = "0x" + f"{index:040x}"
        rows.append({**base, "wallet": wallet, "wide_policy_fingerprint": f"p{index}"})
        if index == 0:
            rows.append({**base, "wallet": wallet, "wide_policy_fingerprint": "duplicate"})
    packet = build_packet({"rows": rows}, {}, {}, 0.0)
    assert len(packet["ranked_rows"]) == 12
    assert len(packet["top_10"]) == 10
    assert len({row["wallet"] for row in packet["top_10"]}) == 10


def test_latest_generation_wins_before_eta_ranking() -> None:
    checks = {
        "own_evidenced_policy_available": True,
        "active_temporal_not_proven_negative": True,
        "active_temporal_regime_cell_measured": True,
        "both_resolved_halves_positive": True,
        "f1_concentration_admissible": True,
        "f1_measured_positive_regime_cell": True,
        "f1_venue_reachable_admissible": True,
        "f1_walk_forward_admissible": False,
        "f2_fresh_rows_and_own_policy_copyable": False,
        "f3_not_enabled_or_cooloff_or_fading": False,
    }
    base = {
        "wallet": "0x" + "3" * 40,
        "wide_policy_fingerprint": "same",
        "checks": checks,
        "evidence_deficits": ["f1_walk_forward_admissible", "f2_fresh_rows_and_own_policy_copyable", "f3_not_enabled_or_cooloff_or_fading"],
    }
    rows = [
        {**base, "source_generation": "old", "direct_source": {"attempts": 9, "copyable": 0, "latest_receipt_at": "2026-08-01T00:10:00Z"}},
        {**base, "source_generation": "new", "direct_source": {"attempts": 47, "copyable": 0, "latest_receipt_at": "2026-08-01T00:20:00Z"}},
    ]
    packet = build_packet(
        {"rows": rows, "candidate_count": 2},
        {},
        {},
        0.0,
        deadman_checked_at="cut",
        score_run_id="wide_cut",
    )
    assert packet["collapsed_generations"] == 1
    assert packet["candidate_count"] == 1
    assert packet["ranked_rows"][0]["source_generation"] == "new"
    assert packet["ranked_rows"][0]["direct_source"]["attempts"] == 47
    assert packet["deadman_checked_at"] == "cut"
    assert packet["score_run_id"] == "wide_cut"
    assert packet["soft_clock_bench_depth"] == 1
    second = build_packet(
        {"rows": rows, "candidate_count": 2},
        {},
        {},
        0.0,
        deadman_checked_at="cut2",
        prior_packet=packet,
    )
    assert second["rank_stability"]["cuts_agreed"] == 2
    assert second["rank_stability"]["stable_for_accrual"] is True


def test_candidate_count_mismatch_refuses_bind() -> None:
    row = {
        "wallet": "0x" + "4" * 40,
        "wide_policy_fingerprint": "soft",
        "checks": {
            "own_evidenced_policy_available": True,
            "active_temporal_not_proven_negative": True,
            "active_temporal_regime_cell_measured": True,
            "both_resolved_halves_positive": True,
            "f1_concentration_admissible": True,
            "f1_measured_positive_regime_cell": True,
            "f1_venue_reachable_admissible": True,
            "f1_walk_forward_admissible": True,
            "f2_fresh_rows_and_own_policy_copyable": True,
            "f3_not_enabled_or_cooloff_or_fading": True,
        },
        "evidence_deficits": [],
    }
    packet = build_packet({"rows": [row], "candidate_count": 2}, {}, {}, 0.0)
    assert packet["cut_consistent"] is False
    assert packet["verdict"] == "CUT_MISMATCH_BIND_REFUSED"
    assert packet["binding_action"] is None
    assert packet["discovery_wave_required"] is False
