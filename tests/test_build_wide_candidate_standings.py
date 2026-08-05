from scripts.build_wide_candidate_standings import build_standings


WALLET = "0x0000000000000000000000000000000000000001"


def test_standings_requires_every_current_alpha_and_paper_gate() -> None:
    report = build_standings(
        {
            "ranked_queue": [
                {"wallet": WALLET, "queue_rank": 1, "admission_status": "READY_QUEUE"}
            ]
        },
        {
            "status": "PASS_CURRENT_SOURCE",
            "source_freshness": {"pass": True, "history_is_frozen_d97": False},
            "execution_profiles": {
                "profiles_by_wallet": {
                    WALLET: {"status": "PASS", "eligible": True, "fill_sample": 20}
                }
            },
        },
        {
            "kind": "wide_exact_policy_prospective_paper_state",
            "policy_id": "exact",
            "cohort": {"cohort_id": "fresh", "run_id": "wide"},
            "wallets": {
                WALLET: {
                    "attempted_exact_policy_buys": 60,
                    "copyable_exact_policy_buys": 50,
                    "copyable_rate_pct": 83.333333,
                    "resolved_orders": 50,
                    "fee_covered_resolved_orders": 50,
                    "fee_coverage_pct": 100.0,
                    "fees_usd": 1.0,
                    "post_fee_pnl_usd": 2.0,
                    "first_half_post_fee_pnl_usd": 1.0,
                    "second_half_post_fee_pnl_usd": 1.0,
                    "max_receipt_to_book_fetch_lag_s": 2.0,
                    "p95_receipt_to_book_fetch_lag_s": 1.0,
                    "all_fill_lags_lte_5s": True,
                }
            },
        },
    )

    assert report["summary"]["dual_gate_winners"] == 1
    assert report["standings"][0]["dual_gate_winner"] is True
    assert report["standings"][0]["dual_gate_winner_basis"] == "per_run_gates_only_cumulative_reporting_forbidden"


def test_standings_publishes_cumulative_policy_basin_without_feeding_gate() -> None:
    fingerprint = "a" * 64
    report = build_standings(
        {
            "ranked_queue": [
                {"wallet": WALLET, "queue_rank": 1, "admission_status": "READY_QUEUE"}
            ]
        },
        {
            "status": "PASS_CURRENT_SOURCE",
            "source_freshness": {"pass": True, "history_is_frozen_d97": False},
            "execution_profiles": {
                "profiles_by_wallet": {
                    WALLET: {"status": "PASS", "eligible": True, "fill_sample": 20}
                }
            },
        },
        {
            "kind": "wide_exact_policy_prospective_paper_state",
            "policy_id": "exact",
            "cohort": {"cohort_id": "fresh", "run_id": "run_b"},
            "wallets": {
                WALLET: {
                    "attempted_exact_policy_buys": 20,
                    "copyable_exact_policy_buys": 14,
                    "copyable_rate_pct": 70.0,
                    "resolved_orders": 10,
                    "fee_covered_resolved_orders": 10,
                    "fees_usd": 0.4,
                    "post_fee_pnl_usd": 1.5,
                    "first_half_post_fee_pnl_usd": 0.8,
                    "second_half_post_fee_pnl_usd": 0.7,
                    "max_receipt_to_book_fetch_lag_s": 2.0,
                    "all_fill_lags_lte_5s": True,
                }
            },
        },
        {
            "manifest_id": "manifest",
            "score_run_id": "run_b",
            "capture_watch_wallets": [
                {
                    "wallet": WALLET,
                    "queue_rank": 1,
                    "wide_policy_fingerprint": fingerprint,
                }
            ],
        },
        [
            {
                "kind": "wide_candidate_exact_policy_standings",
                "generated_at": "2026-07-30T00:00:00Z",
                "prospective_cohort": {"run_id": "run_a"},
                "standings": [
                    {
                        "wallet": WALLET,
                        "wide_policy_fingerprint": fingerprint,
                        "attempted_buy_events": 30,
                        "copyable_buy_events": 21,
                        "resolved_orders": 25,
                        "fees_usd": 1.0,
                        "post_fee_pnl_usd": 3.0,
                    },
                    {
                        "wallet": WALLET,
                        "attempted_buy_events": 999,
                    },
                ],
            },
            {
                "kind": "wide_candidate_exact_policy_standings",
                "generated_at": "2026-07-31T00:00:00Z",
                "prospective_cohort": {"run_id": "run_b"},
                "standings": [
                    {
                        "wallet": WALLET,
                        "wide_policy_fingerprint": fingerprint,
                        "attempted_buy_events": 999,
                    }
                ],
            },
        ],
    )

    row = report["standings"][0]
    assert row["wide_policy_fingerprint"] == fingerprint
    assert row["resolved_orders"] == 10
    assert row["attempted_exact_policy_buys"] == 20
    assert row["copyable_exact_policy_buys"] == 14
    assert row["paper_resolved_orders"] == 10
    assert row["per_run_leg_present"] is True
    assert row["dual_gate_winner"] is False
    assert row["cumulative_attempted_exact_policy_buys"] == 50
    assert row["cumulative_copyable_exact_policy_buys"] == 35
    assert row["cumulative_paper_resolved_orders"] == 35
    assert row["cumulative_post_fee_pnl_usd"] == 4.5
    assert row["cumulative_run_count"] == 2
    assert row["cumulative_admission_authority"] is False
    assert report["summary"]["dual_gate_winners"] == 0
    assert report["summary"]["cumulative_paper_resolved_orders"] == 35
    assert report["summary"]["cumulative_history_skipped_rows"] == {
        "missing_wallet_or_fingerprint": 1,
        "same_or_missing_run_id": 1,
    }


def test_standings_refuses_missing_fee_and_resolved_evidence() -> None:
    report = build_standings(
        {
            "ranked_queue": [
                {"wallet": WALLET, "queue_rank": 1, "admission_status": "READY_QUEUE"}
            ]
        },
        {"status": "PASS_CURRENT_SOURCE", "source_freshness": {"pass": True}},
        {"policy_id": "exact", "wallets": {WALLET: {"buy_events": 1}}},
    )

    assert report["summary"]["dual_gate_winners"] == 0
    assert report["summary"]["refusal_counts"]["fees_measured"] == 1
    assert report["summary"]["refusal_counts"]["resolved_gte_50"] == 1


def test_immutable_manifest_roster_overrides_mutable_ready_queue() -> None:
    excluded = "0x0000000000000000000000000000000000000002"
    measurement = {
        "kind": "wide_exact_policy_prospective_paper_state",
        "cohort": {"manifest_id": "manifest", "run_id": "run"},
        "wallets": {},
    }
    report = build_standings(
        {
            "ranked_queue": [
                {"wallet": WALLET, "queue_rank": 1, "admission_status": "READY_QUEUE"},
                {"wallet": excluded, "queue_rank": 2, "admission_status": "READY_QUEUE"},
            ]
        },
        {},
        measurement,
        {
            "manifest_id": "manifest",
            "score_run_id": "run",
            "capture_watch_wallets": [
                {
                    "wallet": WALLET,
                    "queue_rank": 1,
                    "move_slice_keys": ["120-180|0.25-0.50"],
                }
            ],
            "promotion_admitted_wallets": [],
        },
    )

    assert report["summary"]["mutable_ready_queue_wallets"] == 2
    assert report["summary"]["capture_watch_wallets"] == 1
    assert [row["wallet"] for row in report["standings"]] == [WALLET]
    assert report["manifest_reconciliation"]["manifest_identity_exact"] is True
    assert report["slice_failure_matrix"][0]["resolved_orders"] == 0
    assert report["slice_failure_matrix"][0]["checks"]["entry_band_025_050"] is True
