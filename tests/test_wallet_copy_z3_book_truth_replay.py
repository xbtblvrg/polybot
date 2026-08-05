from scripts.report_z3_book_truth_replay import build_forward_lane, build_report


def test_z3_book_truth_report_marks_settlement_pnl_gap_and_forward_wallet() -> None:
    alpha = {
        "updated_at": "2026-07-06T02:35:10+00:00",
        "execution_profiles": {
            "profiles_by_wallet": {
                "0x1111111111111111111111111111111111111111": {
                    "eligible": True,
                    "fill_sample": 25,
                    "raw_fill_coverage": 27,
                    "copyable_rate_pct": 80.0,
                    "mean_edge": 0.02,
                    "median_edge": 0.01,
                    "stale_or_missing_book_observations": 2,
                    "blockers": [],
                }
            }
        },
    }
    lane = {"ranked_wallets": [{"wallet": "0x1111111111111111111111111111111111111111"}], "status": "ANALYZE"}

    report = build_report(alpha_report=alpha, lane_state=lane)
    forward = build_forward_lane(report)

    assert report["verdict"]["status"] == "ANALYZE"
    assert report["rows"][0]["latency_edge_pnl_proxy_usd"] == 0.5
    assert report["rows"][0]["paper_pnl_usd"] is None
    assert report["rows"][0]["eligible_for_forward_paper_lane"] is True
    assert "settlement_pnl_missing_for_book_truth_replay" in report["verdict"]["blockers"]
    assert forward["wallets"] == ["0x1111111111111111111111111111111111111111"]


def test_z3_book_truth_report_prefers_settlement_backed_replay() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    source_replay = {
        "kind": "wallet_copy_z3_eligible_profile_book_truth_replay",
        "summary": {
            "copyable_buy_events": 19,
            "book_covered_events": 30,
            "paper_pnl_usd": 1.25,
        },
        "wallets": [
            {
                "wallet": wallet,
                "buy_events": 30,
                "book_covered_events": 30,
                "copyable_buy_events": 19,
                "rejected_buy_events": 11,
                "resolved_copyable_events": 19,
                "paper_pnl_usd": 1.25,
                "roi_pct": 12.5,
                "win_rate_pct": 60.0,
                "primary_promotable": False,
                "reject_reasons": {"best_ask_above_250bps_copy_limit": 11},
            }
        ],
    }
    lane = {"ranked_wallets": [{"wallet": wallet}], "status": "ANALYZE"}

    report = build_report(alpha_report={}, lane_state=lane, source_replay=source_replay)
    forward = build_forward_lane(report)

    assert report["verdict"]["settlement_pnl_available"] is True
    assert report["verdict"]["copyable_buy_events"] == 19
    assert report["verdict"]["promotable_wallets"] == 0
    assert report["rows"][0]["paper_pnl_basis"] == "settlement_backed_book_truth_replay"
    assert "copyable_buy_events_below_20_floor" in report["rows"][0]["blockers"]
    assert forward["wallets"] == [wallet]


def test_z3_forward_lane_attaches_below_floor_queue_pass() -> None:
    z3_wallet = "0x3333333333333333333333333333333333333333"
    queue_wallet = "0xad825954d08beba32f74b594821f4251460c3df1"
    report = {
        "kind": "wallet_copy_z3_book_truth_replay",
        "generated_at": "2026-07-06T03:45:00+00:00",
        "verdict": {"status": "ANALYZE"},
        "rows": [{"wallet": z3_wallet, "eligible_for_forward_paper_lane": True}],
    }
    queue = {
        "ranked_members": [
            {
                "wallet": queue_wallet,
                "ready_for_live": False,
                "replay": {
                    "status": "PASS",
                    "copyable_buy_events": 8,
                    "paper_pnl_usd": 0.485392,
                },
            }
        ]
    }

    forward = build_forward_lane(report, member_queue=queue)

    assert forward["wallets"] == [z3_wallet, queue_wallet]
    assert forward["wallet_count"] == 2
    assert forward["extra_wallets"] == [queue_wallet]
    assert forward["extra_wallet_source"] == "below_floor_full_pool_strict_replay_pass"
