from __future__ import annotations

from scripts.report_member_day_pnl_decomposition import build_report


def test_member_day_pnl_decomposition_includes_active_zero_rows_and_fee_share() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    ledger = {
        "orders": [
            {
                "order_id": "win-a",
                "final_status": "FILLED",
                "submitted_at": "2026-07-14T10:00:00+00:00",
                "source_wallet": wallet_a,
                "condition_id": "cond-a",
                "market_slug": "btc-updown-5m-1780000000",
                "side": "YES",
                "requested_size_usd": 4.0,
                "requested_shares": 10.0,
                "expected_fee_gate": {"expected_fee_usd": 0.2},
            },
            {
                "order_id": "reject-a",
                "final_status": "REJECTED",
                "submitted_at": "2026-07-14T10:01:00+00:00",
                "source_wallet": wallet_a,
            },
        ]
    }
    guard_state = {
        "active_set_runtime": {
            "members": [
                {"source_wallet": wallet_a, "candidate_id": "candidate-a", "policy_id": "policy", "enabled": True},
                {"source_wallet": wallet_b, "candidate_id": "candidate-b", "policy_id": "policy", "enabled": True},
            ]
        }
    }

    report = build_report(
        ledger=ledger,
        guard_state=guard_state,
        resolutions={"cond-a": {"direction": "UP", "source": "test"}},
        day="2026-07-14",
    )

    by_wallet = {row["source_wallet"]: row for row in report["members"]}
    assert report["total"]["orders"] == 2
    assert report["total"]["resolved_fills"] == 1
    assert report["total"]["pnl_usd"] == 6.0
    assert report["total"]["expected_fee_usd"] == 0.2
    assert by_wallet[wallet_a]["orders"] == 2
    assert by_wallet[wallet_a]["rejects"] == 1
    assert by_wallet[wallet_a]["avg_resolved_filled_size_usd"] == 4.0
    assert by_wallet[wallet_a]["resolved_expected_fee_share_of_resolved_cost_pct"] == 5.0
    assert by_wallet[wallet_b]["orders"] == 0
    assert by_wallet[wallet_b]["candidate_id"] == "candidate-b"


def test_member_day_pnl_decomposition_filters_to_requested_day() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ledger = {
        "orders": [
            {
                "order_id": "prior",
                "final_status": "FILLED",
                "submitted_at": "2026-07-13T23:59:59+00:00",
                "source_wallet": wallet,
                "condition_id": "cond",
                "side": "YES",
                "requested_size_usd": 1.0,
                "requested_shares": 2.0,
            },
            {
                "order_id": "day",
                "final_status": "FILLED",
                "submitted_at": "2026-07-14T00:00:00+00:00",
                "source_wallet": wallet,
                "condition_id": "cond",
                "side": "NO",
                "requested_size_usd": 1.0,
                "requested_shares": 2.0,
            },
        ]
    }

    report = build_report(
        ledger=ledger,
        guard_state={},
        resolutions={"cond": {"direction": "UP", "source": "test"}},
        day="2026-07-14",
    )

    assert report["total"]["orders"] == 1
    assert report["total"]["pnl_usd"] == -1.0
    assert report["members"][0]["source_wallet"] == wallet
