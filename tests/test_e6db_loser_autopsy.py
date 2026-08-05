import json
from pathlib import Path

from scripts.report_e6db_loser_autopsy import E6DB, build_autopsy


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def _order(*, slug: str, submitted_at: str, side: str, cost: float, shares: float, fee: float) -> dict:
    order_id = f"order-{slug}"
    return {
        "order_id": order_id,
        "intent_id": f"intent-{slug}",
        "source_wallet": E6DB,
        "wallet_name": "live_primary_84407dac",
        "condition_id": f"condition-{slug}",
        "market_slug": slug,
        "side": side,
        "limit_price": round(cost / shares, 6),
        "status": "FILLED",
        "final_status": "FILLED",
        "submitted_at": submitted_at,
        "expected_fee_gate": {"expected_fee_usd": fee},
        "trade_result": {
            "response_fill_price": round(cost / shares, 6),
            "response_filled_size_usd": cost,
            "response_fill_size_shares": shares,
        },
    }


def test_e6db_loser_autopsy_computes_payoff_shape_and_classification(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.json"
    resolutions = tmp_path / "resolutions.jsonl"
    _write_json(
        ledger,
        {
            "orders": [
                _order(
                    slug="btc-updown-5m-1783987200",
                    submitted_at="2026-07-14T00:00:04+00:00",
                    side="YES",
                    cost=2.5,
                    shares=5.0,
                    fee=0.1,
                ),
                _order(
                    slug="btc-updown-5m-1783987500",
                    submitted_at="2026-07-14T00:05:04+00:00",
                    side="YES",
                    cost=2.5,
                    shares=5.0,
                    fee=0.1,
                ),
                _order(
                    slug="btc-updown-5m-1783987800",
                    submitted_at="2026-07-14T00:10:04+00:00",
                    side="NO",
                    cost=2.5,
                    shares=5.0,
                    fee=0.1,
                ),
            ]
        },
    )
    _write_jsonl(
        resolutions,
        [
            {"expiry_unix_ts": 1783987500, "window_type": "5m", "direction": "DOWN", "source": "test"},
            {"expiry_unix_ts": 1783987800, "window_type": "5m", "direction": "UP", "source": "test"},
            {"expiry_unix_ts": 1783988100, "window_type": "5m", "direction": "UP", "source": "test"},
        ],
    )

    report = build_autopsy(ledger_path=ledger, resolutions_path=resolutions, day="2026-07-14")

    summary = report["summary"]
    assert summary["fills"] == 3
    assert summary["resolved_windows"] == 3
    assert summary["winning_windows"] == 1
    assert summary["losing_windows"] == 2
    assert summary["realized_pnl_usd"] == -2.5
    assert summary["avg_win_per_winner_usd"] == 2.5
    assert summary["avg_loss_per_loser_abs_usd"] == 2.5
    assert summary["expected_fee_usd"] == 0.3
    assert summary["diagnostic_pre_expected_fee_pnl_usd"] == -2.2
    assert summary["actual_win_rate"] == 0.333333
    assert summary["required_win_rate_at_payoff_shape"] == 0.5
    assert summary["gap_sigma_pp"] == 28.867513
    assert summary["gap_in_sigma"] == -0.57735
    assert summary["classification"] == "structural_negative_edge"
    assert report["per_window"][0]["side"] == "YES"
    assert report["per_window"][0]["resolved_outcome"] == "Down"
    assert report["per_window"][0]["result"] == "LOSS"
