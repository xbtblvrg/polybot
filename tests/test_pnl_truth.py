from __future__ import annotations

import json
import urllib.parse

import pytest

from src.wallet_copy.pnl_truth import (
    build_pnl_truth,
    chain_reconciliation,
    discrepancy_report,
    fetch_live_position_value,
    price_bucket,
    price_subbucket,
    resolved_pnl,
    validate_resolutions_nonempty_for_fills,
)
import src.wallet_copy.pnl_truth as pnl_truth


def test_pnl_truth_scores_fill_resolution_join_by_day_member_and_band() -> None:
    orders = [
        {
            "order_id": "win",
            "final_status": "FILLED",
            "submitted_at": "2026-07-05T10:00:00+00:00",
            "source_wallet": "0x1111111111111111111111111111111111111111",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-1780000000",
            "side": "YES",
            "limit_price": 0.4,
            "requested_size_usd": 4.0,
            "requested_shares": 10.0,
        },
        {
            "order_id": "loss",
            "final_status": "FILLED",
            "submitted_at": "2026-07-05T10:05:00+00:00",
            "source_wallet": "0x2222222222222222222222222222222222222222",
            "condition_id": "0xdef",
            "market_slug": "btc-updown-5m-1780000300",
            "side": "NO",
            "limit_price": 0.6,
            "requested_size_usd": 2.0,
            "requested_shares": 5.0,
        },
        {"order_id": "reject", "final_status": "REJECTED", "submitted_at": "2026-07-05T10:06:00+00:00"},
    ]
    resolutions = {
        "0xabc": {"direction": "UP", "source": "test"},
        "0xdef": {"direction": "UP", "source": "test"},
    }

    truth = build_pnl_truth({"orders": orders}, resolutions)

    assert truth["total"]["orders"] == 3
    assert truth["total"]["resolved_fills"] == 2
    assert truth["total"]["rejects"] == 1
    assert truth["total"]["pnl_usd"] == 4.0
    assert truth["by_day"]["2026-07-05"]["pnl_usd"] == 4.0
    assert truth["by_member"]["0x1111111111111111111111111111111111111111"]["pnl_usd"] == 6.0
    assert truth["by_price_band"]["02_50_70"]["pnl_usd"] == -2.0


def test_price_subbands_are_additive_and_preserve_price_bands() -> None:
    prices = (0.24, 0.25, 0.31, 0.32, 0.39, 0.40, 0.49, 0.50)
    expected_subbands = (
        "00_00_25",
        "01a_25_32",
        "01a_25_32",
        "01b_32_40",
        "01b_32_40",
        "01c_40_50",
        "01c_40_50",
        "02_50_70",
    )
    assert tuple(price_subbucket(price) for price in prices) == expected_subbands
    assert tuple(price_bucket(price) for price in prices) == (
        "00_00_25",
        "01_25_50",
        "01_25_50",
        "01_25_50",
        "01_25_50",
        "01_25_50",
        "01_25_50",
        "02_50_70",
    )

    orders = [
        {
            "order_id": f"order-{index}",
            "final_status": "FILLED",
            "submitted_at": f"2026-07-05T10:{index:02d}:00+00:00",
            "condition_id": f"condition-{index}",
            "side": "YES",
            "limit_price": price,
            "requested_size_usd": 1.0,
            "requested_shares": 2.0,
        }
        for index, price in enumerate(prices)
    ]
    resolutions = {
        f"condition-{index}": {"direction": "UP", "source": "test"}
        for index in range(len(prices))
    }
    truth = build_pnl_truth({"orders": orders}, resolutions)

    assert set(truth["by_price_band"]) == {"00_00_25", "01_25_50", "02_50_70"}
    parent_by_subband = {
        "00_00_25": "00_00_25",
        "01a_25_32": "01_25_50",
        "01b_32_40": "01_25_50",
        "01c_40_50": "01_25_50",
        "02_50_70": "02_50_70",
    }
    for parent, parent_metric in truth["by_price_band"].items():
        child_metrics = [
            metric
            for subband, metric in truth["by_price_subband"].items()
            if parent_by_subband[subband] == parent
        ]
        for key in ("orders", "fills", "resolved_fills", "cost_usd", "payout_usd", "pnl_usd"):
            assert sum(metric[key] for metric in child_metrics) == pytest.approx(parent_metric[key])


def test_pnl_truth_keeps_banked_response_cost_immutable_when_receipt_arrives() -> None:
    order = {
        "order_id": "0xorder",
        "final_status": "FILLED",
        "submitted_at": "2026-07-05T10:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": 4.0,
            "response_fill_size_shares": 10.0,
            "market_order_amount_adjustment_usd": 0.5,
            "tx_hashes": ["0xtx"],
        },
    }

    truth = build_pnl_truth(
        {"orders": [order]},
        {"0xabc": {"direction": "UP"}},
        receipt_costs={"0xtx": 4.75},
    )

    event = truth["events"][0]
    assert event["cost_usd"] == 4.0
    assert event["cost_basis_source"] == "response_filled_size_usd"
    assert event["cost_basis_key"] == ""
    assert event["pnl_usd"] == 6.0
    assert truth["total"]["pnl_usd"] == 6.0
    assert truth["cost_basis_source"] == "response_filled_size_usd"
    assert truth["fallback_cost_basis_source"] == "response_filled_size_usd"
    assert truth["scope"]["cost_basis_counts"] == {"response_filled_size_usd": 1}


def test_pnl_truth_backfills_realized_entry_band_and_day_in_band_fill_rate() -> None:
    orders = []
    for index, (cost, shares) in enumerate(((3.0, 10.0), (1.1, 10.0), (3.5, 10.0))):
        orders.append(
            {
                "order_id": f"fill-{index}",
                "final_status": "FILLED",
                "submitted_at": f"2026-07-05T10:{index:02d}:00+00:00",
                "condition_id": "0xabc",
                "side": "YES",
                "limit_price": 0.30,
                "source_intent": {"metadata": {"copy_model": "drip"}},
                "trade_result": {
                    "response_filled_size_usd": cost,
                    "response_fill_size_shares": shares,
                },
            }
        )

    truth = build_pnl_truth({"orders": orders}, {"0xabc": {"direction": "UP"}})

    assert [event["realized_entry_price"] for event in truth["events"]] == [0.3, 0.11, 0.35]
    assert [event["realized_entry_band"] for event in truth["events"]] == [
        "01a_25_32",
        "00_00_25",
        "01b_32_40",
    ]
    assert [event["out_of_band_fill"] for event in truth["events"]] == [False, True, True]
    assert [event["realized_entry_classification"] for event in truth["events"]] == [
        "RULED_FLOOR_PASS",
        "RULED_FLOOR_BREACH",
        "RULED_FLOOR_PASS",
    ]
    assert all(event["copy_model"] == "drip" for event in truth["events"])
    assert all(
        event["floor_gate_copy_model_in_coverage_set"] is True
        for event in truth["events"]
    )
    assert all(event["floor_gate_enforced"] is False for event in truth["events"])
    day = truth["by_day"]["2026-07-05"]
    assert day["in_band_fill_rate"] == pytest.approx(1 / 3)
    assert day["out_of_band_fill_count"] == 2
    assert day["out_of_band_cost_usd"] == 4.6
    assert day["ruled_floor_breach_count"] == 1
    assert day["ruled_floor_breach_cost_usd"] == 1.1
    assert day["floor_gate_copy_model_in_coverage_set_fill_rate"] == 1.0
    assert day["floor_gate_enforced_fill_rate"] == 0.0
    assert truth["by_price_subband"]["01a_25_32"]["keyed_on"] == "decision_price"
    realized = truth["by_realized_entry_band"]
    assert realized["00_00_25"]["fills"] == 1
    assert realized["00_00_25"]["cost_usd"] == 1.1
    assert realized["00_00_25"]["keyed_on"] == "realized_entry_price"
    assert realized["01a_25_32"]["fills"] == 1
    assert realized["01b_32_40"]["fills"] == 1


@pytest.mark.parametrize(
    ("fill_count", "expected_status", "expected_falsified"),
    ((5, "WATCH", False), (15, "FALSIFIED", True)),
)
def test_pnl_truth_reports_in_band_zero_winner_holdout_tripwire(
    fill_count: int,
    expected_status: str,
    expected_falsified: bool,
) -> None:
    orders = [
        {
            "order_id": f"loser-{index}",
            "final_status": "FILLED",
            "submitted_at": f"2026-08-04T10:{index:02d}:00+00:00",
            "condition_id": f"condition-{index}",
            "side": "YES",
            "limit_price": 0.28,
            "trade_result": {
                "response_filled_size_usd": 2.8,
                "response_fill_size_shares": 10.0,
            },
        }
        for index in range(fill_count)
    ]
    resolutions = {
        f"condition-{index}": {"direction": "DOWN", "source": "test"}
        for index in range(fill_count)
    }

    day = build_pnl_truth({"orders": orders}, resolutions)["by_day"]["2026-08-04"]

    assert day["payout_fill_count"] == 0
    assert day["in_band_resolved_fill_count"] == fill_count
    assert day["in_band_payout_fill_count"] == 0
    assert day["in_band_winner_binomial_lower_tail_p_value"] == pytest.approx(
        (1.0 - 0.29787234) ** fill_count
    )
    assert day["in_band_holdout_live_status"] == expected_status
    assert day["in_band_holdout_live_falsified"] is expected_falsified


def test_pnl_truth_flattens_gate_probe_age_and_source_event_age() -> None:
    order = {
        "order_id": "aged-fill",
        "final_status": "FILLED",
        "submitted_at": "2026-08-04T13:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "limit_price": 0.30,
        "trade_result": {
            "response_filled_size_usd": 1.0,
            "response_fill_size_shares": 4.0,
            "gate_probe_best_ask": 0.26,
            "submit_best_ask_evidence": {
                "gate_probe_best_ask_age_at_gate_s": 0.25,
                "max_gate_probe_best_ask_age_at_gate_s": 1.0,
                "book_age_at_submit_sent_s": 0.5,
                "gate_to_submit_sent_s": 0.25,
                "threshold_independent_of_cache_ttl": True,
                "generation_sha256": "generation-d20",
                "event_age_s": 1.5,
            },
        },
    }

    event = build_pnl_truth(
        {"orders": [order]}, {"0xabc": {"direction": "UP"}}
    )["events"][0]

    assert event["gate_probe_best_ask"] == 0.26
    assert event["gate_probe_best_ask_age_at_gate_s"] == 0.25
    assert event["book_age_at_submit_sent_s"] == 0.5
    assert event["gate_to_submit_sent_s"] == 0.25
    assert event["event_age_s"] == 1.5
    assert event["floor_gate_enforced"] is True
    assert event["floor_gate_generation_sha256"] == "generation-d20"
    assert event["realized_minus_limit_price"] == -0.05
    assert event["realized_minus_gate_probe_best_ask"] == -0.01
    day = build_pnl_truth(
        {"orders": [order]}, {"0xabc": {"direction": "UP"}}
    )["by_day"]["2026-08-04"]
    assert day["book_age_at_submit_sent_s"] == {
        "count": 1,
        "min": 0.5,
        "median": 0.5,
        "max": 0.5,
    }
    assert day["gate_to_submit_sent_s"]["median"] == 0.25
    assert day["threshold_independent_of_cache_ttl"] is True
    assert day["downward_slippage_from_limit"] == {
        "count": 1,
        "min": 0.05,
        "median": 0.05,
        "p95": 0.05,
        "max": 0.05,
    }


def test_pnl_truth_uses_actual_trade_record_cost_when_available() -> None:
    order = {
        "order_id": "0xorder",
        "final_status": "FILLED",
        "submitted_at": "2026-07-07T10:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": 4.0,
            "response_fill_size_shares": 10.0,
            "tx_hashes": ["0xactual"],
        },
    }

    truth = build_pnl_truth(
        {"orders": [order]},
        {"0xabc": {"direction": "UP"}},
        actual_trade_costs={"0xactual": {"actual_cost_usd": 3.25}},
    )

    event = truth["events"][0]
    assert event["cost_usd"] == 3.25
    assert event["intended_cost_usd"] == 4.0
    assert event["price_improvement_usd"] == 0.75
    assert event["cost_basis_source"] == "actual_trade_record"
    assert event["cost_basis_key"] == "0xactual"
    assert event["pnl_usd"] == 6.75
    assert truth["cost_basis_source"] == "actual_trade_record"
    assert truth["scope"]["cost_basis_counts"] == {"actual_trade_record": 1}


def test_pnl_truth_fallback_uses_raw_response_cost_without_actual_trade_record() -> None:
    order = {
        "order_id": "0xorder",
        "final_status": "FILLED",
        "submitted_at": "2026-07-07T10:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": 4.0,
            "response_fill_size_shares": 10.0,
            "market_order_amount_adjustment_usd": 0.5,
            "tx_hashes": ["0xmissing"],
        },
    }

    truth = build_pnl_truth(
        {"orders": [order]},
        {"0xabc": {"direction": "UP"}},
        actual_trade_costs={"0xother": {"actual_cost_usd": 3.25}},
    )

    event = truth["events"][0]
    assert event["cost_usd"] == 4.0
    assert event["intended_cost_usd"] == 4.5
    assert event["price_improvement_usd"] == 0.0
    assert event["cost_basis_source"] == "response_filled_size_usd"
    assert event["pnl_usd"] == 6.0


def test_pnl_truth_market_buy_precision_adjustment_never_reduces_cost_basis() -> None:
    # Regression for the 2026-07-22 13:50Z fill: response principal $1.02 with
    # a -$0.02 precision adjustment must score cost $1.02 (WIN +$3.23 on 4.25
    # shares), not the intended $1.00 (+$3.25).
    order = {
        "order_id": "0xcfa6",
        "final_status": "FILLED",
        "submitted_at": "2026-07-22T13:50:44+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": 1.02,
            "response_fill_size_shares": 4.25,
            "making_amount": 1.02,
            "market_order_amount_adjustment_usd": -0.02,
        },
    }

    truth = build_pnl_truth({"orders": [order]}, {"0xabc": {"direction": "UP"}})

    event = truth["events"][0]
    assert event["cost_usd"] == 1.02
    assert event["intended_cost_usd"] == 1.0
    assert event["cost_basis_source"] == "response_filled_size_usd"
    assert event["pnl_usd"] == 3.23


def test_pnl_truth_rejected_actual_trade_record_falls_back_to_intended_not_receipt() -> None:
    order = {
        "order_id": "0xorder",
        "final_status": "FILLED",
        "submitted_at": "2026-07-07T10:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "actual_trade_cost_rejected_reason": "suspect_actual_improvement_or_missing_size",
        "trade_result": {
            "response_filled_size_usd": 4.0,
            "response_fill_size_shares": 10.0,
            "tx_hashes": ["0xtx"],
        },
    }

    truth = build_pnl_truth(
        {"orders": [order]},
        {"0xabc": {"direction": "UP"}},
        receipt_costs={"0xtx": 4.75},
    )

    event = truth["events"][0]
    assert event["cost_usd"] == 4.0
    assert event["cost_basis_source"] == "response_filled_size_usd"
    assert event["cost_basis_key"] == "actual_trade_rejected_fallback"
    assert event["pnl_usd"] == 6.0


def test_pnl_truth_fallback_cost_ignores_recorded_market_order_adjustment() -> None:
    order = {
        "order_id": "fallback",
        "final_status": "FILLED",
        "submitted_at": "2026-07-05T10:00:00+00:00",
        "condition_id": "0xabc",
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": 4.0,
            "response_fill_size_shares": 10.0,
            "market_order_amount_adjustment_usd": 0.5,
        },
    }

    truth = build_pnl_truth({"orders": [order]}, {"0xabc": {"direction": "UP"}})

    event = truth["events"][0]
    assert event["cost_usd"] == 4.0
    assert event["intended_cost_usd"] == 4.5
    assert event["cost_basis_source"] == "response_filled_size_usd"
    assert event["pnl_usd"] == 6.0


def test_pnl_truth_discrepancy_names_cached_writeback_delta() -> None:
    ledger = {
        "resolution_writeback": {"resolved_filled_orders": 2, "pnl_usd": -47.57},
        "orders": [
            {
                "final_status": "FILLED",
                "submitted_at": "2026-07-05T10:00:00+00:00",
                "condition_id": "0xabc",
                "side": "YES",
                "requested_size_usd": 4.0,
                "requested_shares": 10.0,
                "pnl_usd": 1.23,
            }
        ],
    }
    truth = build_pnl_truth(ledger, {"0xabc": {"direction": "UP"}})

    report = discrepancy_report(ledger, truth)

    assert report["canonical_pnl_usd"] == 6.0
    assert report["itemized_delta_pnl_usd"] == -53.57
    assert report["comparisons"][0]["source"] == "ledger.resolution_writeback"


def test_resolved_pnl_sign_conventions_for_yes_and_no() -> None:
    yes_win, yes_pnl = resolved_pnl(side="YES", shares=10.0, cost=4.0, resolution={"direction": "UP"})
    no_win, no_pnl = resolved_pnl(side="NO", shares=10.0, cost=4.0, resolution={"direction": "DOWN"})
    yes_loss, yes_loss_pnl = resolved_pnl(side="YES", shares=10.0, cost=4.0, resolution={"direction": "DOWN"})
    no_loss, no_loss_pnl = resolved_pnl(side="NO", shares=10.0, cost=4.0, resolution={"direction": "UP"})

    assert yes_win is True
    assert yes_pnl == 6.0
    assert no_win is True
    assert no_pnl == 6.0
    assert yes_loss is True
    assert yes_loss_pnl == -4.0
    assert no_loss is True
    assert no_loss_pnl == -4.0


def test_zero_resolution_snapshot_raises_when_ledger_has_fills() -> None:
    ledger = {"orders": [{"final_status": "FILLED"}]}

    with pytest.raises(RuntimeError, match="zero rows"):
        validate_resolutions_nonempty_for_fills(ledger, {}, resolutions_path="empty.jsonl")


def test_discrepancy_report_itemizes_order_id_amount_formula_delta() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "partial-fill-order",
                "final_status": "FILLED",
                "submitted_at": "2026-07-05T10:00:00+00:00",
                "condition_id": "0xabc",
                "side": "YES",
                "requested_size_usd": 10.0,
                "requested_shares": 20.0,
                "trade_result": {
                    "response_filled_size_usd": 4.0,
                    "response_fill_size_shares": 10.0,
                },
            }
        ],
    }
    truth = build_pnl_truth(ledger, {"0xabc": {"direction": "UP"}})

    report = discrepancy_report(ledger, truth)

    assert report["canonical_pnl_usd"] == 6.0
    assert report["itemized_delta_orders"][0]["order_id"] == "partial-fill-order"
    assert report["itemized_delta_orders"][0]["delta_legacy_vs_canonical_usd"] == 4.0
    assert report["itemized_delta_orders"][0]["cause"] == "legacy_requested_or_max_fill_amount_differs_from_actual_exchange_fill"


def test_fetch_live_position_value_paginates_until_short_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POLYMARKET_PROXY", "0xabc")
    calls: list[str] = []

    class FakeResponse:
        def __init__(self, payload: list[dict[str, str]]) -> None:
            self.payload = payload

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(self.payload).encode("utf-8")

    def fake_urlopen(request: object, timeout: float) -> FakeResponse:
        url = str(getattr(request, "full_url", ""))
        calls.append(url)
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        offset = int(query.get("offset", ["0"])[0])
        if offset == 0:
            return FakeResponse([{"currentValue": "0.1", "initialValue": "0.2", "cashPnl": "0.3"}] * 500)
        return FakeResponse([{"currentValue": "0.4", "initialValue": "0.5", "cashPnl": "0.6"}])

    monkeypatch.setattr(pnl_truth.urllib.request, "urlopen", fake_urlopen)

    result = fetch_live_position_value()

    assert result["status"] == "LIVE_MARK"
    assert result["rows_fetched"] == 501
    assert result["truncated"] is False
    assert result["open_position_value_usd"] == 50.4
    assert "offset=500" in calls[-1]


def test_scorecard_api_creds_prefers_derive_without_create_key() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def derive_api_key(self) -> dict[str, str]:
            self.calls.append("derive_api_key")
            return {"api_key": "key", "api_secret": "secret", "api_passphrase": "pass"}

        def create_or_derive_api_key(self) -> dict[str, str]:
            self.calls.append("create_or_derive_api_key")
            raise AssertionError("scorecard lane must not call create-key when derive exists")

    client = FakeClient()

    creds = pnl_truth._derive_scorecard_api_creds(client)

    assert creds["api_key"] == "key"
    assert client.calls == ["derive_api_key"]


def test_chain_reconciliation_passes_open_unresolved_position_with_cash_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_balance(config: object) -> float:
        return 311.565184

    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.389931,
        unresolved_open_cost_usd=2.31,
        unresolved_max_payout_usd=14.4375,
    )

    assert result["status"] == "PASS"
    assert result["status_basis"] == "cash_identity_with_unresolved_bounds_v2"
    assert result["account_value_basis"] == "live_cash_balance_plus_unresolved_open_cost"
    assert result["cash_delta_vs_expected_identity_usd"] == -0.734885
    assert result["delta_vs_expected_usd"] == -0.734885


def test_chain_reconciliation_passes_paid_unscored_unresolved_win(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_balance(config: object) -> float:
        return 326.737569

    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.389931,
        unresolved_open_cost_usd=2.31,
        unresolved_max_payout_usd=14.4375,
    )

    assert result["status"] == "PASS"
    assert result["status_basis"] == "cash_identity_with_unresolved_bounds_v2"
    assert result["cash_delta_vs_expected_identity_usd"] == 14.4375


def test_chain_reconciliation_mismatches_genuine_cash_shortfall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_balance(config: object) -> float:
        return 312.0

    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )
    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", "0")

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
    )

    assert result["status"] == "MISMATCH"
    assert result["cash_delta_vs_expected_identity_usd"] == -3.0


def test_chain_reconciliation_treats_live_open_mark_as_diagnostic_not_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_balance(config: object) -> float:
        return 313.0

    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 20.0},
    )
    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", "0")

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.0,
        unresolved_open_cost_usd=2.0,
        unresolved_max_payout_usd=14.0,
    )

    assert result["status"] == "PASS"
    assert result["cash_delta_vs_expected_identity_usd"] == 0.0
    assert result["account_value_usd"] == 315.0
    assert result["live_mark_account_value_usd"] == 333.0
    assert result["valuation_basis_adjustment_usd"] == 18.0
    assert result["point_in_time_adjustments"]["basis_unification_usd"] == 18.0
    assert result["point_in_time_adjustments"]["adjusted_delta_vs_expected_usd"] == 0.0


def test_chain_reconciliation_escalates_would_be_mismatch_to_slow_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    samples = iter([312.0, 312.0, 312.0, 315.0, 315.0])

    async def fake_balance(config: object) -> float:
        return next(samples)

    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_COUNT", "3")
    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_INTERVAL_S", "0")
    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_COUNT", "2")
    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", "0")
    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
    )

    assert result["status"] == "PASS"
    assert result["live_cash_balance_usd"] == 315.0
    assert result["balance_sampling"]["escalation"]["triggered"] is True
    assert result["balance_sampling"]["escalation"]["initial_status"] == "MISMATCH"
    assert [row["balance_usd"] for row in result["balance_sampling"]["samples"]] == [312.0, 312.0, 312.0, 315.0, 315.0]


def test_chain_reconciliation_resample_arguments_override_slow_env_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def fake_balance(config: object) -> float:
        nonlocal calls
        calls += 1
        return 312.0

    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_COUNT", "2")
    monkeypatch.setenv("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S", "0")
    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=-20.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
        balance_sample_count=1,
        balance_sample_interval_s=0.0,
        mismatch_resample_count=0,
        mismatch_resample_interval_s=0.0,
    )

    assert result["status"] == "MISMATCH"
    assert calls == 1
    assert result["balance_sampling"]["escalation"]["triggered"] is False
    assert [row["balance_usd"] for row in result["balance_sampling"]["samples"]] == [312.0]


def test_chain_reconciliation_retries_unavailable_balance_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    samples = iter([-1.0, 343.0])

    async def fake_balance(config: object) -> float:
        return next(samples)

    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_COUNT", "1")
    monkeypatch.setenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_COUNT", "1")
    monkeypatch.setenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_INTERVAL_S", "0")
    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=8.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
    )

    assert result["status"] == "PASS"
    assert result["balance_status"] == "OK"
    assert result["live_cash_balance_usd"] == 343.0
    assert result["balance_sampling"]["escalation"]["reason"] == "unavailable_balance_fetch"
    assert result["balance_sampling"]["escalation"]["initial_status"] == "UNAVAILABLE"
    assert [row["balance_usd"] for row in result["balance_sampling"]["samples"]] == [-1.0, 343.0]
    assert result["balance_sampling"]["samples"][1]["unavailable_retry_sample"] is True


def test_chain_reconciliation_keeps_unavailable_after_retry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    samples = iter([-1.0, -1.0])

    async def fake_balance(config: object) -> float:
        return next(samples)

    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_COUNT", "1")
    monkeypatch.setenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_COUNT", "1")
    monkeypatch.setenv("WALLET_COPY_BALANCE_UNAVAILABLE_RESAMPLE_INTERVAL_S", "0")
    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=8.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
    )

    assert result["status"] == "UNAVAILABLE"
    assert result["balance_status"] == "UNAVAILABLE"
    assert result["balance_reason"] == "clob_balance_fetch_returned_negative"
    assert result["balance_sampling"]["escalation"]["reason"] == "unavailable_balance_fetch"
    assert [row["balance_usd"] for row in result["balance_sampling"]["samples"]] == [-1.0, -1.0]


def test_chain_reconciliation_uses_median_balance_sample_and_logs_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    samples = iter([345.0, 343.0, 343.0])

    async def fake_balance(config: object) -> float:
        return next(samples)

    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_COUNT", "3")
    monkeypatch.setenv("WALLET_COPY_BALANCE_SAMPLE_INTERVAL_S", "0")
    monkeypatch.setattr(pnl_truth, "_fetch_live_balance", fake_balance)
    monkeypatch.setattr(
        pnl_truth,
        "fetch_live_position_value",
        lambda: {"status": "LIVE_MARK", "open_position_value_usd": 0.0},
    )

    result = chain_reconciliation(
        baseline_usd=335.0,
        canonical_pnl_usd=8.0,
        unresolved_open_cost_usd=0.0,
        unresolved_max_payout_usd=0.0,
    )

    assert result["status"] == "PASS"
    assert result["live_cash_balance_usd"] == 343.0
    assert result["account_value_usd"] == 343.0
    assert result["balance_sampling"]["strategy"] == "consistent_pair_then_median_valid_balance_samples"
    assert result["balance_sampling"]["selection_method"] == "consistent_pair"
    assert result["balance_sampling"]["consistent_pair_balance_usd"] == [343.0, 343.0]
    assert [row["balance_usd"] for row in result["balance_sampling"]["samples"]] == [345.0, 343.0, 343.0]
