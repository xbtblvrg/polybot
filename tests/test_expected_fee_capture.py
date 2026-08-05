from __future__ import annotations

import time
from pathlib import Path

import scripts.run_wallet_copy_live_execution as live_execution
from src.wallet_copy.execution import LiveExecutionLedgerConfig, LiveWalletCopyLifecycle
from src.wallet_copy.fees import (
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.models import CopyIntent


def _intent(*, price: float = 0.13, shares: float = 26.846152) -> CopyIntent:
    return CopyIntent(
        intent_id="ci_fee_capture",
        source_wallet="0x251c1a283703beed41590b0875a8dcb8ddd1541f",
        wallet_name="fee_capture",
        source_event_id="event_fee_capture",
        condition_id="cond-fee",
        market_slug="btc-updown-5m-1783368000",
        outcome="Down",
        side="NO",
        limit_price=price,
        wallet_usdc_size=round(price * shares, 6),
        copy_size_usd=round(price * shares, 6),
        shares=shares,
        observed_ts=time.time(),
        strategy_family="wallet_copy",
        policy_id="fee_policy",
        sizing_policy_id="fee_size",
        mode="paper",
        action="BUY",
        token_id="token-down",
        event_ts=time.time(),
        live_orders_allowed=False,
        metadata={"copy_model": "inventory"},
    )


def test_expected_fee_formula_matches_receipt_fit() -> None:
    fee = expected_polymarket_buy_fee_usd(shares=26.846152, price=0.13)

    assert fee == 0.0
    assert modeled_unvalidated_polymarket_buy_fee_usd(shares=26.846152, price=0.13) == 0.212534


def test_expected_fee_capture_gate_stamps_intent_without_filtering() -> None:
    stamped, summary = live_execution._apply_expected_fee_capture_gate([_intent()])

    assert len(stamped) == 1
    assert summary["output_intents"] == 1
    assert summary["buy_intents_with_fee_estimate"] == 1
    gate = stamped[0].metadata["expected_fee_gate"]
    assert gate["status"] == "PASS"
    assert gate["threshold_change"] is False
    assert gate["expected_fee_usd"] == 0.0
    assert gate["expected_total_cost_usd"] == 3.49
    assert gate["modeled_unvalidated_fee_usd"] == 0.212534
    assert gate["accounting_authority"] is False


def test_live_ledger_persists_expected_fee_reconciliation(tmp_path: Path) -> None:
    intent = live_execution._apply_expected_fee_capture_gate([_intent()])[0][0]
    lifecycle = LiveWalletCopyLifecycle(
        LiveExecutionLedgerConfig(
            state_path=str(tmp_path / "live_state.json"),
            event_log_path=str(tmp_path / "live_events.jsonl"),
        )
    )

    order = lifecycle.record_result(
        intent=intent,
        trade_decision={},
        parity_capsule={},
        result={
            "order_id": "0xfee",
            "status": "submitted",
            "post_status": "matched",
            "execution_role": "taker",
            "response_fill_size_shares": 26.846152,
            "response_fill_price": 0.13,
            "response_filled_size_usd": 3.489999,
            "making_amount": 3.489999,
            "taking_amount": 26.846152,
        },
    )

    assert order["expected_fee_gate"]["expected_fee_usd"] == 0.0
    assert order["expected_fee_gate"]["modeled_unvalidated_fee_usd"] == 0.212534
    comparison = order["expected_vs_realized_fee"]
    assert comparison["status"] == "MODEL_DEMOTED_NO_REALIZED_FEE"
    assert comparison["response_expected_fee_usd"] == 0.0
    assert comparison["response_modeled_unvalidated_fee_usd"] == 0.212534
    assert comparison["expected_total_cost_usd"] == 3.489999
    assert comparison["accounting_authority"] is False
    assert comparison["realized_fee_source"] == "tx_receipt_pusd_debit_pending_scorecard"


def test_live_ledger_persists_raw_clob_reject_payload(tmp_path: Path) -> None:
    intent = live_execution._apply_expected_fee_capture_gate([_intent(price=0.48, shares=5.0)])[0][0]
    lifecycle = LiveWalletCopyLifecycle(
        LiveExecutionLedgerConfig(
            state_path=str(tmp_path / "live_state.json"),
            event_log_path=str(tmp_path / "live_events.jsonl"),
        )
    )

    order = lifecycle.record_result(
        intent=intent,
        trade_decision={},
        parity_capsule={},
        result={
            "order_id": "0xreject",
            "status": "unfilled",
            "final_status": "unfilled",
            "execution_role": "taker",
            "order_type": "FAK",
            "error_class": "fak_no_match",
            "error": "no orders found to match with FAK order",
            "fill_ratio": 0.0,
            "unfilled_size_usd": 2.4,
        },
    )

    assert order["final_status"] == "REJECTED"
    assert order["raw_clob_reject_payload"]["error_class"] == "fak_no_match"
    assert order["raw_clob_reject_payload"]["order_type"] == "FAK"
    assert order["raw_clob_reject_payload"]["unfilled_size_usd"] == 2.4
