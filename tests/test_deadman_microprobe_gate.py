import argparse

from scripts import probe_deadman_microprobe_gate as probe
from src.wallet_copy.models import WalletEvent


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        min_price=0.25,
        max_price=0.50,
        max_price_for_probe=0.25,
        max_book_fetches=20,
        keep_scored_events=20,
        wallet_fraction=0.10,
        probe_max_order_usd=0.50,
        total_probe_exposure_usd=2.0,
        auto_demote_loss_usd=2.0,
        max_slippage_bps=0.0,
        max_freshest_buy_lag_s=7200.0,
        min_clob_copyable_buys_24h=5,
        min_temporal_history_events_scanned=153000.0,
        fail_closed_on_active_unproven_temporal_slice=True,
        direction_id="unit-direction",
        corrected_joint_gate=False,
    )


def _event(index: int, *, now_s: float, wallet: str = "0x19729634ac5ffcd658f0847b9e8cf7c026a95821") -> WalletEvent:
    return WalletEvent(
        source_wallet=wallet,
        wallet_name="unit",
        row_type="trade",
        action="BUY",
        condition_id=f"0x{index:064x}",
        market_slug=f"btc-updown-5m-{int(now_s) - 300}",
        outcome="Up",
        price=0.30,
        size=10.0,
        usdc_size=3.0,
        event_ts=now_s - 60.0 - index,
        observed_ts=now_s,
        token_id=f"token-{index}",
        asset="BTC",
        duration="5m",
    )


class _NoLiquidityClob:
    def get_book(self, token_id: str) -> dict:
        return {"asset_id": token_id, "asks": [], "bids": []}


class _FillableClob:
    def get_book(self, token_id: str) -> dict:
        return {"asset_id": token_id, "asks": [{"price": "0.30", "size": "100"}], "bids": []}


def test_microprobe_gate_does_not_count_historical_replay_as_current_clob() -> None:
    now_s = 1_783_800_000.0
    wallet = "0x19729634ac5ffcd658f0847b9e8cf7c026a95821"

    row = probe._row_for_wallet(
        wallet,
        events=[],
        fetch_report={"status": "PASS"},
        clearance_by_wallet={
            wallet: {
                "metrics": {
                    "copyable_buy_events": 21,
                    "candidate_clob_backed_orders": 21,
                    "paper_pnl_usd": 6.849388,
                }
            }
        },
        queue_by_wallet={},
        rotation_by_wallet={},
        clob=_NoLiquidityClob(),
        now_s=now_s,
        args=_args(),
    )

    assert row["historical_replay_reference_not_gate"]["clearance_copyable_buy_events"] == 21
    assert row["current_clob_backed"]["copyable_buy_count_24h"] == 0
    assert row["gate"]["pass"] is False
    assert "freshest_buy_lag_gt_7200_or_no_current_buy" in row["gate"]["reasons"]
    assert "current_clob_backed_copyable_buys_24h_below_5" in row["gate"]["reasons"]


def test_microprobe_gate_passes_with_fresh_current_book_backed_buys() -> None:
    now_s = 1_783_800_000.0
    wallet = "0x19729634ac5ffcd658f0847b9e8cf7c026a95821"
    events = [_event(index, now_s=now_s, wallet=wallet) for index in range(5)]

    row = probe._row_for_wallet(
        wallet,
        events=events,
        fetch_report={"status": "PASS"},
        clearance_by_wallet={},
        queue_by_wallet={},
        rotation_by_wallet={},
        clob=_FillableClob(),
        now_s=now_s,
        args=_args(),
    )

    assert row["freshness"]["freshest_buy_lag_s"] <= 7200.0
    assert row["current_clob_backed"]["copyable_buy_count_24h"] == 5
    assert row["gate"] == {"freshness_pass": True, "clob_copyable_pass": True, "pass": True, "reasons": []}


def test_corrected_joint_gate_logs_eligibility_and_prior_live_abort(monkeypatch) -> None:
    now_s = 1_783_800_000.0
    wallet = "0x59603775762a631d4bcd156980c0a174bbc4c2d2"
    args = _args()
    args.corrected_joint_gate = True
    events = [_event(index, now_s=now_s, wallet=wallet) for index in range(5)]
    monkeypatch.setattr(probe, "_temporal_slice_exclusion", lambda member: {"excluded": False, "reason": None})

    row = probe._row_for_wallet(
        wallet,
        events=events,
        fetch_report={"status": "PASS"},
        clearance_by_wallet={},
        queue_by_wallet={},
        rotation_by_wallet={},
        clob=_FillableClob(),
        now_s=now_s,
        args=args,
        prior_abort_by_wallet={
            wallet: {
                "ts": "2026-07-10T04:36:23Z",
                "pnl": -4.327037,
                "rule": "worse_of_abort_lte_usd <= -2.50",
            }
        },
    )
    decision = probe._decision_for_rows([row], args)

    assert row["gate"]["pass"] is True
    assert row["prior_live_abort"]["pnl"] == -4.327037
    assert decision["action"] == "ELIGIBLE_PENDING_FABLE_RULING"
    assert decision["best_wallet"] == wallet


def test_corrected_joint_gate_fails_closed_on_active_unproven_temporal_slice(monkeypatch) -> None:
    now_s = 1_783_800_000.0
    wallet = "0x59603775762a631d4bcd156980c0a174bbc4c2d2"
    args = _args()
    args.corrected_joint_gate = True
    events = [_event(index, now_s=now_s, wallet=wallet) for index in range(5)]
    monkeypatch.setattr(
        probe,
        "_temporal_slice_exclusion",
        lambda member: {
            "excluded": False,
            "reason": "no_active_temporal_slice_proven_negative",
            "active_slices": ["weekend", "dead_band_18_22_utc"],
            "evaluated_slices": [
                {
                    "slice": "weekend",
                    "label": "UNPROVEN",
                    "resolved_trades": 0,
                    "pnl_usd": 0.0,
                    "roi_pct": None,
                    "label_reason": "n=0 below min_trades=5; roi=None",
                },
                {
                    "slice": "dead_band_18_22_utc",
                    "label": "PROVEN-POSITIVE",
                    "resolved_trades": 45,
                    "pnl_usd": 49.78406,
                    "roi_pct": 29.249669,
                    "label_reason": "n=45 >= min_trades=3; roi=29.249669 > 0",
                },
            ],
        },
    )

    row = probe._row_for_wallet(
        wallet,
        events=events,
        fetch_report={"status": "PASS"},
        clearance_by_wallet={},
        queue_by_wallet={},
        rotation_by_wallet={},
        clob=_FillableClob(),
        now_s=now_s,
        args=args,
        temporal_registry_basis={"events_scanned": 154091, "generated_at": "2026-07-11T20:54:26Z"},
    )
    decision = probe._decision_for_rows([row], args)

    assert row["corrected_joint_gate"]["corrected_prong2_pass"] is True
    assert row["corrected_joint_gate"]["temporal_slice_gate_pass"] is False
    assert row["corrected_joint_gate"]["temporal_integrity"]["active_unproven_slices"][0]["slice"] == "weekend"
    assert "temporal_slice_active_unproven_basis" in row["gate"]["reasons"]
    assert decision["action"] == "NO_ELIGIBLE_WALLET"


def test_microprobe_decision_never_emits_promotion_action_in_legacy_mode() -> None:
    args = _args()
    args.corrected_joint_gate = False

    decision = probe._decision_for_rows(
        [{"wallet": "0x59603775762a631d4bcd156980c0a174bbc4c2d2", "gate": {"pass": True}}],
        args,
    )

    assert decision["action"] == "ELIGIBLE_PENDING_FABLE_RULING"
    assert not decision["action"].startswith("PROMOTE")
