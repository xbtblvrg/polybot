from __future__ import annotations

from src.wallet_copy.models import CopyIntent
from src.wallet_copy.portfolio_allocator import (
    PortfolioAllocatorConfig,
    allocate_portfolio_intents,
)
from scripts.audit_wallet_copy_dispatch_throughput import build_dispatch_throughput_audit
from scripts.run_portfolio_allocator_paper_shadow import _load_shadow_event_intents, build_paper_shadow_report


def _intent(idx: int, *, wallet: str | None = None, size_usd: float = 10.0) -> CopyIntent:
    source_wallet = wallet or f"0x{idx + 1:040x}"
    return CopyIntent(
        source_wallet=source_wallet,
        wallet_name=f"member-{idx}",
        source_event_id=f"event-{idx}",
        condition_id=f"condition-{idx % 2}",
        market_slug=f"btc-updown-5m-{1_783_600_000 + idx * 300}",
        outcome="Up",
        side="YES",
        limit_price=0.50,
        wallet_usdc_size=size_usd / 0.10,
        copy_size_usd=size_usd,
        shares=round(size_usd / 0.50, 6),
        observed_ts=1_783_600_001.0,
        token_id=f"token-{idx}",
        metadata={"portfolio_member_id": source_wallet.lower(), "copy_model": "drip"},
    )


def test_portfolio_allocator_gives_floor_then_score_weighted_pro_rata() -> None:
    intents = [_intent(0), _intent(1), _intent(2)]
    scores = {
        intents[0].source_wallet.lower(): {"portfolio_score": 10},
        intents[1].source_wallet.lower(): {"portfolio_score": 5},
        intents[2].source_wallet.lower(): {"portfolio_score": 1},
    }

    result = allocate_portfolio_intents(
        intents,
        member_scores=scores,
        available_cash_usd=9.0,
        now_ts=1_783_600_000.0,
        config=PortfolioAllocatorConfig(min_member_allocation_usd=1.0, min_intent_allocation_usd=1.0),
    )

    by_member = {row["source_wallet"]: row for row in result.member_allocations}
    assert result.status == "PASS"
    assert result.allocated_usd == 9.0
    assert result.starved_member_count == 0
    assert all(row["allocated_usd"] >= 1.0 for row in by_member.values())
    assert by_member[intents[0].source_wallet.lower()]["allocated_usd"] > by_member[
        intents[1].source_wallet.lower()
    ]["allocated_usd"]
    assert by_member[intents[1].source_wallet.lower()]["allocated_usd"] > by_member[
        intents[2].source_wallet.lower()
    ]["allocated_usd"]
    assert result.scaled_intents[0].intent_id != intents[0].intent_id
    assert result.scaled_intents[0].metadata["portfolio_allocation"]["budget_usd"] == 9.0


def test_portfolio_allocator_prioritizes_scores_when_budget_below_member_floors() -> None:
    intents = [_intent(0), _intent(1), _intent(2)]
    scores = {
        intents[0].source_wallet.lower(): {"portfolio_score": 100},
        intents[1].source_wallet.lower(): {"portfolio_score": 50},
        intents[2].source_wallet.lower(): {"portfolio_score": 1},
    }

    result = allocate_portfolio_intents(
        intents,
        member_scores=scores,
        available_cash_usd=2.0,
        now_ts=1_783_600_000.0,
        config=PortfolioAllocatorConfig(min_member_allocation_usd=1.0, min_intent_allocation_usd=1.0),
    )

    by_member = {row["source_wallet"]: row for row in result.member_allocations}
    assert result.status == "ANALYZE"
    assert result.allocated_member_count == 2
    assert result.starved_member_count == 1
    assert by_member[intents[0].source_wallet.lower()]["allocated_usd"] == 1.0
    assert by_member[intents[1].source_wallet.lower()]["allocated_usd"] == 1.0
    assert by_member[intents[2].source_wallet.lower()]["allocated_usd"] == 0.0
    deferred = [row for row in result.intent_allocations if row.status == "DEFERRED"]
    assert deferred[0].reason == "budget_below_member_floor"


def test_portfolio_allocator_counts_five_minute_recycling_and_book_room() -> None:
    intents = [_intent(0), _intent(1), _intent(2)]

    result = allocate_portfolio_intents(
        intents,
        member_scores={intent.source_wallet.lower(): 1 for intent in intents},
        available_cash_usd=5.0,
        existing_exposure_usd=1.0,
        recycle_credits=[
            {"amount_usd": 3.0, "available_at_ts": 1_783_600_100.0, "source": "btc5m_resolve"},
            {"amount_usd": 10.0, "available_at_ts": 1_783_601_000.0, "source": "outside_horizon"},
        ],
        now_ts=1_783_600_000.0,
        config=PortfolioAllocatorConfig(
            min_member_allocation_usd=1.0,
            min_intent_allocation_usd=1.0,
            max_concurrent_book_usd=7.0,
        ),
    )

    assert result.budget_usd == 6.0
    assert result.allocated_usd == 6.0
    assert result.summary["recyclable_cash_usd"] == 3.0
    assert result.summary["book_room_usd"] == 6.0


def test_dispatch_throughput_audit_passes_100_source_single_guard_burst() -> None:
    report = build_dispatch_throughput_audit(
        signal_count=100,
        available_cash_usd=125.0,
        max_intents_per_cycle=6,
        guard_cycle_interval_s=0.5,
        per_order_submit_budget_s=0.15,
        max_api_requests_per_s=25.0,
        target_drain_s=30.0,
    )

    assert report["status"] == "PASS"
    assert report["single_submitter_invariant"] == "single_guard_serial_submitter_preserved"
    assert report["allocator"]["allocated_member_count"] == 100
    assert report["allocator"]["starved_member_count"] == 0
    assert report["dispatch_model"]["estimated_drain_s"] <= 30.0
    assert report["api_budget"]["estimated_api_requests_per_s"] <= 25.0
    assert report["fairness"]["all_members_receive_floor_when_cash_sufficient"] is True


def test_portfolio_allocator_paper_shadow_records_measured_and_modeled_timers() -> None:
    intents = [_intent(idx) for idx in range(12)]

    report = build_paper_shadow_report(
        intents,
        source="unit-test",
        available_cash_usd=24.0,
        reserve_cash_usd=0.0,
        min_member_allocation_usd=1.0,
        min_intent_allocation_usd=1.0,
        max_intents_per_cycle=6,
        guard_cycle_interval_s=0.5,
        per_order_submit_budget_s=0.15,
        max_api_requests_per_s=25.0,
        target_drain_s=30.0,
        real_arrival_gate_max_s=47.0,
    )

    assert report["status"] == "PASS"
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["allocator"]["starved_member_count"] == 0
    assert report["timing"]["cycle_count"] == 2
    assert report["timing"]["modeled_guard_drain_s"] > 0
    assert report["timing"]["measured_python_drain_s"] >= 0


def test_portfolio_allocator_paper_shadow_replays_densest_real_arrivals(tmp_path) -> None:
    path = tmp_path / "shadow.jsonl"
    rows = []
    for idx, arrival in enumerate([100.0, 210.0, 211.0, 212.0]):
        rows.append(
            {
                "kind": "wallet_copy_realtime_shadow_watch_event",
                "event_id": f"rt-{idx}",
                "wallet": f"0x{idx + 1:040x}",
                "market_slug": "btc-updown-5m-1783429500",
                "token_id": f"token-{idx}",
                "received_at_s": arrival,
                "source_ts": arrival - 0.5,
                "source_price": 0.45,
                "drip_tranche_usd": 1.0,
                "parity_limit_price": 0.50,
                "parity_fillable": True,
                "parity": {"copy_size_usd": 1.0, "max_copy_price": 0.50, "book_market": "condition"},
            }
        )
    path.write_text("\n".join(__import__("json").dumps(row) for row in rows) + "\n")

    intents, summary = _load_shadow_event_intents(str(path), max_events=3, selection="densest")
    report = build_paper_shadow_report(
        intents,
        source="unit-shadow",
        source_summary=summary,
        available_cash_usd=10.0,
        reserve_cash_usd=0.0,
        min_member_allocation_usd=1.0,
        min_intent_allocation_usd=1.0,
        max_intents_per_cycle=2,
        guard_cycle_interval_s=0.5,
        per_order_submit_budget_s=0.15,
        max_api_requests_per_s=25.0,
        target_drain_s=30.0,
        real_arrival_gate_max_s=47.0,
    )

    assert summary["rows_loaded"] == 4
    assert summary["rows_selected"] == 3
    assert summary["selected_arrival_span_s"] == 2.0
    assert report["arrival_profile"]["arrival_span_s"] == 2.0
    assert report["timing"]["cycle_count"] == 2
    assert report["timing"]["simulated_cycle_count"] == 3
    assert report["timing"]["simulated_guard_drain_s"] >= report["arrival_profile"]["arrival_span_s"]
