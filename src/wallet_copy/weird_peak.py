"""Weird-Peak bridge for the canonical wallet-copy stack."""

from __future__ import annotations

from typing import Any

from src.wallet_copy.models import CopyIntent
from src.wallet_copy.strategy import WEIRD_PEAK_WALLET
from src.weird_peak_exact_copy_paper_flow import WeirdPeakExactCopyConfig, run_exact_copy_once
from src.weird_peak_wallet_tracker import WalletTrackConfig, WeirdPeakWalletTracker


def run_weird_peak_exact_copy_paper_once(
    *,
    wallet: str = WEIRD_PEAK_WALLET,
    limit: int = 500,
    tracker_state_path: str = "data/research/weird_peak_exact_copy_wallet_tracker_state.json",
    tracker_event_log_path: str = "data/research/weird_peak_exact_copy_wallet_tracker_events.jsonl",
    paper_state_path: str = "data/research/weird_peak_exact_copy_paper_flow_state.json",
    paper_event_log_path: str = "data/research/weird_peak_exact_copy_paper_flow_events.jsonl",
    wallet_size_fraction: float = 1.0,
    max_order_usd: float = 0.0,
    enable_fast_preconfirm: bool = True,
) -> dict[str, Any]:
    tracker = WeirdPeakWalletTracker(
        WalletTrackConfig(
            target_wallet=wallet,
            poll_limit=limit,
            btc_5m_only=True,
            event_log_path=tracker_event_log_path,
            state_path=tracker_state_path,
        )
    )
    payload = tracker.poll_once()
    return run_exact_copy_once(
        payload,
        WeirdPeakExactCopyConfig(
            target_wallet=wallet,
            state_path=paper_state_path,
            event_log_path=paper_event_log_path,
            tracker_state_path=tracker_state_path,
            wallet_size_fraction=wallet_size_fraction,
            max_order_usd=max_order_usd,
            enable_fast_preconfirm=enable_fast_preconfirm,
        ),
    )


def intents_from_weird_peak_state(state: dict[str, Any]) -> list[CopyIntent]:
    intents: list[CopyIntent] = []
    for order in state.get("paper_orders") or []:
        if not isinstance(order, dict):
            continue
        if order.get("wallet_attribution_live_admissible") is not True:
            continue
        price = float(order.get("limit_price") or order.get("wallet_price") or 0.0)
        size_usd = float(order.get("size_usd") or 0.0)
        if price <= 0 or size_usd <= 0:
            continue
        outcome = str(order.get("outcome") or order.get("wallet_outcome") or "")
        side = "YES" if outcome == "Up" else "NO" if outcome == "Down" else ""
        if not side:
            continue
        intents.append(
            CopyIntent(
                source_wallet=str(order.get("target_wallet") or WEIRD_PEAK_WALLET).lower(),
                wallet_name="weird_peak",
                source_event_id=str(order.get("paper_order_id") or order.get("canonical_key")),
                condition_id=str(order.get("condition_id") or order.get("wallet_condition_id") or ""),
                market_slug=str(order.get("market_slug") or ""),
                market_id=str(order.get("market_id") or ""),
                outcome=outcome,
                side=side,
                limit_price=price,
                wallet_usdc_size=float(order.get("wallet_usdc_size") or size_usd),
                copy_size_usd=size_usd,
                shares=round(size_usd / price, 6),
                observed_ts=0.0,
                strategy_family="wallet_copy_exact_v1",
                policy_id="weird_peak_exact_copy_adapted",
                sizing_policy_id=str((order.get("sizing_policy") or {}).get("type") or "weird_peak_size"),
                order_type="PAPER_SOURCE_FILL",
                token_id=str(order.get("token_id") or order.get("wallet_token_id") or ""),
                event_ts=None,
                api_latency_s=order.get("api_latency_s"),
                metadata={"source_weird_peak_order": order},
            )
        )
    return intents
