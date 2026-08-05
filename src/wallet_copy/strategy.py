"""Copy-intent generation policies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.wallet_copy.models import CopyIntent, SizingPolicy, WalletEvent, WalletSpec, num


WEIRD_PEAK_WALLET = "0x9f5ffe76a818dce37c70f947998b52b70671a008"


def outcome_to_side(outcome: str) -> str:
    value = str(outcome or "").strip().lower()
    if value in {"up", "yes"}:
        return "YES"
    if value in {"down", "no"}:
        return "NO"
    raise ValueError(f"unsupported wallet outcome for copy intent: {outcome!r}")


@dataclass(frozen=True)
class CopyPolicy:
    policy_id: str = "exact_copy_all_buys"
    strategy_family: str = "wallet_copy_exact_v1"
    allowed_assets: tuple[str, ...] = ("BTC",)
    market_filter: str = "btc_5m"
    min_price: float = 0.01
    max_price: float = 1.0
    max_event_age_s: float = 0.0
    order_type: str = "PAPER_SOURCE_FILL"
    sizing: SizingPolicy = SizingPolicy()

    def accepts(self, event: WalletEvent, *, now_ts: float | None = None) -> tuple[bool, str]:
        if not event.is_buy:
            return False, "not_buy"
        if not event.condition_id:
            return False, "missing_condition_id"
        if not event.outcome:
            return False, "missing_outcome"
        try:
            outcome_to_side(event.outcome)
        except ValueError:
            return False, "unsupported_outcome"
        if not (float(self.min_price) <= float(event.price) <= float(self.max_price)):
            return False, "price_outside_policy"
        if self.allowed_assets and event.asset and event.asset.upper() not in {item.upper() for item in self.allowed_assets}:
            return False, "asset_not_allowed"
        if self.market_filter == "btc_5m" and event.asset and event.duration:
            if event.asset.upper() != "BTC" or event.duration != "5m":
                return False, "market_filter_mismatch"
        if self.max_event_age_s > 0 and event.event_ts is not None and now_ts is not None:
            if max(0.0, now_ts - event.event_ts) > float(self.max_event_age_s):
                return False, "event_too_old"
        if self.sizing.size_usd(event) <= 0:
            return False, "size_zero"
        return True, "accepted"


def event_to_intent(
    event: WalletEvent,
    *,
    policy: CopyPolicy | None = None,
    mode: str = "paper",
    now_ts: float | None = None,
) -> CopyIntent | None:
    copy_policy = policy or CopyPolicy()
    accepted, reason = copy_policy.accepts(event, now_ts=now_ts)
    if not accepted:
        return None
    side = outcome_to_side(event.outcome)
    copy_size = copy_policy.sizing.size_usd(event)
    shares = round(copy_size / float(event.price), 6) if event.price > 0 else 0.0
    return CopyIntent(
        source_wallet=event.source_wallet,
        wallet_name=event.wallet_name,
        source_event_id=event.event_id,
        condition_id=event.condition_id,
        market_slug=event.market_slug,
        market_id=event.market_id,
        outcome=event.outcome,
        side=side,
        limit_price=round(float(event.price), 6),
        wallet_usdc_size=round(float(event.usdc_size), 6),
        copy_size_usd=copy_size,
        shares=shares,
        observed_ts=event.observed_ts,
        strategy_family=copy_policy.strategy_family,
        policy_id=copy_policy.policy_id,
        sizing_policy_id=copy_policy.sizing.policy_id,
        mode="live" if mode == "live" else "paper",
        order_type=copy_policy.order_type,
        token_id=event.token_id,
        event_ts=event.event_ts,
        api_latency_s=event.api_latency_s,
        live_orders_allowed=False,
        reason=reason,
        metadata={
            "row_type": event.row_type,
            "asset": event.asset,
            "duration": event.duration,
            "source_fingerprint": event.source_fingerprint,
            "transaction_hash": event.transaction_hash,
            "wallet_size": event.size,
            "wallet_copy_policy": {
                "min_price": copy_policy.min_price,
                "max_price": copy_policy.max_price,
                "max_event_age_s": copy_policy.max_event_age_s,
            },
        },
    )


def build_intents(
    events: list[WalletEvent],
    *,
    policy: CopyPolicy | None = None,
    mode: str = "paper",
    now_ts: float | None = None,
) -> list[CopyIntent]:
    intents: dict[str, CopyIntent] = {}
    for event in events:
        intent = event_to_intent(event, policy=policy, mode=mode, now_ts=now_ts)
        if intent is not None:
            intents[intent.intent_id] = intent
    return sorted(intents.values(), key=lambda intent: (intent.event_ts or 0.0, intent.intent_id))


def wallet_spec_from_mapping(value: dict[str, Any]) -> WalletSpec:
    return WalletSpec(
        name=str(value.get("name") or value.get("wallet_name") or value.get("address") or "wallet"),
        address=str(value.get("address") or value.get("wallet") or value.get("target_wallet") or ""),
        enabled=bool(value.get("enabled", True)),
        data_api=str(value.get("data_api") or "https://data-api.polymarket.com"),
        market_filter=str(value.get("market_filter") or "btc_5m"),
        asset_allowlist=tuple(str(item).upper() for item in value.get("asset_allowlist", ["BTC"])),
        tags=tuple(str(item) for item in value.get("tags", [])),
        notes=str(value.get("notes") or ""),
    )


def sizing_from_mapping(value: dict[str, Any]) -> SizingPolicy:
    return SizingPolicy(
        policy_id=str(value.get("policy_id") or value.get("sizing_policy_id") or "wallet_fraction_1_cap_0"),
        basis=str(value.get("basis") or "wallet_usdc_fraction"),  # type: ignore[arg-type]
        wallet_fraction=num(value.get("wallet_fraction", value.get("wallet_size_fraction", 1.0)), 1.0),
        fixed_usd=num(value.get("fixed_usd", value.get("order_usd", 1.0)), 1.0),
        max_order_usd=num(value.get("max_order_usd"), 0.0),
        min_order_usd=num(value.get("min_order_usd"), 0.0),
    )
