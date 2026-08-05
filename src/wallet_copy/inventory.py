"""Multi-wallet inventory plan construction."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from src.wallet_copy.consensus import unique_intents, wallet_outcome_posture
from src.wallet_copy.models import CopyIntent, num, stable_id


@dataclass(frozen=True)
class InventoryConfig:
    min_agreeing_wallets: int = 2
    allow_opposing_wallets: bool = False
    max_price_spread: float = 0.08
    max_window_usd: float = 10.0
    max_per_wallet_usd: float = 2.0
    min_plan_usd: float = 1.0
    strategy_family: str = "wallet_copy_multi_wallet_inventory_v1"
    policy_id: str = "same_market_same_outcome_inventory"

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InventoryPlan:
    plan_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    wallet_names: tuple[str, ...]
    source_wallets: tuple[str, ...]
    child_intent_ids: tuple[str, ...]
    child_orders: tuple[dict[str, Any], ...]
    average_price: float
    total_usd: float
    total_shares: float
    status: str
    blockers: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _market_key(intent: CopyIntent) -> str:
    return intent.condition_id or intent.market_slug


def _scaled_child_orders(group: list[CopyIntent], cfg: InventoryConfig) -> tuple[list[dict[str, Any]], float, float]:
    capped = []
    for intent in group:
        child_usd = float(intent.copy_size_usd)
        if cfg.max_per_wallet_usd > 0:
            child_usd = min(child_usd, float(cfg.max_per_wallet_usd))
        if child_usd <= 0 or intent.limit_price <= 0:
            continue
        capped.append((intent, child_usd))
    total = sum(size for _, size in capped)
    scale = 1.0
    if cfg.max_window_usd > 0 and total > float(cfg.max_window_usd):
        scale = float(cfg.max_window_usd) / total
    rows: list[dict[str, Any]] = []
    total_usd = 0.0
    total_shares = 0.0
    for intent, child_usd in capped:
        scaled_usd = round(child_usd * scale, 6)
        scaled_shares = round(scaled_usd / float(intent.limit_price), 6)
        if scaled_usd <= 0:
            continue
        total_usd += scaled_usd
        total_shares += scaled_shares
        rows.append(
            {
                "source_wallet": intent.source_wallet.lower(),
                "wallet_name": intent.wallet_name,
                "source_intent_id": intent.intent_id,
                "source_event_id": intent.source_event_id,
                "token_id": intent.token_id,
                "market_id": intent.market_id,
                "limit_price": intent.limit_price,
                "wallet_usdc_size": intent.wallet_usdc_size,
                "requested_copy_size_usd": intent.copy_size_usd,
                "inventory_size_usd": scaled_usd,
                "inventory_shares": scaled_shares,
                "observed_ts": intent.observed_ts,
                "event_ts": intent.event_ts,
            }
        )
    return rows, round(total_usd, 6), round(total_shares, 6)


def build_inventory_plans(
    intents: list[CopyIntent],
    *,
    config: InventoryConfig | None = None,
) -> list[InventoryPlan]:
    cfg = config or InventoryConfig()
    by_market: dict[str, list[CopyIntent]] = {}
    for intent in unique_intents(intents):
        by_market.setdefault(_market_key(intent), []).append(intent)
    plans: list[InventoryPlan] = []
    for market_key, market_intents in by_market.items():
        posture = wallet_outcome_posture(market_intents)
        hedged_wallets = sorted(wallet for wallet, row in posture.items() if row.get("is_hedged_or_exit"))
        outcomes = sorted({intent.outcome for intent in market_intents if intent.outcome})
        for outcome in outcomes:
            group = [
                intent
                for intent in market_intents
                if intent.outcome == outcome and not posture.get(intent.source_wallet.lower(), {}).get("is_hedged_or_exit")
            ]
            wallets = sorted({intent.source_wallet.lower() for intent in group})
            opposing_wallets = sorted(
                {
                    intent.source_wallet.lower()
                    for intent in market_intents
                    if intent.outcome != outcome and intent.source_wallet.lower() not in wallets
                    and not posture.get(intent.source_wallet.lower(), {}).get("is_hedged_or_exit")
                }
            )
            prices = [float(intent.limit_price) for intent in group if intent.limit_price > 0]
            child_orders, total_usd, total_shares = _scaled_child_orders(group, cfg)
            blockers: list[str] = []
            if len(wallets) < int(cfg.min_agreeing_wallets):
                blockers.append("insufficient_agreeing_wallets")
            if hedged_wallets and not cfg.allow_opposing_wallets:
                blockers.append("hedged_or_exit_wallets_present")
            if opposing_wallets and not cfg.allow_opposing_wallets:
                blockers.append("opposing_wallets_present")
            if prices and max(prices) - min(prices) > float(cfg.max_price_spread):
                blockers.append("price_spread_too_wide")
            if total_usd < float(cfg.min_plan_usd):
                blockers.append("inventory_size_below_minimum")
            status = "PASS" if not blockers else "BLOCKED"
            plan_id = stable_id(
                "ip",
                {
                    "market": market_key,
                    "outcome": outcome,
                    "wallets": wallets,
                    "child_intents": sorted(intent.intent_id for intent in group),
                    "policy_id": cfg.policy_id,
                },
            )
            plans.append(
                InventoryPlan(
                    plan_id=plan_id,
                    condition_id=group[0].condition_id if group else market_key,
                    market_slug=group[0].market_slug if group else "",
                    outcome=outcome,
                    side=group[0].side if group else "",
                    wallet_names=tuple(sorted({intent.wallet_name for intent in group})),
                    source_wallets=tuple(wallets),
                    child_intent_ids=tuple(sorted(intent.intent_id for intent in group)),
                    child_orders=tuple(child_orders),
                    average_price=round(sum(prices) / len(prices), 6) if prices else 0.0,
                    total_usd=round(total_usd, 6),
                    total_shares=round(total_shares, 6),
                    status=status,
                    blockers=tuple(blockers),
                    metadata={
                        "strategy_family": cfg.strategy_family,
                        "policy_id": cfg.policy_id,
                        "opposing_wallets": opposing_wallets,
                        "hedged_wallets": hedged_wallets,
                        "wallet_posture": posture,
                        "config": cfg.asdict(),
                    },
                )
            )
    return sorted(plans, key=lambda plan: (plan.status != "PASS", plan.condition_id, plan.outcome))


def inventory_plan_to_intent(plan: InventoryPlan) -> CopyIntent | None:
    if plan.status != "PASS" or plan.total_usd <= 0 or plan.average_price <= 0:
        return None
    child_orders = [row for row in plan.child_orders if isinstance(row, dict)]
    first_child = child_orders[0] if child_orders else {}
    observed_ts = max((num(row.get("observed_ts")) for row in child_orders), default=0.0)
    event_ts = max((num(row.get("event_ts")) for row in child_orders), default=0.0) or None
    return CopyIntent(
        intent_id=stable_id("ci", {"inventory_plan_id": plan.plan_id}),
        source_wallet="INVENTORY",
        wallet_name="+".join(plan.wallet_names),
        source_event_id=plan.plan_id,
        condition_id=plan.condition_id,
        market_slug=plan.market_slug,
        outcome=plan.outcome,
        side=plan.side,
        limit_price=plan.average_price,
        wallet_usdc_size=plan.total_usd,
        copy_size_usd=plan.total_usd,
        shares=round(num(plan.total_shares), 6),
        observed_ts=observed_ts,
        strategy_family=str(plan.metadata.get("strategy_family") or "wallet_copy_multi_wallet_inventory_v1"),
        policy_id=str(plan.metadata.get("policy_id") or "same_market_same_outcome_inventory"),
        sizing_policy_id="inventory_scaled_sum",
        mode="paper",
        order_type="PAPER_SOURCE_FILL",
        token_id=str(first_child.get("token_id") or ""),
        market_id=str(first_child.get("market_id") or ""),
        event_ts=event_ts,
        metadata={"inventory_plan": plan.asdict()},
    )


def inventory_plan_child_intents(plan: InventoryPlan, *, policy_id: str | None = None) -> list[CopyIntent]:
    """Expand a passing inventory plan into the child orders it actually copies.

    The aggregate plan intent is useful as a compact window posture, but live
    readiness is about copying wallet orders. Returning one CopyIntent per
    child order keeps paper scoring, window density, and later live parity tied
    to the same order surface.
    """

    if plan.status != "PASS" or plan.total_usd <= 0 or plan.average_price <= 0:
        return []
    child_orders = [row for row in plan.child_orders if isinstance(row, dict)]
    if not child_orders:
        return []
    plan_policy_id = str(policy_id or plan.metadata.get("policy_id") or "same_market_same_outcome_inventory")
    strategy_family = str(plan.metadata.get("strategy_family") or "wallet_copy_multi_wallet_inventory_v1")
    plan_summary = {
        "plan_id": plan.plan_id,
        "condition_id": plan.condition_id,
        "market_slug": plan.market_slug,
        "outcome": plan.outcome,
        "side": plan.side,
        "source_wallets": list(plan.source_wallets),
        "wallet_names": list(plan.wallet_names),
        "child_order_count": len(child_orders),
        "average_price": plan.average_price,
        "total_usd": plan.total_usd,
        "total_shares": plan.total_shares,
        "metadata": {
            "strategy_family": strategy_family,
            "policy_id": plan_policy_id,
            "opposing_wallets": (plan.metadata or {}).get("opposing_wallets") or [],
            "hedged_wallets": (plan.metadata or {}).get("hedged_wallets") or [],
        },
    }
    intents: list[CopyIntent] = []
    for index, child in enumerate(child_orders):
        price = num(child.get("limit_price"))
        size_usd = num(child.get("inventory_size_usd"))
        shares = num(child.get("inventory_shares")) or (round(size_usd / price, 6) if price > 0 else 0.0)
        if price <= 0 or size_usd <= 0 or shares <= 0:
            continue
        source_wallet = str(child.get("source_wallet") or "").lower()
        source_event_id = str(child.get("source_event_id") or child.get("source_intent_id") or f"{plan.plan_id}:{index}")
        intents.append(
            CopyIntent(
                intent_id=stable_id(
                    "ci",
                    {
                        "inventory_plan_id": plan.plan_id,
                        "child_source_intent_id": child.get("source_intent_id"),
                        "child_source_event_id": source_event_id,
                        "child_index": index,
                        "policy_id": plan_policy_id,
                        "size_usd": round(size_usd, 6),
                    },
                ),
                source_wallet=source_wallet or "INVENTORY",
                wallet_name=str(child.get("wallet_name") or source_wallet or "inventory_child"),
                source_event_id=source_event_id,
                condition_id=plan.condition_id,
                market_slug=plan.market_slug,
                outcome=plan.outcome,
                side=plan.side,
                limit_price=price,
                wallet_usdc_size=num(child.get("wallet_usdc_size")),
                copy_size_usd=round(size_usd, 6),
                shares=round(shares, 6),
                observed_ts=num(child.get("observed_ts")),
                strategy_family=strategy_family,
                policy_id=plan_policy_id,
                sizing_policy_id="inventory_child_scaled",
                mode="paper",
                order_type="PAPER_SOURCE_FILL",
                token_id=str(child.get("token_id") or ""),
                market_id=str(child.get("market_id") or ""),
                event_ts=num(child.get("event_ts")) or None,
                live_orders_allowed=False,
                reason="multi-wallet inventory child BUY copied into deterministic paper intent",
                metadata={
                    "inventory_plan": plan_summary,
                    "inventory_child_order": dict(child),
                    "inventory_child_index": index,
                    "inventory_scoring_mode": "child_copy_intents",
                },
            )
        )
    return intents
