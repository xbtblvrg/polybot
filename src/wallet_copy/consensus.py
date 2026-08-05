"""Multi-wallet consensus and inventory construction."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from src.wallet_copy.models import CopyIntent, stable_id


@dataclass(frozen=True)
class ConsensusConfig:
    min_agreeing_wallets: int = 2
    allow_opposing_wallets: bool = False
    max_price_spread: float = 0.08
    max_consensus_usd: float = 0.0
    strategy_family: str = "wallet_copy_multi_wallet_consensus_v1"
    policy_id: str = "same_market_same_outcome_consensus"


@dataclass(frozen=True)
class ConsensusSignal:
    signal_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    wallet_names: tuple[str, ...]
    source_wallets: tuple[str, ...]
    source_intent_ids: tuple[str, ...]
    average_price: float
    min_price: float
    max_price: float
    total_copy_size_usd: float
    status: str
    blockers: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _market_key(intent: CopyIntent) -> str:
    return intent.condition_id or intent.market_slug


def wallet_outcome_posture(intents: list[CopyIntent]) -> dict[str, dict[str, Any]]:
    """Summarize each wallet's net outcome posture inside one market window."""

    by_wallet: dict[str, dict[str, float]] = {}
    for intent in intents:
        wallet = intent.source_wallet.lower()
        outcome = str(intent.outcome or "")
        if not wallet or not outcome:
            continue
        by_wallet.setdefault(wallet, {})
        by_wallet[wallet][outcome] = by_wallet[wallet].get(outcome, 0.0) + float(intent.copy_size_usd)
    posture: dict[str, dict[str, Any]] = {}
    for wallet, outcomes in by_wallet.items():
        positive = {outcome: usd for outcome, usd in outcomes.items() if usd > 1e-9}
        dominant = max(positive.items(), key=lambda item: item[1])[0] if positive else ""
        total = sum(positive.values())
        dominant_usd = positive.get(dominant, 0.0)
        posture[wallet] = {
            "outcome_usd": {outcome: round(usd, 6) for outcome, usd in sorted(positive.items())},
            "dominant_outcome": dominant,
            "dominant_usd": round(dominant_usd, 6),
            "total_usd": round(total, 6),
            "is_hedged_or_exit": len(positive) > 1,
        }
    return posture


def unique_intents(intents: list[CopyIntent]) -> list[CopyIntent]:
    unique: dict[str, CopyIntent] = {}
    for intent in intents:
        source_fingerprint = ""
        if isinstance(intent.metadata, dict):
            source_fingerprint = str(intent.metadata.get("source_fingerprint") or "")
        key = source_fingerprint or intent.intent_id or stable_id(
            "ci",
            {
                "wallet": intent.source_wallet.lower(),
                "source_event_id": intent.source_event_id,
                "condition_id": intent.condition_id,
                "outcome": intent.outcome,
                "price": intent.limit_price,
                "size": intent.copy_size_usd,
            },
        )
        unique.setdefault(key, intent)
    return list(unique.values())


def build_consensus_signals(
    intents: list[CopyIntent],
    *,
    config: ConsensusConfig | None = None,
) -> list[ConsensusSignal]:
    cfg = config or ConsensusConfig()
    by_market: dict[str, list[CopyIntent]] = {}
    for intent in unique_intents(intents):
        by_market.setdefault(_market_key(intent), []).append(intent)

    signals: list[ConsensusSignal] = []
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
            opposing = [
                intent
                for intent in market_intents
                if intent.outcome != outcome
                and intent.source_wallet.lower() not in wallets
                and not posture.get(intent.source_wallet.lower(), {}).get("is_hedged_or_exit")
            ]
            prices = [float(intent.limit_price) for intent in group]
            blockers: list[str] = []
            if len(wallets) < int(cfg.min_agreeing_wallets):
                blockers.append("insufficient_agreeing_wallets")
            if hedged_wallets and not cfg.allow_opposing_wallets:
                blockers.append("hedged_or_exit_wallets_present")
            if opposing and not cfg.allow_opposing_wallets:
                blockers.append("opposing_wallets_present")
            if prices and max(prices) - min(prices) > float(cfg.max_price_spread):
                blockers.append("price_spread_too_wide")
            total_usd = sum(float(intent.copy_size_usd) for intent in group)
            if cfg.max_consensus_usd > 0:
                total_usd = min(total_usd, float(cfg.max_consensus_usd))
            status = "PASS" if not blockers else "BLOCKED"
            signal_id = stable_id(
                "cs",
                {
                    "market": market_key,
                    "outcome": outcome,
                    "wallets": wallets,
                    "intent_ids": sorted(intent.intent_id for intent in group),
                    "policy_id": cfg.policy_id,
                },
            )
            signals.append(
                ConsensusSignal(
                    signal_id=signal_id,
                    condition_id=group[0].condition_id if group else market_key,
                    market_slug=group[0].market_slug if group else "",
                    outcome=outcome,
                    side=group[0].side if group else "",
                    wallet_names=tuple(sorted({intent.wallet_name for intent in group})),
                    source_wallets=tuple(wallets),
                    source_intent_ids=tuple(sorted(intent.intent_id for intent in group)),
                    average_price=round(sum(prices) / len(prices), 6) if prices else 0.0,
                    min_price=round(min(prices), 6) if prices else 0.0,
                    max_price=round(max(prices), 6) if prices else 0.0,
                    total_copy_size_usd=round(total_usd, 6),
                    status=status,
                    blockers=tuple(blockers),
                    metadata={
                        "strategy_family": cfg.strategy_family,
                        "policy_id": cfg.policy_id,
                        "opposing_intent_ids": [intent.intent_id for intent in opposing],
                        "wallet_posture": posture,
                        "hedged_wallets": hedged_wallets,
                    },
                )
            )
    return sorted(signals, key=lambda signal: (signal.status != "PASS", signal.condition_id, signal.outcome))


def consensus_signal_to_intent(signal: ConsensusSignal) -> CopyIntent | None:
    if signal.status != "PASS" or signal.total_copy_size_usd <= 0 or signal.average_price <= 0:
        return None
    return CopyIntent(
        intent_id=stable_id("ci", {"consensus_signal_id": signal.signal_id}),
        source_wallet="CONSENSUS",
        wallet_name="+".join(signal.wallet_names),
        source_event_id=signal.signal_id,
        condition_id=signal.condition_id,
        market_slug=signal.market_slug,
        outcome=signal.outcome,
        side=signal.side,
        limit_price=signal.average_price,
        wallet_usdc_size=signal.total_copy_size_usd,
        copy_size_usd=signal.total_copy_size_usd,
        shares=round(signal.total_copy_size_usd / signal.average_price, 6),
        observed_ts=0.0,
        strategy_family=str(signal.metadata.get("strategy_family") or "wallet_copy_multi_wallet_consensus_v1"),
        policy_id=str(signal.metadata.get("policy_id") or "same_market_same_outcome_consensus"),
        sizing_policy_id="consensus_sum",
        mode="paper",
        order_type="PAPER_SOURCE_FILL",
        metadata={"consensus_signal": signal.asdict()},
    )
