"""Score-weighted portfolio allocation for concurrent wallet-copy signals.

Flow stage: LIVE/PROMOTE. This module is deliberately pure: it never submits
orders and never mutates the live guard. The same CopyIntent transform can be
used by paper and live paths after evidence gates approve it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping, Sequence

from src.wallet_copy.models import CopyIntent, num, stable_id


@dataclass(frozen=True)
class RecycleCredit:
    amount_usd: float
    available_at_ts: float
    source: str = "position_recycle"

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioAllocatorConfig:
    reserve_cash_usd: float = 0.0
    recycling_horizon_s: float = 300.0
    min_member_allocation_usd: float = 1.0
    min_intent_allocation_usd: float = 1.0
    max_member_allocation_usd: float = 0.0
    max_concurrent_book_usd: float = 0.0
    score_floor: float = 0.01
    strategy_family: str = "wallet_copy_portfolio_allocator_v1"
    sizing_policy_id: str = "score_weighted_pro_rata_cash_book"

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioIntentAllocation:
    intent_id: str
    member_id: str
    source_wallet: str
    requested_usd: float
    allocated_usd: float
    allocated_shares: float
    allocation_ratio: float
    score: float
    status: str
    reason: str = ""

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PortfolioAllocationResult:
    status: str
    budget_usd: float
    allocated_usd: float
    unallocated_usd: float
    requested_usd: float
    member_count: int
    allocated_member_count: int
    starved_member_count: int
    scaled_intents: tuple[CopyIntent, ...] = ()
    intent_allocations: tuple[PortfolioIntentAllocation, ...] = ()
    member_allocations: tuple[dict[str, Any], ...] = ()
    summary: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scaled_intents"] = [intent.asdict() for intent in self.scaled_intents]
        return payload


def _member_id(intent: CopyIntent) -> str:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    for key in ("portfolio_member_id", "live_member_id", "candidate_id"):
        value = str(metadata.get(key) or "").strip().lower()
        if value:
            return value
    wallet = str(intent.source_wallet or "").strip().lower()
    return wallet or str(intent.wallet_name or intent.intent_id).strip().lower()


def _score_value(row: Any) -> float:
    if isinstance(row, Mapping):
        for key in ("portfolio_score", "copyability_score", "score", "paper_pnl_usd", "rolling_pnl_usd"):
            if key in row:
                return num(row.get(key))
        return 0.0
    return num(row)


def _score_for_member(
    member_id: str,
    intents: Sequence[CopyIntent],
    member_scores: Mapping[str, Any] | None,
    *,
    score_floor: float,
) -> float:
    scores = member_scores or {}
    candidates = [
        scores.get(member_id),
        scores.get(member_id.lower()),
        scores.get(str(intents[0].source_wallet or "").lower()) if intents else None,
    ]
    for candidate in candidates:
        score = _score_value(candidate)
        if score > 0:
            return max(float(score), float(score_floor))
    metadata_scores = []
    for intent in intents:
        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        for key in ("portfolio_score", "copyability_score", "score"):
            value = num(metadata.get(key))
            if value > 0:
                metadata_scores.append(value)
    if metadata_scores:
        return max(max(metadata_scores), float(score_floor))
    return 1.0


def _normalize_recycle_credits(
    recycle_credits: Sequence[RecycleCredit | Mapping[str, Any]] | None,
    *,
    now_ts: float,
) -> list[RecycleCredit]:
    credits: list[RecycleCredit] = []
    for row in recycle_credits or []:
        if isinstance(row, RecycleCredit):
            credits.append(row)
            continue
        if not isinstance(row, Mapping):
            continue
        amount = num(row.get("amount_usd"))
        available_at = num(row.get("available_at_ts"))
        if available_at <= 0 and row.get("seconds_until_available") is not None:
            available_at = float(now_ts) + num(row.get("seconds_until_available"))
        if amount > 0 and available_at > 0:
            credits.append(
                RecycleCredit(
                    amount_usd=amount,
                    available_at_ts=available_at,
                    source=str(row.get("source") or "position_recycle"),
                )
            )
    return credits


def _waterfill_weighted(
    desired: Mapping[str, float],
    weights: Mapping[str, float],
    budget: float,
    *,
    initial: Mapping[str, float] | None = None,
) -> dict[str, float]:
    allocations = {member: round(num((initial or {}).get(member)), 12) for member in desired}
    remaining_budget = max(0.0, float(budget) - sum(allocations.values()))
    unmet = {
        member: max(0.0, float(wanted) - allocations.get(member, 0.0))
        for member, wanted in desired.items()
    }
    if remaining_budget <= 1e-12 or not unmet:
        return allocations
    if remaining_budget >= sum(unmet.values()) - 1e-12:
        return {member: round(allocations.get(member, 0.0) + unmet.get(member, 0.0), 6) for member in desired}

    active = {member for member, amount in unmet.items() if amount > 1e-12 and weights.get(member, 0.0) > 0}
    while active and remaining_budget > 1e-12:
        total_weight = sum(max(0.0, float(weights.get(member, 0.0))) for member in active)
        if total_weight <= 0:
            break
        capped: list[str] = []
        for member in active:
            share = remaining_budget * max(0.0, float(weights.get(member, 0.0))) / total_weight
            if share >= unmet[member] - 1e-12:
                capped.append(member)
        if not capped:
            for member in active:
                share = remaining_budget * max(0.0, float(weights.get(member, 0.0))) / total_weight
                allocations[member] = allocations.get(member, 0.0) + share
            remaining_budget = 0.0
            break
        for member in capped:
            take = unmet[member]
            allocations[member] = allocations.get(member, 0.0) + take
            remaining_budget -= take
            unmet[member] = 0.0
            active.remove(member)
    return {member: round(max(0.0, amount), 6) for member, amount in allocations.items()}


def _member_floor_allocations(
    desired: Mapping[str, float],
    scores: Mapping[str, float],
    budget: float,
    *,
    floor_usd: float,
) -> dict[str, float]:
    if floor_usd <= 0 or budget <= 0:
        return {member: 0.0 for member in desired}
    allocations = {member: 0.0 for member in desired}
    remaining = float(budget)
    for member in sorted(desired, key=lambda item: (-scores[item], item)):
        target = min(float(floor_usd), desired[member])
        if target <= 0:
            continue
        if remaining + 1e-12 < target:
            break
        allocations[member] = round(target, 6)
        remaining -= target
    return allocations


def _split_member_intents(
    intents: Sequence[CopyIntent],
    allocation_usd: float,
    *,
    min_intent_allocation_usd: float,
) -> dict[str, float]:
    if allocation_usd <= 0:
        return {}
    positive = [intent for intent in intents if float(intent.copy_size_usd) > 0 and float(intent.limit_price) > 0]
    if not positive:
        return {}
    if len(positive) == 1:
        return {positive[0].intent_id: round(allocation_usd, 6)} if allocation_usd >= min_intent_allocation_usd else {}

    remaining_intents = list(positive)
    remaining_allocation = float(allocation_usd)
    output: dict[str, float] = {}
    while remaining_intents and remaining_allocation >= min_intent_allocation_usd:
        requested_total = sum(float(intent.copy_size_usd) for intent in remaining_intents)
        if requested_total <= 0:
            break
        small = []
        for intent in remaining_intents:
            amount = remaining_allocation * float(intent.copy_size_usd) / requested_total
            if amount < min_intent_allocation_usd:
                small.append(intent)
        if not small:
            for intent in remaining_intents:
                amount = remaining_allocation * float(intent.copy_size_usd) / requested_total
                output[intent.intent_id] = round(amount, 6)
            break
        if len(small) == len(remaining_intents):
            winner = max(remaining_intents, key=lambda item: (float(item.copy_size_usd), item.intent_id))
            output[winner.intent_id] = round(remaining_allocation, 6)
            break
        remaining_intents = [intent for intent in remaining_intents if intent not in small]
    return output


def allocate_portfolio_intents(
    intents: Sequence[CopyIntent],
    *,
    member_scores: Mapping[str, Any] | None = None,
    available_cash_usd: float,
    existing_exposure_usd: float = 0.0,
    recycle_credits: Sequence[RecycleCredit | Mapping[str, Any]] | None = None,
    now_ts: float,
    config: PortfolioAllocatorConfig | None = None,
) -> PortfolioAllocationResult:
    cfg = config or PortfolioAllocatorConfig()
    valid_intents = [
        intent
        for intent in intents
        if str(intent.action or "").upper() == "BUY"
        and float(intent.copy_size_usd) > 0
        and float(intent.limit_price) > 0
    ]
    credits = _normalize_recycle_credits(recycle_credits, now_ts=float(now_ts))
    recyclable = sum(
        credit.amount_usd
        for credit in credits
        if credit.available_at_ts <= float(now_ts) + max(0.0, float(cfg.recycling_horizon_s))
    )
    spendable_cash = max(0.0, float(available_cash_usd) - max(0.0, float(cfg.reserve_cash_usd)) + recyclable)
    book_room = None
    if cfg.max_concurrent_book_usd > 0:
        book_room = max(0.0, float(cfg.max_concurrent_book_usd) - max(0.0, float(existing_exposure_usd)))
    budget = min(spendable_cash, book_room) if book_room is not None else spendable_cash

    by_member: dict[str, list[CopyIntent]] = {}
    for intent in valid_intents:
        by_member.setdefault(_member_id(intent), []).append(intent)
    requested_by_member: dict[str, float] = {}
    score_by_member: dict[str, float] = {}
    for member, rows in by_member.items():
        requested = sum(float(intent.copy_size_usd) for intent in rows)
        if cfg.max_member_allocation_usd > 0:
            requested = min(requested, float(cfg.max_member_allocation_usd))
        requested_by_member[member] = round(requested, 6)
        score_by_member[member] = _score_for_member(
            member,
            rows,
            member_scores,
            score_floor=float(cfg.score_floor),
        )

    floor_alloc = _member_floor_allocations(
        requested_by_member,
        score_by_member,
        budget,
        floor_usd=float(cfg.min_member_allocation_usd),
    )
    member_alloc = _waterfill_weighted(requested_by_member, score_by_member, budget, initial=floor_alloc)

    intent_amounts: dict[str, float] = {}
    for member, rows in by_member.items():
        intent_amounts.update(
            _split_member_intents(
                rows,
                member_alloc.get(member, 0.0),
                min_intent_allocation_usd=max(0.0, float(cfg.min_intent_allocation_usd)),
            )
        )

    allocations: list[PortfolioIntentAllocation] = []
    scaled_intents: list[CopyIntent] = []
    for member, rows in by_member.items():
        score = score_by_member[member]
        for intent in rows:
            allocated = round(intent_amounts.get(intent.intent_id, 0.0), 6)
            requested = round(float(intent.copy_size_usd), 6)
            ratio = round(allocated / requested, 9) if requested > 0 else 0.0
            shares = round(allocated / float(intent.limit_price), 6) if allocated > 0 else 0.0
            reason = "allocated"
            status = "ALLOCATED"
            if allocated <= 0:
                status = "DEFERRED"
                reason = (
                    "budget_below_member_floor"
                    if budget < len(by_member) * max(0.0, float(cfg.min_member_allocation_usd))
                    else "allocation_below_intent_floor"
                )
            allocations.append(
                PortfolioIntentAllocation(
                    intent_id=intent.intent_id,
                    member_id=member,
                    source_wallet=str(intent.source_wallet or "").lower(),
                    requested_usd=requested,
                    allocated_usd=allocated,
                    allocated_shares=shares,
                    allocation_ratio=ratio,
                    score=round(score, 6),
                    status=status,
                    reason=reason,
                )
            )
            if allocated <= 0:
                continue
            metadata = dict(intent.metadata or {})
            allocated_intent_id = stable_id(
                "ci",
                {
                    "portfolio_source_intent_id": intent.intent_id,
                    "portfolio_member_id": member,
                    "allocated_usd": allocated,
                    "sizing_policy_id": cfg.sizing_policy_id,
                },
            )
            metadata["portfolio_allocation"] = {
                "schema_version": 1,
                "flow_stage": "LIVE",
                "status": "PASS",
                "strategy_family": cfg.strategy_family,
                "member_id": member,
                "member_score": round(score, 6),
                "requested_usd": requested,
                "allocated_usd": allocated,
                "allocation_ratio": ratio,
                "budget_usd": round(float(budget), 6),
                "available_cash_usd": round(float(available_cash_usd), 6),
                "recyclable_cash_usd": round(float(recyclable), 6),
                "existing_exposure_usd": round(float(existing_exposure_usd), 6),
                "max_concurrent_book_usd": round(float(cfg.max_concurrent_book_usd), 6),
            }
            scaled_intents.append(
                CopyIntent.from_dict(
                    {
                        **intent.asdict(),
                        "intent_id": allocated_intent_id,
                        "copy_size_usd": allocated,
                        "shares": shares,
                        "sizing_policy_id": cfg.sizing_policy_id,
                        "metadata": metadata,
                    }
                )
            )

    member_rows: list[dict[str, Any]] = []
    for member in sorted(by_member, key=lambda item: (-score_by_member[item], item)):
        allocation = round(member_alloc.get(member, 0.0), 6)
        requested = requested_by_member[member]
        member_rows.append(
            {
                "member_id": member,
                "source_wallet": str(by_member[member][0].source_wallet or "").lower(),
                "score": round(score_by_member[member], 6),
                "requested_usd": requested,
                "allocated_usd": allocation,
                "allocation_ratio": round(allocation / requested, 9) if requested > 0 else 0.0,
                "status": "ALLOCATED" if allocation > 0 else "DEFERRED",
            }
        )

    allocated_total = round(sum(row.allocated_usd for row in allocations), 6)
    requested_total = round(sum(requested_by_member.values()), 6)
    allocated_members = sum(1 for row in member_rows if row["allocated_usd"] > 0)
    starved_members = sum(1 for row in member_rows if row["allocated_usd"] <= 0)
    sorted_scaled = sorted(
        scaled_intents,
        key=lambda intent: (
            num((intent.metadata or {}).get("portfolio_allocation", {}).get("member_score")),
            float(intent.copy_size_usd),
            intent.intent_id,
        ),
        reverse=True,
    )
    status = "PASS"
    if starved_members:
        status = "ANALYZE"
    if not valid_intents or allocated_total <= 0:
        status = "SKIPPED"
    summary = {
        "flow_stage": "LIVE",
        "strategy_family": cfg.strategy_family,
        "allocation_rule": "member_floor_then_score_weighted_pro_rata",
        "single_book_exposure_managed": cfg.max_concurrent_book_usd > 0,
        "available_cash_usd": round(float(available_cash_usd), 6),
        "reserve_cash_usd": round(float(cfg.reserve_cash_usd), 6),
        "recyclable_cash_usd": round(float(recyclable), 6),
        "recycling_horizon_s": round(float(cfg.recycling_horizon_s), 6),
        "existing_exposure_usd": round(float(existing_exposure_usd), 6),
        "book_room_usd": None if book_room is None else round(float(book_room), 6),
        "budget_usd": round(float(budget), 6),
        "requested_usd": requested_total,
        "allocated_usd": allocated_total,
        "member_count": len(by_member),
        "allocated_member_count": allocated_members,
        "starved_member_count": starved_members,
        "config": cfg.asdict(),
        "recycle_credits_considered": [credit.asdict() for credit in credits],
    }
    return PortfolioAllocationResult(
        status=status,
        budget_usd=round(float(budget), 6),
        allocated_usd=allocated_total,
        unallocated_usd=round(max(0.0, float(budget) - allocated_total), 6),
        requested_usd=requested_total,
        member_count=len(by_member),
        allocated_member_count=allocated_members,
        starved_member_count=starved_members,
        scaled_intents=tuple(sorted_scaled),
        intent_allocations=tuple(allocations),
        member_allocations=tuple(member_rows),
        summary=summary,
    )


def allocation_result_id(result: PortfolioAllocationResult) -> str:
    return stable_id(
        "pa",
        {
            "budget_usd": result.budget_usd,
            "allocated_usd": result.allocated_usd,
            "intent_allocations": [row.asdict() for row in result.intent_allocations],
        },
    )
