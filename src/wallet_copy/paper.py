"""Deterministic paper execution for wallet-copy intents."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill
from src.wallet_copy.models import CopyIntent, LifecycleEvent, PaperOrder, WalletEvent, stable_id, utc_now_iso
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json


@dataclass(frozen=True)
class PaperExecutionConfig:
    state_path: str = "data/research/wallet_copy_paper_state.json"
    event_log_path: str = "data/research/wallet_copy_paper_events.jsonl"
    fill_model: str = "executable_copy_fill_v1"
    fallback_slippage_bps: float = 250.0
    min_fill_ratio: float = 0.999
    max_api_latency_s: float = 0.0
    allow_fallback_without_book: bool = True
    reset_existing_state: bool = False
    retain_orders: int = 5_000
    retain_lifecycle_events: int = 15_000
    retain_dedupe_ids: int = 250_000


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_paper_state",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "orders": [],
        "lifecycle_events": [],
        "positions": {},
        "wallets": {},
        "dedupe": {
            "order_ids": [],
            "intent_ids": [],
            "wallet_lifecycle_ids": [],
        },
        "summary": {},
    }


def _position_key(condition_id: str, outcome: str, source_wallet: str) -> str:
    return f"{condition_id}|{outcome}|{source_wallet.lower()}"


def _lifecycle(status: str, order_id: str, intent_id: str, message: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return LifecycleEvent(
        ts=utc_now_iso(),
        status=status,  # type: ignore[arg-type]
        order_id=order_id,
        intent_id=intent_id,
        message=message,
        payload=payload or {},
    ).asdict()


def _scaled_wallet_proceeds(
    event: WalletEvent,
    *,
    reduced_shares: float,
    default_redeem_price: float | None = None,
) -> dict[str, Any]:
    """Return paper proceeds for only the shares we actually copied."""

    source_size = max(0.0, float(event.size))
    source_usdc = max(0.0, float(event.usdc_size))
    if source_usdc > 0 and source_size > 0:
        ratio = min(1.0, max(0.0, float(reduced_shares) / source_size))
        return {
            "proceeds_usd": source_usdc * ratio,
            "source_proceeds_usd": source_usdc,
            "source_size": source_size,
            "proceeds_scaling_ratio": ratio,
            "proceeds_basis": "source_wallet_usdc_pro_rata",
        }
    if float(event.price) > 0:
        return {
            "proceeds_usd": float(event.price) * float(reduced_shares),
            "source_proceeds_usd": source_usdc,
            "source_size": source_size,
            "proceeds_scaling_ratio": None,
            "proceeds_basis": "event_price_times_copied_shares",
        }
    fallback_price = 1.0 if default_redeem_price is None else float(default_redeem_price)
    return {
        "proceeds_usd": max(0.0, fallback_price) * float(reduced_shares),
        "source_proceeds_usd": source_usdc,
        "source_size": source_size,
        "proceeds_scaling_ratio": None,
        "proceeds_basis": "copied_shares_default_redeem_value",
    }


def _summarize(state: dict[str, Any]) -> dict[str, Any]:
    orders = [row for row in state.get("orders") or [] if isinstance(row, dict)]
    filled = [row for row in orders if row.get("final_status") == "FILLED"]
    rejected = [row for row in orders if row.get("final_status") == "REJECTED"]
    open_positions = [
        row
        for row in (state.get("positions") or {}).values()
        if isinstance(row, dict) and float(row.get("shares") or 0.0) > 1e-9
    ]
    cost_usd = sum(float(row.get("filled_size_usd") or 0.0) for row in filled)
    fill_source_counts = Counter(
        str(
            (
                (row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {})
                .get("fill_estimate")
                if isinstance(
                    (row.get("source_intent") if isinstance(row.get("source_intent"), dict) else {}).get(
                        "fill_estimate"
                    ),
                    dict,
                )
                else {}
            ).get("source")
            or "unknown"
        )
        for row in filled
    )
    return {
        "paper_orders": len(orders),
        "filled_orders": len(filled),
        "rejected_orders": len(rejected),
        "fill_source_counts": dict(sorted(fill_source_counts.items())),
        "open_position_lots": len(open_positions),
        "cost_usd": round(cost_usd, 6),
        "wallet_count": len(state.get("wallets") or {}),
        "latest_order_ts": max((str(row.get("updated_at") or "") for row in orders), default=None),
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": state.get("fill_model"),
    }


def _dedupe_bucket(state: dict[str, Any], key: str) -> list[str]:
    dedupe = state.get("dedupe")
    if not isinstance(dedupe, dict):
        dedupe = {}
        state["dedupe"] = dedupe
    rows = dedupe.get(key)
    if not isinstance(rows, list):
        rows = []
        dedupe[key] = rows
    return [str(row) for row in rows if row]


def _persist_dedupe_bucket(state: dict[str, Any], key: str, values: set[str], *, retain: int) -> None:
    dedupe = state.setdefault("dedupe", {})
    if not isinstance(dedupe, dict):
        dedupe = {}
        state["dedupe"] = dedupe
    limit = max(1, int(retain))
    dedupe[key] = sorted(str(value) for value in values if value)[-limit:]


class PaperWalletCopyEngine:
    """Applies copy intents to an auditable paper ledger.

    The engine applies the same CopyIntent contract every time, but order fills
    now go through an executable fill estimate. With no CLOB book attached, the
    fallback is deterministic source price plus conservative slippage; with CLOB
    evidence, insufficient fillability becomes an auditable paper rejection.
    """

    def __init__(self, config: PaperExecutionConfig | None = None):
        self.config = config or PaperExecutionConfig()
        self._reset_consumed = False

    def load_state(self) -> dict[str, Any]:
        should_reset = self.config.reset_existing_state and not self._reset_consumed
        self._reset_consumed = True
        state = None if should_reset else load_json(self.config.state_path, default=None)
        if not isinstance(state, dict) or state.get("kind") != "wallet_copy_paper_state":
            state = _empty_state()
        state["paper_only"] = True
        state["can_trade"] = False
        state["live_orders_allowed"] = False
        state["fill_model"] = self.config.fill_model
        dedupe = state.setdefault("dedupe", {})
        if not isinstance(dedupe, dict):
            state["dedupe"] = {
                "order_ids": [],
                "intent_ids": [],
                "wallet_lifecycle_ids": [],
            }
        else:
            dedupe.setdefault("order_ids", [])
            dedupe.setdefault("intent_ids", [])
            dedupe.setdefault("wallet_lifecycle_ids", [])
        return state

    def save_state(self, state: dict[str, Any], new_events: list[dict[str, Any]]) -> None:
        orders = [row for row in state.get("orders") or [] if isinstance(row, dict)]
        lifecycle = [row for row in state.get("lifecycle_events") or [] if isinstance(row, dict)]
        order_ids = set(_dedupe_bucket(state, "order_ids"))
        intent_ids = set(_dedupe_bucket(state, "intent_ids"))
        lifecycle_ids = set(_dedupe_bucket(state, "wallet_lifecycle_ids"))
        order_ids.update(str(row.get("order_id")) for row in orders if row.get("order_id"))
        intent_ids.update(str(row.get("intent_id")) for row in orders if row.get("intent_id"))
        lifecycle_ids.update(
            str(row.get("wallet_lifecycle_id"))
            for row in lifecycle
            if isinstance(row, dict) and row.get("wallet_lifecycle_id")
        )
        _persist_dedupe_bucket(state, "order_ids", order_ids, retain=int(self.config.retain_dedupe_ids))
        _persist_dedupe_bucket(state, "intent_ids", intent_ids, retain=int(self.config.retain_dedupe_ids))
        _persist_dedupe_bucket(
            state,
            "wallet_lifecycle_ids",
            lifecycle_ids,
            retain=int(self.config.retain_dedupe_ids),
        )
        state["orders"] = orders[-int(self.config.retain_orders):]
        state["lifecycle_events"] = lifecycle[-int(self.config.retain_lifecycle_events):]
        state["summary"] = _summarize(state)
        atomic_write_json(self.config.state_path, state)
        append_jsonl_many(self.config.event_log_path, new_events)

    def apply_intents(self, intents: list[CopyIntent]) -> dict[str, Any]:
        state = self.load_state()
        existing_order_ids = set(_dedupe_bucket(state, "order_ids"))
        existing_order_ids.update(str(row.get("order_id")) for row in state.get("orders") or [] if row.get("order_id"))
        existing_intent_ids = set(_dedupe_bucket(state, "intent_ids"))
        existing_intent_ids.update(str(row.get("intent_id")) for row in state.get("orders") or [] if row.get("intent_id"))
        new_events: list[dict[str, Any]] = []

        for intent in intents:
            event = self._apply_intent_to_state(
                state,
                intent,
                existing_order_ids=existing_order_ids,
                existing_intent_ids=existing_intent_ids,
            )
            if event is not None:
                new_events.append(event)

        self.save_state(state, new_events)
        return state

    def apply_wallet_lifecycle_events(self, events: list[WalletEvent]) -> dict[str, Any]:
        state = self.load_state()
        existing_lifecycle_ids = self._wallet_lifecycle_ids(state)
        new_events: list[dict[str, Any]] = []
        for event in events:
            record = self._apply_lifecycle_event_to_state(
                state,
                event,
                existing_lifecycle_ids=existing_lifecycle_ids,
            )
            if record is not None:
                new_events.append(record)
        self.save_state(state, new_events)
        return state

    def apply_wallet_events_in_order(
        self,
        events: list[WalletEvent],
        *,
        intents: list[CopyIntent] | None = None,
        intent_by_event_id: dict[str, CopyIntent] | None = None,
    ) -> dict[str, Any]:
        """Replay wallet BUY and lifecycle rows in source event order.

        Exact-copy paper truth must preserve chronology. Applying all BUYs first
        can make an earlier SELL/MERGE/REDEEM reduce a position that did not
        exist yet in real time.
        """

        state = self.load_state()
        existing_order_ids = set(_dedupe_bucket(state, "order_ids"))
        existing_order_ids.update(str(row.get("order_id")) for row in state.get("orders") or [] if row.get("order_id"))
        existing_intent_ids = set(_dedupe_bucket(state, "intent_ids"))
        existing_intent_ids.update(str(row.get("intent_id")) for row in state.get("orders") or [] if row.get("intent_id"))
        existing_lifecycle_ids = self._wallet_lifecycle_ids(state)
        intent_map = dict(intent_by_event_id or {})
        for intent in intents or []:
            intent_map.setdefault(intent.source_event_id, intent)
        new_events: list[dict[str, Any]] = []

        for event in sorted(events, key=lambda row: (row.event_ts or 0.0, row.event_id)):
            if event.is_buy:
                intent = intent_map.get(event.event_id)
                if intent is None:
                    continue
                order_event = self._apply_intent_to_state(
                    state,
                    intent,
                    existing_order_ids=existing_order_ids,
                    existing_intent_ids=existing_intent_ids,
                )
                if order_event is not None:
                    new_events.append(order_event)
                continue
            lifecycle_event = self._apply_lifecycle_event_to_state(
                state,
                event,
                existing_lifecycle_ids=existing_lifecycle_ids,
            )
            if lifecycle_event is not None:
                new_events.append(lifecycle_event)

        self.save_state(state, new_events)
        return state

    @staticmethod
    def _wallet_lifecycle_ids(state: dict[str, Any]) -> set[str]:
        ids = set(_dedupe_bucket(state, "wallet_lifecycle_ids"))
        ids.update(
            str(row.get("wallet_lifecycle_id"))
            for row in state.get("lifecycle_events") or []
            if isinstance(row, dict) and row.get("wallet_lifecycle_id")
        )
        return ids

    def _apply_intent_to_state(
        self,
        state: dict[str, Any],
        intent: CopyIntent,
        *,
        existing_order_ids: set[str],
        existing_intent_ids: set[str],
    ) -> dict[str, Any] | None:
        if intent.intent_id in existing_intent_ids:
            return None
        order_id = stable_id("po", intent.intent_id)
        if order_id in existing_order_ids:
            return None
        fill = estimate_executable_fill(
            intent,
            FillModelConfig(
                model_id=self.config.fill_model,
                fallback_slippage_bps=self.config.fallback_slippage_bps,
                min_fill_ratio=self.config.min_fill_ratio,
                max_api_latency_s=self.config.max_api_latency_s,
                allow_fallback_without_book=bool(self.config.allow_fallback_without_book),
            ),
        )
        final_status = "FILLED" if fill.get("status") == "FILLED" else "REJECTED"
        lifecycle = [
            _lifecycle("INTENT_RECEIVED", order_id, intent.intent_id, "copy intent accepted by paper engine"),
            _lifecycle("PAPER_SUBMITTED", order_id, intent.intent_id, "paper order submitted"),
            (
                _lifecycle("PAPER_FILLED", order_id, intent.intent_id, "paper order filled by executable fill model", fill)
                if final_status == "FILLED"
                else _lifecycle("SKIPPED", order_id, intent.intent_id, "paper order rejected by executable fill model", fill)
            ),
        ]
        ts = utc_now_iso()
        order = PaperOrder(
            order_id=order_id,
            intent_id=intent.intent_id,
            source_wallet=intent.source_wallet.lower(),
            wallet_name=intent.wallet_name,
            condition_id=intent.condition_id,
            market_slug=intent.market_slug,
            outcome=intent.outcome,
            side=intent.side,
            limit_price=float(fill.get("effective_price") or intent.limit_price),
            requested_size_usd=float(intent.copy_size_usd),
            requested_shares=float(intent.shares),
            filled_size_usd=float(fill.get("filled_size_usd") or 0.0),
            filled_shares=float(fill.get("filled_shares") or 0.0),
            status=final_status,
            final_status=final_status,
            submitted_at=ts,
            updated_at=ts,
            paper_only=True,
            live_orders_allowed=False,
            fill_model=str(fill.get("fill_model") or self.config.fill_model),
            lifecycle=lifecycle,
            source_intent={**intent.asdict(), "fill_estimate": fill},
        ).asdict()
        state.setdefault("orders", []).append(order)
        state.setdefault("lifecycle_events", []).extend(lifecycle)
        state.setdefault("wallets", {}).setdefault(
            intent.source_wallet.lower(),
            {"wallet_name": intent.wallet_name, "orders": 0, "filled_size_usd": 0.0},
        )
        wallet_row = state["wallets"][intent.source_wallet.lower()]
        wallet_row["orders"] = int(wallet_row.get("orders") or 0) + 1
        wallet_row["filled_size_usd"] = round(
            float(wallet_row.get("filled_size_usd") or 0.0) + float(order.get("filled_size_usd") or 0.0),
            6,
        )
        if final_status == "FILLED":
            self._add_position(state, order)
        existing_order_ids.add(order_id)
        existing_intent_ids.add(intent.intent_id)
        return {"event": "wallet_copy_paper_order", **order}

    def _apply_lifecycle_event_to_state(
        self,
        state: dict[str, Any],
        event: WalletEvent,
        *,
        existing_lifecycle_ids: set[str],
    ) -> dict[str, Any] | None:
        action = event.action.upper()
        if action == "BUY":
            return None
        lifecycle_id = stable_id("wl", {"event_id": event.event_id, "action": action})
        if lifecycle_id in existing_lifecycle_ids:
            return None
        record = {
            "ts": utc_now_iso(),
            "event": "wallet_copy_wallet_lifecycle",
            "wallet_lifecycle_id": lifecycle_id,
            "source_wallet": event.source_wallet.lower(),
            "wallet_name": event.wallet_name,
            "source_event_id": event.event_id,
            "condition_id": event.condition_id,
            "outcome": event.outcome,
            "wallet_action": action,
            "wallet_size": event.size,
            "wallet_usdc_size": event.usdc_size,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        if action == "SELL":
            record["position_reduction"] = self._reduce_position(state, event)
        elif action == "MERGE":
            record["position_reduction"] = self._merge_positions(state, event)
        elif action == "REDEEM":
            record["position_reduction"] = self._redeem_positions(state, event)
        else:
            record["position_reduction"] = {"status": "RECORDED_ONLY", "reason": "unsupported_lifecycle_action"}
        state.setdefault("lifecycle_events", []).append(record)
        existing_lifecycle_ids.add(lifecycle_id)
        return record

    def _add_position(self, state: dict[str, Any], order: dict[str, Any]) -> None:
        key = _position_key(order["condition_id"], order["outcome"], order["source_wallet"])
        positions = state.setdefault("positions", {})
        position = positions.get(key)
        if not isinstance(position, dict):
            position = {
                "position_id": key,
                "condition_id": order["condition_id"],
                "outcome": order["outcome"],
                "source_wallet": order["source_wallet"],
                "wallet_name": order["wallet_name"],
                "shares": 0.0,
                "cost_usd": 0.0,
                "avg_price": 0.0,
                "opened_at": order["submitted_at"],
                "updated_at": order["updated_at"],
                "order_ids": [],
            }
        position["shares"] = round(float(position.get("shares") or 0.0) + float(order["filled_shares"]), 6)
        position["cost_usd"] = round(float(position.get("cost_usd") or 0.0) + float(order["filled_size_usd"]), 6)
        position["avg_price"] = round(
            (float(position["cost_usd"]) / float(position["shares"])) if float(position["shares"]) > 0 else 0.0,
            6,
        )
        position["updated_at"] = order["updated_at"]
        order_ids = [str(item) for item in position.get("order_ids") or []]
        if order["order_id"] not in order_ids:
            order_ids.append(order["order_id"])
        position["order_ids"] = order_ids
        positions[key] = position

    def _reduce_position(self, state: dict[str, Any], event: WalletEvent) -> dict[str, Any]:
        key = _position_key(event.condition_id, event.outcome, event.source_wallet)
        position = (state.get("positions") or {}).get(key)
        if not isinstance(position, dict):
            return {"status": "NO_MATCHING_POSITION"}
        before_shares = float(position.get("shares") or 0.0)
        reduce_shares = min(before_shares, max(0.0, float(event.size)))
        if reduce_shares <= 0:
            return {"status": "ZERO_REDUCTION", "before_shares": before_shares}
        avg_price = float(position.get("avg_price") or 0.0)
        cost_removed = reduce_shares * avg_price
        proceeds_info = _scaled_wallet_proceeds(event, reduced_shares=reduce_shares)
        proceeds = float(proceeds_info["proceeds_usd"])
        position["shares"] = round(before_shares - reduce_shares, 6)
        position["cost_usd"] = round(max(0.0, float(position.get("cost_usd") or 0.0) - cost_removed), 6)
        position["updated_at"] = utc_now_iso()
        return {
            "status": "REDUCED",
            "before_shares": round(before_shares, 6),
            "reduced_shares": round(reduce_shares, 6),
            "after_shares": position["shares"],
            "proceeds_usd": round(proceeds, 6),
            "cost_removed_usd": round(cost_removed, 6),
            "realized_pnl_usd": round(proceeds - cost_removed, 6),
            "source_proceeds_usd": round(float(proceeds_info["source_proceeds_usd"]), 6),
            "source_size": round(float(proceeds_info["source_size"]), 6),
            "proceeds_scaling_ratio": (
                round(float(proceeds_info["proceeds_scaling_ratio"]), 6)
                if proceeds_info["proceeds_scaling_ratio"] is not None
                else None
            ),
            "proceeds_basis": proceeds_info["proceeds_basis"],
        }

    def _matching_positions(self, state: dict[str, Any], event: WalletEvent) -> list[dict[str, Any]]:
        positions = state.get("positions") or {}
        rows = []
        for row in positions.values():
            if not isinstance(row, dict):
                continue
            if str(row.get("source_wallet") or "").lower() != event.source_wallet.lower():
                continue
            if str(row.get("condition_id") or "") != event.condition_id:
                continue
            if float(row.get("shares") or 0.0) <= 1e-9:
                continue
            rows.append(row)
        return rows

    @staticmethod
    def _reduce_position_row(position: dict[str, Any], shares: float) -> dict[str, Any]:
        before_shares = float(position.get("shares") or 0.0)
        reduce_shares = min(before_shares, max(0.0, float(shares)))
        avg_price = float(position.get("avg_price") or 0.0)
        cost_removed = reduce_shares * avg_price
        position["shares"] = round(before_shares - reduce_shares, 6)
        position["cost_usd"] = round(max(0.0, float(position.get("cost_usd") or 0.0) - cost_removed), 6)
        position["updated_at"] = utc_now_iso()
        return {
            "position_id": position.get("position_id"),
            "outcome": position.get("outcome"),
            "before_shares": round(before_shares, 6),
            "reduced_shares": round(reduce_shares, 6),
            "after_shares": position["shares"],
            "cost_removed_usd": round(cost_removed, 6),
        }

    def _merge_positions(self, state: dict[str, Any], event: WalletEvent) -> dict[str, Any]:
        matches = self._matching_positions(state, event)
        if len(matches) < 2:
            available_positions = [
                {
                    "position_id": row.get("position_id"),
                    "outcome": row.get("outcome"),
                    "shares": round(float(row.get("shares") or 0.0), 6),
                    "avg_price": round(float(row.get("avg_price") or 0.0), 6),
                }
                for row in matches
            ]
            available_outcomes = sorted({str(row.get("outcome") or "") for row in matches if row.get("outcome")})
            return {
                "status": "NO_PAIRED_POSITION",
                "matching_positions": len(matches),
                "available_outcomes": available_outcomes,
                "available_positions": available_positions,
                "reason": "merge requires simultaneously held paired outcome positions",
            }
        max_pair_shares = min(float(row.get("shares") or 0.0) for row in matches)
        requested = max(0.0, float(event.size))
        reduce_shares = min(max_pair_shares, requested if requested > 0 else max_pair_shares)
        if reduce_shares <= 0:
            return {"status": "ZERO_REDUCTION", "matching_positions": len(matches)}
        reductions = [self._reduce_position_row(row, reduce_shares) for row in matches]
        cost_removed = sum(float(row.get("cost_removed_usd") or 0.0) for row in reductions)
        proceeds = reduce_shares
        return {
            "status": "MERGED_PAIR_REDUCED",
            "reduced_each_outcome_shares": round(reduce_shares, 6),
            "proceeds_usd": round(proceeds, 6),
            "cost_removed_usd": round(cost_removed, 6),
            "realized_pnl_usd": round(proceeds - cost_removed, 6),
            "reductions": reductions,
        }

    def _redeem_positions(self, state: dict[str, Any], event: WalletEvent) -> dict[str, Any]:
        matches = self._matching_positions(state, event)
        if event.outcome:
            matches = [row for row in matches if str(row.get("outcome") or "").lower() == event.outcome.lower()]
        if not matches:
            return {"status": "NO_MATCHING_POSITION"}
        reductions = []
        for row in matches:
            shares = max(0.0, float(event.size))
            reductions.append(self._reduce_position_row(row, shares if shares > 0 else float(row.get("shares") or 0.0)))
        reduced_shares = sum(float(row.get("reduced_shares") or 0.0) for row in reductions)
        cost_removed = sum(float(row.get("cost_removed_usd") or 0.0) for row in reductions)
        proceeds_info = _scaled_wallet_proceeds(
            event,
            reduced_shares=reduced_shares,
            default_redeem_price=1.0,
        )
        proceeds = float(proceeds_info["proceeds_usd"])
        return {
            "status": "REDEEMED_POSITION_REDUCED",
            "proceeds_usd": round(proceeds, 6),
            "cost_removed_usd": round(cost_removed, 6),
            "realized_pnl_usd": round(proceeds - cost_removed, 6),
            "source_proceeds_usd": round(float(proceeds_info["source_proceeds_usd"]), 6),
            "source_size": round(float(proceeds_info["source_size"]), 6),
            "proceeds_scaling_ratio": (
                round(float(proceeds_info["proceeds_scaling_ratio"]), 6)
                if proceeds_info["proceeds_scaling_ratio"] is not None
                else None
            ),
            "proceeds_basis": proceeds_info["proceeds_basis"],
            "reductions": reductions,
        }


def run_paper_intents(
    intents: list[CopyIntent],
    *,
    state_path: str | Path = "data/research/wallet_copy_paper_state.json",
    event_log_path: str | Path = "data/research/wallet_copy_paper_events.jsonl",
) -> dict[str, Any]:
    engine = PaperWalletCopyEngine(
        PaperExecutionConfig(state_path=str(state_path), event_log_path=str(event_log_path))
    )
    return engine.apply_intents(intents)
