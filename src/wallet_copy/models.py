"""Canonical wallet-copy data contracts.

These dataclasses intentionally avoid old strategy-family assumptions. They are
small enough to be logged as JSONL, replayed in paper, and adapted to live
execution without changing the decision payload.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


CopyMode = Literal["paper", "live"]
OrderLifecycleStatus = Literal[
    "INTENT_RECEIVED",
    "PAPER_SUBMITTED",
    "PAPER_FILLED",
    "PAPER_REDUCED",
    "PAPER_REDEEMED",
    "LIVE_SUBMITTED",
    "LIVE_FILLED",
    "LIVE_REJECTED",
    "SKIPPED",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_ts() -> float:
    return time.time()


def num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_id(prefix: str, value: Any, *, length: int = 24) -> str:
    digest = hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{digest}"


def parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            return ts / 1000.0
        if ts > 1e10:
            return ts / 1000.0
        return ts
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_ts(float(text))
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


@dataclass(frozen=True)
class WalletSpec:
    name: str
    address: str
    enabled: bool = True
    data_api: str = "https://data-api.polymarket.com"
    market_filter: str = "btc_5m"
    asset_allowlist: tuple[str, ...] = ("BTC",)
    tags: tuple[str, ...] = ()
    notes: str = ""

    def normalized_address(self) -> str:
        return self.address.lower()

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["address"] = self.normalized_address()
        return payload


@dataclass(frozen=True)
class WalletEvent:
    source_wallet: str
    wallet_name: str
    row_type: str
    action: str
    condition_id: str
    market_slug: str
    outcome: str
    price: float
    size: float
    usdc_size: float
    event_ts: float | None
    observed_ts: float
    event_id: str = ""
    schema_version: int = 1
    source: str = "polymarket_data_api"
    market_id: str = ""
    event_slug: str = ""
    title: str = ""
    asset: str = ""
    duration: str = ""
    window_start_s: int | None = None
    token_id: str = ""
    outcome_index: int | None = None
    transaction_hash: str = ""
    api_latency_s: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.event_id:
            object.__setattr__(
                self,
                "event_id",
                stable_id(
                    "we",
                    {
                        "wallet": self.source_wallet.lower(),
                        "row_type": self.row_type,
                        "tx": self.transaction_hash,
                        "condition_id": self.condition_id,
                        "token_id": self.token_id,
                        "outcome": self.outcome,
                        "price": round(float(self.price), 8),
                        "size": round(float(self.size), 8),
                        "event_ts": self.event_ts,
                    },
                ),
            )

    @property
    def age_s(self) -> float | None:
        if self.event_ts is None:
            return None
        return max(0.0, float(self.observed_ts) - float(self.event_ts))

    @property
    def is_buy(self) -> bool:
        return self.action.upper() == "BUY"

    @property
    def source_fingerprint(self) -> str:
        """Source-level identity independent from API endpoint row type."""

        tx_or_event = self.transaction_hash or self.event_id
        return stable_id(
            "wef",
            {
                "wallet": self.source_wallet.lower(),
                "tx_or_event": tx_or_event,
                "condition_id": self.condition_id,
                "token_id": self.token_id,
                "outcome": self.outcome,
                "action": self.action.upper(),
                "price": round(float(self.price), 8),
                "size": round(float(self.size), 8),
                "event_ts": self.event_ts,
            },
        )

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_fingerprint"] = self.source_fingerprint
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "WalletEvent":
        return cls(**{key: value.get(key) for key in cls.__dataclass_fields__ if key in value})


@dataclass(frozen=True)
class SizingPolicy:
    policy_id: str = "wallet_fraction_1_cap_0"
    basis: Literal["wallet_usdc_fraction", "fixed_usd"] = "wallet_usdc_fraction"
    wallet_fraction: float = 1.0
    fixed_usd: float = 1.0
    max_order_usd: float = 0.0
    min_order_usd: float = 0.0

    def size_usd(self, event: WalletEvent) -> float:
        if self.basis == "fixed_usd":
            size = max(0.0, float(self.fixed_usd))
        else:
            size = max(0.0, float(event.usdc_size) * max(0.0, float(self.wallet_fraction)))
        if self.max_order_usd > 0:
            size = min(size, float(self.max_order_usd))
        if size < max(0.0, float(self.min_order_usd)):
            return 0.0
        return round(size, 6)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CopyIntent:
    source_wallet: str
    wallet_name: str
    source_event_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    limit_price: float
    wallet_usdc_size: float
    copy_size_usd: float
    shares: float
    observed_ts: float
    source_row_event_id: str = ""
    intent_id: str = ""
    schema_version: int = 1
    strategy_family: str = "wallet_copy_exact_v1"
    policy_id: str = "exact_copy_all_buys"
    sizing_policy_id: str = "wallet_fraction_1_cap_0"
    mode: CopyMode = "paper"
    action: str = "BUY"
    order_type: str = "PAPER_SOURCE_FILL"
    token_id: str = ""
    market_id: str = ""
    event_ts: float | None = None
    api_latency_s: float | None = None
    live_orders_allowed: bool = False
    reason: str = "wallet BUY copied into deterministic paper intent"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.intent_id:
            object.__setattr__(
                self,
                "intent_id",
                stable_id(
                    "ci",
                    {
                        "wallet": self.source_wallet.lower(),
                        "source_event_id": self.source_event_id,
                        "condition_id": self.condition_id,
                        "outcome": self.outcome,
                        "price": round(float(self.limit_price), 8),
                        "copy_size_usd": round(float(self.copy_size_usd), 6),
                        "policy_id": self.policy_id,
                        "sizing_policy_id": self.sizing_policy_id,
                    },
                ),
            )

    def asdict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CopyIntent":
        return cls(**{key: value.get(key) for key in cls.__dataclass_fields__ if key in value})


@dataclass(frozen=True)
class LifecycleEvent:
    ts: str
    status: OrderLifecycleStatus
    order_id: str
    intent_id: str
    message: str
    payload: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperOrder:
    order_id: str
    intent_id: str
    source_wallet: str
    wallet_name: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    limit_price: float
    requested_size_usd: float
    requested_shares: float
    filled_size_usd: float
    filled_shares: float
    status: str
    final_status: str
    submitted_at: str
    updated_at: str
    paper_only: bool = True
    live_orders_allowed: bool = False
    fill_model: str = "source_fill"
    lifecycle: list[dict[str, Any]] = field(default_factory=list)
    source_intent: dict[str, Any] = field(default_factory=dict)

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PositionLot:
    position_id: str
    condition_id: str
    outcome: str
    source_wallet: str
    wallet_name: str
    shares: float
    cost_usd: float
    avg_price: float
    opened_at: str
    updated_at: str
    order_ids: tuple[str, ...] = ()

    def asdict(self) -> dict[str, Any]:
        return asdict(self)
