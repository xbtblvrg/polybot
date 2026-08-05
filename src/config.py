"""Minimal wallet-copy execution configuration.

This module contains only fields required by the shared Polymarket execution
adapter.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


@dataclass(slots=True)
class Config:
    """Execution-only config used by ``TradeExecutor``.

    Paper wallet-copy runs do not need this class. Live execution must still be
    admitted by the wallet-copy adapter with explicit operator permission.
    """

    private_key: str = field(default_factory=lambda: os.getenv("PRIVATE_KEY", ""))
    polymarket_proxy: str = field(default_factory=lambda: os.getenv("POLYMARKET_PROXY", ""))

    builder_api_key: str = field(default_factory=lambda: os.getenv("BUILDER_API_KEY", ""))
    builder_secret: str = field(default_factory=lambda: os.getenv("BUILDER_SECRET", ""))
    builder_passphrase: str = field(default_factory=lambda: os.getenv("BUILDER_PASSPHRASE", ""))
    relayer_api_key: str = field(default_factory=lambda: os.getenv("RELAYER_API_KEY", ""))
    relayer_api_key_address: str = field(default_factory=lambda: os.getenv("RELAYER_API_KEY_ADDRESS", ""))

    chain_id: int = field(default_factory=lambda: _env_int("CHAIN_ID", 137))
    clob_host: str = field(default_factory=lambda: os.getenv("CLOB_HOST", "https://clob.polymarket.com"))
    positions_path: str = field(
        default_factory=lambda: os.getenv("POSITIONS_PATH", "data/wallet_copy_live_positions.json")
    )

    live_capital_guard_enabled: bool = field(
        default_factory=lambda: _env_bool("LIVE_CAPITAL_GUARD_ENABLED", True)
    )
    live_capital_min_buffer_usd: float = field(
        default_factory=lambda: _env_float("LIVE_CAPITAL_MIN_BUFFER_USD", 0.0)
    )
    wallet_copy_inventory_min_available_balance_usd: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_INVENTORY_MIN_AVAILABLE_BALANCE_USD", 0.0)
    )
    wallet_copy_max_chase_ticks: int = field(
        default_factory=lambda: _env_int("WALLET_COPY_MAX_CHASE_TICKS", 0)
    )
    wallet_copy_max_buy_price: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_MAX_BUY_PRICE", 0.0)
    )
    wallet_copy_min_buy_price: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_MIN_BUY_PRICE", 0.0)
    )
    wallet_copy_chase_max_price: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_CHASE_MAX_PRICE", 0.0)
    )
    wallet_copy_chase_tick_size: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_CHASE_TICK_SIZE", 0.01)
    )
    wallet_copy_book_snapshot_timeout_s: float = field(
        default_factory=lambda: _env_float("WALLET_COPY_BOOK_SNAPSHOT_TIMEOUT_S", 2.0)
    )

    def validate_execution_ready(self) -> None:
        """Validate fields needed before constructing a live CLOB client."""

        if not self.private_key:
            raise ValueError("PRIVATE_KEY is required for live wallet-copy execution")
        if self.chain_id <= 0:
            raise ValueError("CHAIN_ID must be positive")
        if not self.clob_host.startswith(("http://", "https://")):
            raise ValueError("CLOB_HOST must be an HTTP(S) URL")
        Path(self.positions_path).parent.mkdir(parents=True, exist_ok=True)


config = Config()
