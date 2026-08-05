"""
Wallet-copy Trade Executor for Polymarket.

Provides async trade execution via py_clob_client with:
- Proper async initialization of ClobClient
- Thread-safe position management with async JSON helpers
- Comprehensive error handling and retry logic
- Full API credential derivation on initialization
"""

import asyncio
import json
import logging
import os
import re
import time as _time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import Config
from src.utils import setup_logger, retry_async
from src.wallet_copy.gate_registry import PRE_SUBMIT_REFUSAL_CLASSES


class ClobServerError(Exception):
    """Retryable server-side CLOB error (HTTP 5xx). Only these get retried."""
    pass

# Attempt to import py_clob_client_v2 first; fall back to the canonical
# py_clob_client package when v2 is not installed on the operator machine.
try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import OrderArgs, OrderType, MarketOrderArgs
    from py_clob_client_v2.order_builder.constants import BUY, SELL
    CLOB_AVAILABLE = True
    CLOB_CLIENT_PACKAGE = "py_clob_client_v2"
except ImportError:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, MarketOrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL

        CLOB_AVAILABLE = True
        CLOB_CLIENT_PACKAGE = "py_clob_client"
    except ImportError:
        CLOB_AVAILABLE = False
        CLOB_CLIENT_PACKAGE = ""
        BUY = "BUY"
        SELL = "SELL"

logger = setup_logger(__name__)

WALLET_COPY_MIN_SHARES = 5.0
WALLET_COPY_MIN_SHARE_HARD_CAP_USD = 2.5
ORDER137_PINNED_WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
ORDER137_PINNED_POLICY_ID = "wide_fp_4028560ff42ee6da51e71529"
ORDER137_REVERT_STATE_PATH = Path("data/research/order137_min_share_cap_state.json")
ORDER137_REVERT_CACHE_TTL_S = 1.0
_ORDER137_REVERT_CACHE: Dict[str, Any] = {
    "checked_at": 0.0,
    "mtime_ns": None,
    "revert_active": False,
}


def _order137_revert_active() -> bool:
    """Data-driven kill switch for the ORDER137 pinned-seat min-share cap.

    The running guard cannot reload code, so the mechanical revert is a file
    another (fresh) process writes.  Missing/unreadable file means "not
    reverted": ORDER137 stays armed and the standing $2.50 ceiling plus the
    -$4.00 kill line remain the outer bounds.
    """
    now = _time.monotonic()
    cache = _ORDER137_REVERT_CACHE
    if now - float(cache.get("checked_at") or 0.0) < ORDER137_REVERT_CACHE_TTL_S:
        return bool(cache.get("revert_active"))
    cache["checked_at"] = now
    try:
        mtime_ns = ORDER137_REVERT_STATE_PATH.stat().st_mtime_ns
    except OSError:
        cache["mtime_ns"] = None
        cache["revert_active"] = False
        return False
    if mtime_ns == cache.get("mtime_ns"):
        return bool(cache.get("revert_active"))
    try:
        payload = json.loads(ORDER137_REVERT_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # Keep the last good value; a torn write must not silently re-arm.
        return bool(cache.get("revert_active"))
    cache["mtime_ns"] = mtime_ns
    cache["revert_active"] = bool(
        isinstance(payload, dict) and payload.get("revert_active") is True
    )
    return bool(cache["revert_active"])


def _decimal_places(value: Decimal) -> int:
    return max(0, -value.normalize().as_tuple().exponent)


def _quantize_down(value: Decimal, places: int) -> Decimal:
    step = Decimal(1).scaleb(-places)
    return value.quantize(step, rounding=ROUND_DOWN)


def _market_buy_amount_with_valid_share_precision(
    size_usd: float,
    price: float,
    min_amount_usd: float = 0.0,
    max_overshoot_usd: float = 0.5,
) -> float:
    """Return a BUY market amount whose implied shares fit CLOB precision.

    Polymarket market BUY orders validate both the USDC maker amount (2
    decimals on BTC 5m tick-size 0.01 markets) and the implied taker shares
    (4 decimals). Some apparently-safe amounts, e.g. 2.00 at 0.95, imply a
    repeating share quantity and can be rejected server-side even after the
    client builder rounds. Choose the largest cent amount at or below the
    requested size whose amount / price has at most 4 decimals. When that
    amount would fall below min_amount_usd (the CLOB min order), search
    upward instead for the smallest valid amount >= min_amount_usd within
    max_overshoot_usd above the requested size, so barely-above-min copies
    are rounded up rather than skipped.
    """
    if size_usd <= 0 or price <= 0:
        return max(0.0, float(size_usd))
    amount_cents = int((Decimal(str(size_usd)) * Decimal("100")).to_integral_value(rounding=ROUND_DOWN))
    price_dec = _quantize_down(Decimal(str(price)), 2)
    if price_dec <= 0:
        return float(_quantize_down(Decimal(str(size_usd)), 2))

    def _valid_amount(cents: int) -> Optional[Decimal]:
        amount = Decimal(cents) / Decimal("100")
        shares = _quantize_down(amount / price_dec, 4)
        if _quantize_down(shares * price_dec, 2) == amount:
            return amount
        return None

    min_dec = Decimal(str(max(0.0, min_amount_usd)))
    below_min_amount: Optional[Decimal] = None
    for cents in range(amount_cents, 0, -1):
        amount = _valid_amount(cents)
        if amount is None:
            continue
        if amount >= min_dec:
            return float(amount)
        below_min_amount = amount
        break
    min_cents = int((min_dec * Decimal("100")).to_integral_value(rounding=ROUND_UP))
    overshoot_cents = int(
        (Decimal(str(max(0.0, max_overshoot_usd))) * Decimal("100")).to_integral_value(rounding=ROUND_DOWN)
    )
    for cents in range(max(min_cents, amount_cents + 1), amount_cents + overshoot_cents + 1):
        amount = _valid_amount(cents)
        if amount is not None:
            return float(amount)
    if below_min_amount is not None:
        return float(below_min_amount)
    return float(_quantize_down(Decimal(str(size_usd)), 2))


class TradeExecutor:
    """
    Manages all trade execution against Polymarket via py_clob_client.

    Initialization sequence:
    1. __init__: Check for py_clob_client, load positions file
    2. initialize(): Derive API credentials asynchronously

    Mock Mode:
        If py_clob_client is unavailable or PRIVATE_KEY not set, all operations
        return mock responses with full position tracking for paper/live parity
        checks.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize TradeExecutor with configuration.

        Args:
            config: Config instance with PRIVATE_KEY, CHAIN_ID, CLOB_HOST, and
                optional POLYMARKET_PROXY.

        Sets mock_mode=True if py_clob_client unavailable or PRIVATE_KEY not set.
        Loads existing positions from config.positions_path.
        """
        self.config = config
        self.client: Optional[ClobClient] = None
        self.mock_mode = False
        self._positions: List[Dict[str, Any]] = []
        self._cached_live_balance_usd: Optional[float] = None
        self._balance_guard_lock = asyncio.Lock()
        self._positions_save_lock = asyncio.Lock()

        # Check if we can use real CLOB client
        if not CLOB_AVAILABLE:
            self.mock_mode = True
            logger.warning("py_clob_client not available, running in mock mode")
        elif not config.private_key:
            self.mock_mode = True
            logger.warning("PRIVATE_KEY not configured, running in mock mode")
        else:
            logger.info(f"CLOB mode enabled, package={CLOB_CLIENT_PACKAGE}, host={config.clob_host}")

        # Load existing positions
        self._load_positions()

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _execution_lane_tag(self, decision: Dict[str, Any]) -> str:
        metadata = decision.get("metadata") if isinstance(decision.get("metadata"), dict) else {}
        wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
        wallet_copy_metadata = (
            wallet_copy.get("metadata")
            if isinstance(wallet_copy.get("metadata"), dict)
            else {}
        )
        for raw in (
            decision.get("execution_lane"),
            decision.get("lane"),
            metadata.get("execution_lane"),
            metadata.get("lane"),
            wallet_copy_metadata.get("execution_lane"),
            wallet_copy_metadata.get("lane"),
        ):
            lane = str(raw or "").strip()
            if lane:
                return lane
        return "mock" if self.mock_mode else "clob_unlabeled"

    @classmethod
    def _book_side_rows(cls, book: Any, side: str) -> list[tuple[float, float]]:
        if isinstance(book, dict):
            raw_rows = book.get(side) or []
        else:
            raw_rows = getattr(book, side, []) or []
        rows: list[tuple[float, float]] = []
        for row in raw_rows:
            if isinstance(row, dict):
                price = cls._to_float(row.get("price"))
                size = cls._to_float(row.get("size"))
            else:
                price = cls._to_float(getattr(row, "price", 0.0))
                size = cls._to_float(getattr(row, "size", 0.0))
            if price > 0 and size > 0:
                rows.append((price, size))
        return rows

    def _book_snapshot_timeout_s(self) -> float:
        return max(0.1, float(getattr(self.config, "wallet_copy_book_snapshot_timeout_s", 2.0) or 2.0))

    def _wallet_copy_chase_settings(self) -> tuple[int, float, float]:
        max_ticks = max(0, int(getattr(self.config, "wallet_copy_max_chase_ticks", 0) or 0))
        max_price = max(0.0, float(getattr(self.config, "wallet_copy_chase_max_price", 0.0) or 0.0))
        tick_size = max(0.0, float(getattr(self.config, "wallet_copy_chase_tick_size", 0.01) or 0.0))
        return max_ticks, max_price, tick_size

    def _wallet_copy_max_buy_price(self) -> float:
        return max(0.0, float(getattr(self.config, "wallet_copy_max_buy_price", 0.0) or 0.0))

    def _wallet_copy_min_buy_price(self) -> float:
        return max(0.0, float(getattr(self.config, "wallet_copy_min_buy_price", 0.0) or 0.0))

    @staticmethod
    def _is_wallet_copy_hot_taker_submit(decision: Dict[str, Any], order_type: str) -> bool:
        strategy_reason = str(decision.get("strategy_reason") or "")
        return (
            str(order_type).upper() in {"FAK", "FOK"}
            and (
                strategy_reason.startswith("wallet_copy")
                or isinstance(decision.get("wallet_copy"), dict)
                or str(decision.get("adaptive_mode") or "") == "wallet_copy_inventory"
            )
        )

    @classmethod
    def _wallet_copy_policy_max_order_usd(cls, decision: Dict[str, Any]) -> float:
        """Best-effort wallet-copy policy cap carried on live intent metadata."""
        wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
        metadata = wallet_copy.get("metadata") if isinstance(wallet_copy.get("metadata"), dict) else {}
        policy = metadata.get("wallet_copy_policy") if isinstance(metadata.get("wallet_copy_policy"), dict) else {}
        effective_cap = cls._to_float(policy.get("effective_live_cap_usd"))
        if effective_cap > 0:
            return effective_cap
        payloads = [
            decision,
            wallet_copy,
            policy,
            metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {},
        ]
        caps: list[float] = []
        for payload in payloads:
            for key in ("max_order_usd", "policy_max_order_usd"):
                value = cls._to_float(payload.get(key)) if isinstance(payload, dict) else 0.0
                if value > 0:
                    caps.append(value)
        return min(caps) if caps else 0.0

    @classmethod
    def _wallet_copy_policy_value(cls, decision: Dict[str, Any], key: str) -> float:
        wallet_copy = decision.get("wallet_copy") if isinstance(decision.get("wallet_copy"), dict) else {}
        metadata = wallet_copy.get("metadata") if isinstance(wallet_copy.get("metadata"), dict) else {}
        policy = metadata.get("wallet_copy_policy") if isinstance(metadata.get("wallet_copy_policy"), dict) else {}
        return cls._to_float(policy.get(key))

    def _wallet_copy_chase_limit_price(
        self,
        *,
        action: str,
        order_type: str,
        copied_limit: float,
        max_buy_price: float = 0.0,
    ) -> tuple[float, dict[str, Any]]:
        max_ticks, max_price, tick_size = self._wallet_copy_chase_settings()
        hard_buy_cap = max(0.0, float(max_buy_price or 0.0))
        effective_cap = min(
            value for value in (1.0, max_price, hard_buy_cap) if value > 0
        )
        metadata = {
            "max_chase_ticks": max_ticks,
            "chase_max_price": round(max_price, 6),
            "chase_tick_size": round(tick_size, 6),
            "hard_max_buy_price": round(hard_buy_cap, 6),
            "effective_chase_cap": round(effective_cap, 6),
            "copied_limit_price": round(float(copied_limit), 6) if copied_limit > 0 else 0.0,
            "effective_limit_price": round(float(copied_limit), 6) if copied_limit > 0 else 0.0,
            "chase_ticks_applied": 0,
            "chase_enabled": False,
        }
        if (
            str(action).lower() != "buy"
            or str(order_type).upper() not in {"FAK", "FOK"}
            or copied_limit <= 0
            or max_ticks <= 0
            or max_price <= 0
            or tick_size <= 0
            or copied_limit > effective_cap + 1e-9
        ):
            return copied_limit, metadata
        effective_limit = min(effective_cap, copied_limit + (max_ticks * tick_size))
        effective_limit = round(effective_limit, 6)
        metadata.update(
            {
                "effective_limit_price": effective_limit,
                "chase_ticks_applied": max_ticks if effective_limit > copied_limit + 1e-9 else 0,
                "chase_enabled": effective_limit > copied_limit,
            }
        )
        return effective_limit, metadata

    async def _wallet_copy_post_submit_book_snapshot(
        self,
        *,
        token_id: str,
        copied_limit: float,
    ) -> dict[str, Any]:
        """Fetch non-blocking execution-quality evidence after a live submit attempt."""

        payload = {
            "status": "NOT_FETCHED",
            "token_id": str(token_id or ""),
            "copied_limit": round(float(copied_limit), 6) if copied_limit > 0 else 0.0,
            "max_chase_ticks": self._wallet_copy_chase_settings()[0],
            "chase_max_price": round(self._wallet_copy_chase_settings()[1], 6),
            "chase_tick_size": round(self._wallet_copy_chase_settings()[2], 6),
        }
        if self.mock_mode or self.client is None:
            return {**payload, "status": "SKIPPED", "book_error": "clob_client_unavailable"}
        get_order_book = getattr(self.client, "get_order_book", None)
        if not callable(get_order_book):
            return {**payload, "status": "SKIPPED", "book_error": "get_order_book_unavailable"}
        if not str(token_id or ""):
            return {**payload, "status": "SKIPPED", "book_error": "token_id_missing"}

        loop = asyncio.get_event_loop()
        started = _time.monotonic()
        try:
            book = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: get_order_book(str(token_id))),
                timeout=self._book_snapshot_timeout_s(),
            )
            asks = sorted(self._book_side_rows(book, "asks"), key=lambda item: item[0])
            bids = sorted(self._book_side_rows(book, "bids"), key=lambda item: item[0], reverse=True)
            best_ask = asks[0][0] if asks else 0.0
            best_bid = bids[0][0] if bids else 0.0
            ask_depth_shares = sum(size for price, size in asks if price <= float(copied_limit))
            ask_depth_usd = sum(price * size for price, size in asks if price <= float(copied_limit))
            best_ask_depth_shares = sum(size for price, size in asks if best_ask > 0 and price <= best_ask)
            best_ask_depth_usd = sum(price * size for price, size in asks if best_ask > 0 and price <= best_ask)
            executable_delta = (best_ask - float(copied_limit)) if best_ask > 0 and copied_limit > 0 else 0.0
            return {
                **payload,
                "status": "OK",
                "best_bid": round(best_bid, 6),
                "best_ask": round(best_ask, 6),
                "spread": round(best_ask - best_bid, 6) if best_ask and best_bid else 0.0,
                "ask_depth_at_or_below_limit_shares": round(ask_depth_shares, 6),
                "ask_depth_at_or_below_limit_usd": round(ask_depth_usd, 6),
                "ask_depth_at_or_below_best_ask_shares": round(best_ask_depth_shares, 6),
                "ask_depth_at_or_below_best_ask_usd": round(best_ask_depth_usd, 6),
                "executable_delta": round(executable_delta, 6),
                "book_ts": datetime.now(timezone.utc).isoformat(),
                "fetch_ms": int((_time.monotonic() - started) * 1000),
            }
        except Exception as exc:
            return {
                **payload,
                "status": "ERROR",
                "book_error": str(exc)[:500],
                "fetch_ms": int((_time.monotonic() - started) * 1000),
            }

    @staticmethod
    def _extract_poly_order_id(error_text: str) -> str:
        match = re.search(r"orderID[\"']?\s*[:=]\s*[\"']([^\"']+)", error_text)
        return match.group(1) if match else ""

    @staticmethod
    def _is_fak_no_match(error_text: str) -> bool:
        text = error_text.lower()
        return (
            "fak" in text
            and (
                "no orders found to match" in text
                or "partially filled or killed if no match" in text
            )
        )

    @staticmethod
    def _is_fok_no_match(error_text: str) -> bool:
        text = error_text.lower()
        return (
            "fok" in text
            and (
                "fully filled or killed" in text
                or "couldn't be fully filled" in text
                or "could not be fully filled" in text
            )
        )

    @staticmethod
    def _is_clob_request_exception(error_text: str) -> bool:
        text = error_text.lower()
        return (
            "polyapiexception" in text
            and "status_code=none" in text
            and "request exception" in text
        )

    @staticmethod
    def _truthy_env(value: str) -> bool:
        return value.strip().lower() not in {"0", "false", "no", "off"}

    async def _current_live_balance_for_guard(self) -> float:
        # Live BUY admission must use a fresh CLOB balance when a client is
        # available. The cached value is only a fallback for transient balance
        # read failures, otherwise a stale high balance can leak through after
        # prior fills and produce a CLOB "not enough balance" runtime error.
        if self.client is not None:
            live_balance = await self.get_live_balance()
            if live_balance >= 0:
                return live_balance
        async with self._balance_guard_lock:
            cached = self._cached_live_balance_usd
        if cached is not None and cached >= 0:
            return float(cached)
        return await self.get_live_balance()

    async def _guard_live_buy_capital(
        self,
        *,
        action: str,
        market_id: str,
        clob_side: str,
        outcome_side: str,
        size_usd: float,
        limit_price: float,
        order_size: float,
        order_type: str,
        effective_order_usd: float,
    ) -> Optional[Dict[str, Any]]:
        if action != "buy" or effective_order_usd <= 0:
            return None
        enabled = bool(
            getattr(
                self.config,
                "live_capital_guard_enabled",
                self._truthy_env(os.getenv("LIVE_CAPITAL_GUARD_ENABLED", "true")),
            )
        )
        if not enabled:
            return None

        min_buffer = float(
            getattr(
                self.config,
                "live_capital_min_buffer_usd",
                os.getenv("LIVE_CAPITAL_MIN_BUFFER_USD", "0.0"),
            )
            or 0.0
        )
        live_balance = await self._current_live_balance_for_guard()
        if live_balance < 0:
            return None

        required_balance_usd = round(float(effective_order_usd) + max(0.0, min_buffer), 6)
        if live_balance >= required_balance_usd:
            return None

        error_msg = (
            f"insufficient_live_capital: clob_cash=${live_balance:.6f} "
            f"< required=${required_balance_usd:.6f} for order=${effective_order_usd:.2f}"
        )
        logger.warning(
            "Live capital guard skipped BUY before CLOB submit: "
            "market=%s outcome=%s order_type=%s price=%.3f "
            "order=$%.2f clob_cash=$%.6f buffer=$%.2f",
            market_id,
            outcome_side,
            order_type,
            limit_price,
            effective_order_usd,
            live_balance,
            min_buffer,
        )
        return {
            "status": "unfilled",
            "final_status": "insufficient_live_capital",
            "order_id": "",
            "market_id": market_id,
            "side": clob_side,
            "outcome": outcome_side,
            "size_usd": size_usd,
            "entry_price": limit_price,
            "order_size": order_size,
            "order_type": order_type,
            "fill_size_shares": 0.0,
            "filled_size_usd": 0.0,
            "unfilled_size_usd": size_usd,
            "fill_ratio": 0.0,
            "error_class": "insufficient_live_capital",
            "error": error_msg,
            "clob_balance_usd": live_balance,
            "required_balance_usd": required_balance_usd,
            "effective_order_usd": float(effective_order_usd),
        }

    def _post_response_fill_truth(
        self,
        response: Dict[str, Any],
        side: str,
        limit_price: float,
    ) -> Dict[str, Any]:
        """Normalize CLOB post_order response into executable fill facts.

        For market BUY responses Polymarket reports takingAmount as outcome
        shares received and makingAmount as USDC spent.
        """
        taking_amount = self._to_float(response.get("takingAmount"))
        making_amount = self._to_float(response.get("makingAmount"))
        tx_hashes = self._response_tx_hashes(response)

        if side.upper() == "BUY":
            fill_size_shares = taking_amount
            filled_size_usd = making_amount
        else:
            fill_size_shares = making_amount
            filled_size_usd = taking_amount

        response_fill_price = (
            filled_size_usd / fill_size_shares
            if fill_size_shares > 0 and filled_size_usd > 0
            else limit_price
        )
        realized_entry_price = (
            filled_size_usd / fill_size_shares
            if fill_size_shares > 0 and filled_size_usd > 0
            else None
        )
        realized_entry_band = (
            "00_00_25"
            if realized_entry_price is not None and realized_entry_price < 0.25
            else "01a_25_32"
            if realized_entry_price is not None and realized_entry_price < 0.32
            else "01b_32_40"
            if realized_entry_price is not None and realized_entry_price < 0.40
            else "01c_40_50"
            if realized_entry_price is not None and realized_entry_price < 0.50
            else "02_50_70"
            if realized_entry_price is not None and realized_entry_price < 0.70
            else "03_70_85"
            if realized_entry_price is not None and realized_entry_price < 0.85
            else "04_85_100"
            if realized_entry_price is not None
            else "UNMEASURED"
        )

        return {
            "post_status": str(response.get("status", "") or "").lower(),
            "post_success": bool(response.get("success", False)),
            "post_error_msg": response.get("errorMsg", ""),
            "tx_hashes": tx_hashes,
            "taking_amount": taking_amount,
            "making_amount": making_amount,
            "response_fill_size_shares": fill_size_shares,
            "response_filled_size_usd": filled_size_usd,
            "response_fill_price": response_fill_price if response_fill_price > 0 else None,
            "realized_entry_price": (
                round(realized_entry_price, 9) if realized_entry_price is not None else None
            ),
            "realized_entry_band": realized_entry_band,
            "out_of_band_fill": bool(
                realized_entry_price is not None
                and not (0.25 <= realized_entry_price < 0.32)
            ),
        }

    @staticmethod
    def _response_tx_hashes(response: Dict[str, Any]) -> list[str]:
        tx_hashes: list[str] = []

        def visit(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, list):
                for item in value:
                    visit(item)
                return
            if isinstance(value, dict):
                for key in (
                    "transactionsHashes",
                    "transactionHashes",
                    "transactionHash",
                    "txHash",
                    "hash",
                ):
                    visit(value.get(key))
                for key in ("details", "raw", "response"):
                    visit(value.get(key))
                return
            text = str(value).strip()
            if text and text.lower().startswith("0x") and text not in tx_hashes:
                tx_hashes.append(text)

        visit(response)
        return tx_hashes

    def _load_positions(self) -> None:
        """Load positions from JSON file."""
        positions_path = Path(self.config.positions_path)
        try:
            if positions_path.exists():
                if positions_path.stat().st_size == 0:
                    self._positions = []
                    logger.warning("Positions file is empty; starting with no cached positions: %s", positions_path)
                    return
                with open(positions_path, "r") as f:
                    data = json.load(f)
                    self._positions = data if isinstance(data, list) else []
                    logger.debug(f"Loaded {len(self._positions)} positions from {positions_path}")
            else:
                self._positions = []
                logger.debug(f"No existing positions file at {positions_path}")
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading positions from {positions_path}: {e}")
            self._positions = []

    async def initialize(self) -> None:
        """
        Async initialization: derive API key and set credentials.

        Must be called after __init__ and before any trading operations.
        Uses run_in_executor to wrap synchronous ClobClient initialization.

        Raises:
            RuntimeError: If API credential derivation fails in real mode.
        """
        if self.mock_mode:
            logger.info("Mock mode: skipping API initialization")
            return

        try:
            loop = asyncio.get_event_loop()

            # Initialize ClobClient (synchronous)
            def _init_client() -> ClobClient:
                # Polymarket proxy wallet: sig_type=1 (Gnosis Safe / proxy)
                # sig_type=0 = plain EOA (no proxy), sig_type=1 = proxy wallet with balance
                funder = self.config.polymarket_proxy if self.config.polymarket_proxy else None
                sig_type = 1 if funder else 0
                return ClobClient(
                    host=self.config.clob_host,
                    key=self.config.private_key,
                    chain_id=self.config.chain_id,
                    funder=funder,
                    signature_type=sig_type,
                )

            self.client = await loop.run_in_executor(None, _init_client)
            logger.info("ClobClient initialized")

            # Derive API credentials (synchronous)
            def _derive_creds() -> Dict[str, str]:
                if not self.client:
                    raise RuntimeError("ClobClient not initialized")
                if hasattr(self.client, "create_or_derive_api_key"):
                    creds = self.client.create_or_derive_api_key()
                elif hasattr(self.client, "create_or_derive_api_creds"):
                    creds = self.client.create_or_derive_api_creds()
                elif hasattr(self.client, "derive_api_key"):
                    creds = self.client.derive_api_key()
                else:
                    raise RuntimeError("ClobClient has no supported API credential derivation method")
                return creds

            creds = await loop.run_in_executor(None, _derive_creds)
            logger.debug("API credentials derived successfully")

            # Set credentials on client (synchronous)
            def _set_creds() -> None:
                if not self.client:
                    raise RuntimeError("ClobClient not initialized")
                self.client.set_api_creds(creds)

            await loop.run_in_executor(None, _set_creds)
            logger.info("API credentials configured on ClobClient")

            # Builder keys noted but we do NOT re-derive — that would overwrite valid creds
            if self.config.builder_api_key:
                logger.info("Builder API credentials configured (gasless enabled)")

        except Exception as e:
            logger.error(f"Failed to initialize TradeExecutor: {e}")
            raise RuntimeError(f"API initialization failed: {e}") from e

    @retry_async(max_attempts=1, delay=0.5, backoff=2.0)
    async def execute_trade(self, decision: Dict[str, Any], size_usd: float) -> Dict[str, Any]:
        """
        Execute a wallet-copy intent translated to a Polymarket order decision.

        Args:
            decision: Dict with keys: action, side, limit_price, market_id,
                     clob_token_ids (list of [YES_token, NO_token]), etc.
            size_usd: Position size in USD

        Returns:
            Dict with status, order_id, details.

        Polymarket mapping:
            action="buy",  side="YES" → BUY with YES token_id
            action="buy",  side="NO"  → BUY with NO token_id
            action="sell", side="YES" → SELL with YES token_id
            action="sell", side="NO"  → SELL with NO token_id

        In mock mode, returns mock result and still tracks position.
        """
        # Validate required fields
        required_fields = {"action", "side", "limit_price"}
        missing = required_fields - set(decision.keys())
        if missing:
            error_msg = f"Decision missing fields: {missing}"
            logger.error(error_msg)
            return {"status": "error", "order_id": "", "error": error_msg}

        market_id = decision.get("market_id", "unknown")
        action = decision["action"].lower()  # "buy" or "sell"
        outcome_side = decision["side"].upper()  # "YES" or "NO"
        copied_limit_price = float(decision["limit_price"])
        limit_price = copied_limit_price
        max_order_usd = float(
            getattr(self.config, "max_order_usd", os.getenv("MAX_ORDER_USD", "50.0")) or 0.0
        )
        if max_order_usd > 0 and float(size_usd) > max_order_usd:
            error_msg = (
                f"order_size_hard_cap_exceeded: requested ${float(size_usd):.2f} "
                f"> cap ${max_order_usd:.2f}"
            )
            logger.error(error_msg)
            return {
                "status": "error",
                "final_status": "order_size_hard_cap_exceeded",
                "order_id": "",
                "market_id": market_id,
                "side": "BUY" if action == "buy" else "SELL",
                "outcome": outcome_side,
                "size_usd": float(size_usd),
                "entry_price": limit_price,
                "order_size": 0.0,
                "order_type": str(decision.get("order_type") or "GTC").upper(),
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": float(size_usd),
                "fill_ratio": 0.0,
                "error_class": "order_size_hard_cap_exceeded",
                "error": error_msg,
            }
        requested_size_shares = self._to_float(decision.get("size_shares"))
        order_size = (
            requested_size_shares
            if requested_size_shares > 0
            else size_usd / limit_price if limit_price > 0 else 0
        )

        # Map action → CLOB Side enum
        clob_side = "BUY" if action == "buy" else "SELL"

        # Resolve the correct token_id for the chosen outcome
        clob_token_ids = decision.get("clob_token_ids", [])
        if outcome_side == "YES" and len(clob_token_ids) >= 1:
            token_id = clob_token_ids[0]
        elif outcome_side == "NO" and len(clob_token_ids) >= 2:
            token_id = clob_token_ids[1]
        else:
            token_id = market_id
            logger.warning(f"No clob_token_ids for {outcome_side}, using market_id as token_id")

        # Optional order type override (decision dict may specify "GTC" | "FOK" | "FAK" | "GTD")
        order_type_override = decision.get("order_type")  # "GTC" | "FOK" | "FAK" | "GTD" | None
        post_only_strict = bool(decision.get("post_only_strict", False))
        order_type_upper = str(order_type_override or "GTC").upper()
        skip_post_submit_book_snapshot = self._is_wallet_copy_hot_taker_submit(decision, order_type_upper)
        min_buy_price = self._wallet_copy_min_buy_price()
        max_buy_price = self._wallet_copy_max_buy_price()
        if action == "buy" and min_buy_price > 0 and copied_limit_price < min_buy_price - 1e-9:
            return {
                "status": "unfilled",
                "final_status": "price_band_skip",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": copied_limit_price,
                "order_size": 0.0,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "price_band_skip",
                "error": (
                    "wallet-copy BUY copied price below configured min buy price: "
                    f"{copied_limit_price:.6f} < {min_buy_price:.6f}"
                ),
                "wallet_copy_price_band": {
                    "taxonomy": "price_band_skip",
                    "copied_price": round(copied_limit_price, 6),
                    "min_buy_price": round(min_buy_price, 6),
                    "max_buy_price": round(max_buy_price, 6),
                },
            }
        if action == "buy" and max_buy_price > 0 and copied_limit_price > max_buy_price + 1e-9:
            return {
                "status": "unfilled",
                "final_status": "price_band_skip",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": copied_limit_price,
                "order_size": 0.0,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "price_band_skip",
                "error": (
                    "wallet-copy BUY copied price exceeds configured max buy price: "
                    f"{copied_limit_price:.6f} > {max_buy_price:.6f}"
                ),
                "wallet_copy_price_band": {
                    "taxonomy": "price_band_skip",
                    "copied_price": round(copied_limit_price, 6),
                    "min_buy_price": round(min_buy_price, 6),
                    "max_buy_price": round(max_buy_price, 6),
                },
            }
        limit_price, chase_metadata = self._wallet_copy_chase_limit_price(
            action=action,
            order_type=order_type_upper,
            copied_limit=copied_limit_price,
            max_buy_price=max_buy_price,
        )
        if action == "buy" and max_buy_price > 0 and limit_price > max_buy_price + 1e-9:
            return {
                "status": "unfilled",
                "final_status": "price_band_skip",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": 0.0,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "price_band_skip",
                "error": (
                    "wallet-copy BUY effective price exceeds configured max buy price after chase: "
                    f"{limit_price:.6f} > {max_buy_price:.6f}"
                ),
                "wallet_copy_price_band": {
                    "taxonomy": "price_band_skip",
                    "copied_price": round(copied_limit_price, 6),
                    "effective_price": round(limit_price, 6),
                    "min_buy_price": round(min_buy_price, 6),
                    "max_buy_price": round(max_buy_price, 6),
                },
                "wallet_copy_chase": dict(chase_metadata),
            }

        # Calculate share count. Normal share-sized routes may round up to the
        # known CLOB minimum, but market BUY FAK/FOK orders are USDC-denominated
        # and must not be reported or balance-guarded as a larger share order.
        MIN_SHARES = WALLET_COPY_MIN_SHARES
        raw_shares = (
            requested_size_shares
            if requested_size_shares > 0
            else size_usd / limit_price if limit_price > 0 else 0
        )
        maker_min_share_funding_cap_usd = self._wallet_copy_policy_value(
            decision, "maker_min_share_funding_cap_usd"
        )
        maker_min_share_original_policy_cap_usd = self._wallet_copy_policy_value(
            decision, "maker_min_share_original_policy_cap_usd"
        )
        maker_min_share_base_request_cap_usd = self._wallet_copy_policy_value(
            decision, "maker_min_share_base_request_cap_usd"
        )
        maker_fallback_defense_cap_usd = self._wallet_copy_policy_value(
            decision, "maker_fallback_defense_cap_usd"
        )
        maker_min_share_funding_eligible = bool(
            action == "buy"
            and order_type_upper == "GTC"
            and post_only_strict
            and str(decision.get("strategy_reason") or "")
            in {
                "wallet_copy_passive_at_source",
                "wallet_copy_passive_at_source_shadow_measurement",
            }
            and isinstance(decision.get("wallet_copy"), dict)
            and maker_min_share_base_request_cap_usd > 0
            and 0
            < float(size_usd)
            <= maker_min_share_base_request_cap_usd + 1e-9
            and raw_shares < MIN_SHARES
            and 0.25 - 1e-9 <= limit_price <= 0.50 + 1e-9
            and maker_min_share_funding_cap_usd > 0
            and maker_min_share_funding_cap_usd
            <= WALLET_COPY_MIN_SHARE_HARD_CAP_USD + 1e-9
            and MIN_SHARES * limit_price <= maker_min_share_funding_cap_usd + 1e-9
            and maker_min_share_original_policy_cap_usd >= maker_min_share_funding_cap_usd - 1e-9
        )
        maker_fallback_min_share_eligible = bool(
            action == "buy"
            and order_type_upper == "GTC"
            and post_only_strict
            and str(decision.get("strategy_reason") or "")
            == "wallet_copy_fak_miss_maker_fallback"
            and isinstance(decision.get("wallet_copy_maker_fallback"), dict)
            and raw_shares < MIN_SHARES
        )
        wallet_copy_payload = (
            decision.get("wallet_copy")
            if isinstance(decision.get("wallet_copy"), dict)
            else {}
        )
        wallet_copy_metadata = (
            wallet_copy_payload.get("metadata")
            if isinstance(wallet_copy_payload.get("metadata"), dict)
            else {}
        )
        wallet_copy_policy = (
            wallet_copy_metadata.get("wallet_copy_policy")
            if isinstance(wallet_copy_metadata.get("wallet_copy_policy"), dict)
            else {}
        )
        order137_pinned_seat = bool(
            action == "buy"
            and order_type_upper == "GTC"
            and post_only_strict
            and raw_shares < MIN_SHARES
            and raw_shares > 0
            and str(wallet_copy_payload.get("source_wallet") or "").lower()
            == ORDER137_PINNED_WALLET
            and str(
                wallet_copy_payload.get("policy_id")
                or wallet_copy_policy.get("policy_id")
                or ""
            )
            == ORDER137_PINNED_POLICY_ID
            # Evaluated last so the state file is read only for the pinned seat.
            and not _order137_revert_active()
        )
        allow_sell_below_min_shares = (
            action == "sell"
            and bool(decision.get("allow_sell_below_min_shares", False))
            and str(decision.get("strategy_reason") or "") == "bounded_hedge_orphan_exit"
        )
        allow_passive_precision_below_min_shares = (
            action == "buy"
            and bool(decision.get("allow_passive_precision_below_min_shares", False))
            and bool(decision.get("post_only_strict", False))
            and str(decision.get("strategy_reason") or "")
            in {
                "wallet_copy_passive_at_source",
                "wallet_copy_passive_at_source_shadow_measurement",
            }
        )
        skip_if_below_min_shares = bool(decision.get("skip_if_below_min_shares", False))
        is_market_buy = action == "buy" and order_type_upper in ("FAK", "FOK")
        market_order_amount_precision = 2
        market_buy_amount_usd: Optional[float] = None
        if allow_sell_below_min_shares:
            order_size = round(raw_shares, 6)
            market_order_amount_precision = 6
        elif maker_min_share_funding_eligible or order137_pinned_seat:
            order_size = MIN_SHARES
        elif allow_passive_precision_below_min_shares:
            order_size = round(raw_shares, 2)
        elif is_market_buy:
            market_buy_amount_usd = _market_buy_amount_with_valid_share_precision(
                size_usd, limit_price, min_amount_usd=1.0
            )
            order_size = round(market_buy_amount_usd / limit_price, 6) if limit_price > 0 else 0.0
        elif skip_if_below_min_shares:
            order_size = round(raw_shares, 6)
        else:
            order_size = round(max(raw_shares, MIN_SHARES), 2)
        if (
            skip_if_below_min_shares
            and raw_shares < MIN_SHARES
            and raw_shares > 0
            and not is_market_buy
        ):
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "maker_fallback_below_min_shares",
                "error": (
                    "same-size maker fallback would be below CLOB resting-order minimum: "
                    f"{raw_shares:.6f} < {MIN_SHARES:.2f} shares"
                ),
                "maker": True,
                "wallet_copy_maker_fallback": decision.get("wallet_copy_maker_fallback") or {},
            }
        if is_market_buy and market_buy_amount_usd is not None and market_buy_amount_usd < 1.0:
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "market_buy_precision_below_min_order",
                "error": (
                    "precision-safe market BUY amount would be below CLOB min order: "
                    f"${market_buy_amount_usd:.2f}"
                ),
                "market_order_amount_usd": market_buy_amount_usd,
                "market_order_amount_adjustment_usd": round(
                    float(size_usd) - float(market_buy_amount_usd),
                    6,
                ),
                "wallet_copy_chase": dict(chase_metadata),
                "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                    token_id=token_id,
                    copied_limit=copied_limit_price,
                ),
            }
        wallet_copy_policy_cap_usd = self._wallet_copy_policy_max_order_usd(decision)
        precision_cap_overshoot_usd = 0.1
        precision_adjustment_usd = (
            float(market_buy_amount_usd) - float(size_usd)
            if market_buy_amount_usd is not None
            else 0.0
        )
        precision_cap_limit_usd = wallet_copy_policy_cap_usd
        if (
            wallet_copy_policy_cap_usd > 0
            and size_usd <= wallet_copy_policy_cap_usd + 1e-9
            and precision_adjustment_usd > 1e-9
        ):
            precision_cap_limit_usd = wallet_copy_policy_cap_usd + precision_cap_overshoot_usd
        if (
            is_market_buy
            and market_buy_amount_usd is not None
            and wallet_copy_policy_cap_usd > 0
            and market_buy_amount_usd > precision_cap_limit_usd + 1e-9
        ):
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "market_buy_precision_cap_exceeded",
                "error": (
                    "precision-safe market BUY amount would exceed wallet-copy policy cap: "
                    f"${market_buy_amount_usd:.2f} > ${wallet_copy_policy_cap_usd:.2f}"
                ),
                "market_order_amount_usd": market_buy_amount_usd,
                "market_order_amount_adjustment_usd": round(
                    float(size_usd) - float(market_buy_amount_usd),
                    6,
                ),
                "wallet_copy_policy_cap_usd": round(wallet_copy_policy_cap_usd, 6),
                "wallet_copy_precision_cap_limit_usd": round(precision_cap_limit_usd, 6),
                "wallet_copy_chase": dict(chase_metadata),
                "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                    token_id=token_id,
                    copied_limit=copied_limit_price,
                ),
            }
        maker_min_share_bump_cost_usd = round(MIN_SHARES * limit_price, 6) if limit_price > 0 else 0.0
        maker_min_share_effective_cap_usd = (
            maker_min_share_funding_cap_usd
            if maker_min_share_funding_eligible
            else min(
                WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
                maker_fallback_defense_cap_usd,
            )
            if maker_fallback_min_share_eligible
            and maker_fallback_defense_cap_usd > 0
            else WALLET_COPY_MIN_SHARE_HARD_CAP_USD
            if maker_fallback_min_share_eligible
            else min(
                WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
                max(wallet_copy_policy_cap_usd, maker_min_share_bump_cost_usd),
            )
            if order137_pinned_seat
            else wallet_copy_policy_cap_usd
        )
        if action == "buy" and str(decision.get("strategy_reason") or "") == "wallet_copy_passive_at_source":
            error_class = "passive_at_source_lane_closed"
            assert error_class in PRE_SUBMIT_REFUSAL_CLASSES
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": error_class,
                "error": (
                    "passive-at-source live lane closed by 2026-08-02T16:52Z Fable DIRECTION; "
                    "evidence=data/research/passive_at_source_holdout_latest.json"
                ),
                "maker": True,
                "measurement_shadow_continues": True,
                "passive_lane_closure_direction_id": "2026-08-02T16:52Z-fable-passive-lane-closure",
            }
        if (
            action == "buy"
            and not is_market_buy
            and raw_shares < MIN_SHARES
            and raw_shares > 0
            and (
                maker_min_share_funding_cap_usd > 0
                or maker_fallback_min_share_eligible
                or order137_pinned_seat
            )
            and maker_min_share_bump_cost_usd
            > WALLET_COPY_MIN_SHARE_HARD_CAP_USD + 1e-9
        ):
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "venue_min_share_hard_ceiling_exceeded",
                "error": (
                    "CLOB five-share minimum would exceed the Fable-ruled hard ceiling: "
                    f"{MIN_SHARES:.0f} shares @ {limit_price:.6f} = "
                    f"${maker_min_share_bump_cost_usd:.6f} > "
                    f"${WALLET_COPY_MIN_SHARE_HARD_CAP_USD:.6f}"
                ),
                "maker": bool(post_only_strict),
                "maker_min_share_bump_cost_usd": maker_min_share_bump_cost_usd,
                "maker_min_share_hard_cap_usd": WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
                "maker_min_share_funding_cap_usd": round(
                    maker_min_share_funding_cap_usd, 6
                ),
                "maker_min_share_effective_cap_usd": round(
                    maker_min_share_effective_cap_usd, 6
                ),
            }
        if (
            action == "buy"
            and not is_market_buy
            and not allow_sell_below_min_shares
            and raw_shares < MIN_SHARES
            and raw_shares > 0
            and wallet_copy_policy_cap_usd > 0
            and maker_min_share_bump_cost_usd > maker_min_share_effective_cap_usd + 1e-9
            and (
                not allow_passive_precision_below_min_shares
                or maker_min_share_funding_cap_usd > 0
            )
        ):
            return {
                "status": "unfilled",
                "final_status": "unfilled",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "maker_min_share_bump_exceeds_policy_cap",
                "error": (
                    "CLOB min-share bump would exceed wallet-copy policy cap: "
                    f"{MIN_SHARES:.0f} shares @ {limit_price:.6f} = "
                    f"${maker_min_share_bump_cost_usd:.6f} > ${maker_min_share_effective_cap_usd:.6f}"
                ),
                "maker": True,
                "wallet_copy_policy_cap_usd": round(wallet_copy_policy_cap_usd, 6),
                "maker_min_share_effective_cap_usd": round(
                    maker_min_share_effective_cap_usd, 6
                ),
                "maker_min_share_bump_cost_usd": maker_min_share_bump_cost_usd,
                "wallet_copy_maker_fallback": decision.get("wallet_copy_maker_fallback") or {},
            }
        if (
            raw_shares < MIN_SHARES
            and raw_shares > 0
            and not allow_sell_below_min_shares
            and not allow_passive_precision_below_min_shares
            and not is_market_buy
        ):
            logger.warning(
                f"MIN_SHARES bump: {raw_shares:.1f}→{order_size:.1f} shares "
                f"(${size_usd:.2f}→${order_size * limit_price:.2f} effective)"
            )

        logger.info(
            f"Executing trade: lane={self._execution_lane_tag(decision)}, "
            f"market={market_id}, action={action}, "
            f"outcome={outcome_side}, clob_side={clob_side}, token={token_id[:20]}..., "
            f"size_usd={size_usd}, copied_limit={copied_limit_price}, "
            f"limit_price={limit_price}, order_size={order_size:.2f}"
        )

        if self.mock_mode:
            return await self._execute_mock_trade(
                market_id, clob_side, size_usd, limit_price, order_size
            )

        effective_order_usd = 0.0
        if action == "buy":
            effective_order_usd = (
                round(order_size * limit_price, 2)
                if order_type_upper in ("GTC", "GTD")
                else round(float(market_buy_amount_usd if market_buy_amount_usd is not None else size_usd), 2)
            )
        capital_guard_result = await self._guard_live_buy_capital(
            action=action,
            market_id=market_id,
            clob_side=clob_side,
            outcome_side=outcome_side,
            size_usd=size_usd,
            limit_price=limit_price,
            order_size=order_size,
            order_type=order_type_upper,
            effective_order_usd=effective_order_usd,
        )
        if capital_guard_result is not None:
            return capital_guard_result

        reserved_balance_usd = await self._reserve_balance_for_order(
            decision=decision,
            market_id=market_id,
            outcome_side=outcome_side,
            effective_order_usd=effective_order_usd,
            limit_price=limit_price,
            order_type=order_type_upper,
        )
        if reserved_balance_usd is None:
            return {
                "status": "unfilled",
                "final_status": "balance_guard",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_size": order_size,
                "order_type": order_type_upper,
                "fill_size_shares": 0.0,
                "filled_size_usd": 0.0,
                "unfilled_size_usd": size_usd,
                "fill_ratio": 0.0,
                "error_class": "balance_guard",
                "error": "paired inventory available-balance reserve would be breached",
                "wallet_copy_chase": dict(chase_metadata),
            }

        try:
            result = await self._execute_real_trade(
                token_id, clob_side, size_usd, limit_price, order_size,
                order_type_str=order_type_override,
                post_only_strict=post_only_strict,
                market_order_amount=(
                    order_size if action == "sell" else market_buy_amount_usd
                ),
                market_order_amount_precision=market_order_amount_precision,
            )
            if market_buy_amount_usd is not None:
                result["market_order_amount_usd"] = market_buy_amount_usd
                result["market_order_amount_adjustment_usd"] = round(
                    float(size_usd) - float(market_buy_amount_usd),
                    6,
                )
            if maker_min_share_funding_eligible:
                result["maker_min_share_funding"] = {
                    "flow_stage": "LIVE/DEFEND",
                    "reason": "venue_minimum_exact_five_share_funding",
                    "requested_notional_usd": round(float(size_usd), 6),
                    "funded_notional_usd": maker_min_share_bump_cost_usd,
                    "funded_shares": MIN_SHARES,
                    "limit_price": round(limit_price, 6),
                    "funding_cap_usd": round(
                        maker_min_share_funding_cap_usd, 6
                    ),
                    "original_policy_cap_usd": round(
                        maker_min_share_original_policy_cap_usd, 6
                    ),
                    "base_request_cap_usd": round(
                        maker_min_share_base_request_cap_usd, 6
                    ),
                    "strict_post_only": True,
                }
            if (
                maker_min_share_funding_eligible
                or maker_fallback_min_share_eligible
                or order137_pinned_seat
            ):
                result["maker_min_share_effective_cap_usd"] = round(
                    maker_min_share_effective_cap_usd, 6
                )
                result["maker_min_share_bump_cost_usd"] = maker_min_share_bump_cost_usd
            if order137_pinned_seat:
                result["order137_venue_min_share_cap"] = {
                    "direction_id": "2026-08-01T15:34Z-fable-order137",
                    "source_wallet": ORDER137_PINNED_WALLET,
                    "policy_id": ORDER137_PINNED_POLICY_ID,
                    "hard_ceiling_usd": WALLET_COPY_MIN_SHARE_HARD_CAP_USD,
                    "status": "APPLIED",
                }
            result["wallet_copy_chase"] = dict(chase_metadata)
            if result.get("status") in ("error", "unfilled") or result.get("final_status") in (
                "post_only_rejected",
                "balance_guard",
                "unfilled",
            ):
                await self._release_balance_reservation(reserved_balance_usd)
            if not skip_post_submit_book_snapshot:
                result["wallet_copy_post_submit_book"] = await self._wallet_copy_post_submit_book_snapshot(
                    token_id=token_id,
                    copied_limit=copied_limit_price,
                )
            return result
        except Exception as e:
            await self._release_balance_reservation(reserved_balance_usd)
            err_str = str(e)
            if order_type_upper == "FAK" and self._is_fak_no_match(err_str):
                order_id = self._extract_poly_order_id(err_str)
                logger.info(
                    "FAK unfilled/no match: market=%s outcome=%s limit=%.3f "
                    "size=$%.2f order_id=%s",
                    market_id,
                    outcome_side,
                    limit_price,
                    size_usd,
                    order_id[:16] if order_id else "",
                )
                return {
                    "status": "unfilled",
                    "final_status": "unfilled",
                    "order_id": order_id,
                    "market_id": market_id,
                    "side": clob_side,
                    "outcome": outcome_side,
                    "size_usd": size_usd,
                    "entry_price": limit_price,
                    "order_size": order_size,
                    "order_type": order_type_upper,
                    "fill_size_shares": 0.0,
                    "filled_size_usd": 0.0,
                    "unfilled_size_usd": size_usd,
                    "fill_ratio": 0.0,
                    "error_class": "fak_no_match",
                    "error": err_str,
                    "wallet_copy_chase": dict(chase_metadata),
                    **(
                        {}
                        if skip_post_submit_book_snapshot
                        else {
                            "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                                token_id=token_id,
                                copied_limit=copied_limit_price,
                            )
                        }
                    ),
                }
            if order_type_upper == "FOK" and self._is_fok_no_match(err_str):
                order_id = self._extract_poly_order_id(err_str)
                logger.info(
                    "FOK unfilled/no match: market=%s outcome=%s limit=%.3f "
                    "size=$%.2f order_id=%s",
                    market_id,
                    outcome_side,
                    limit_price,
                    size_usd,
                    order_id[:16] if order_id else "",
                )
                return {
                    "status": "unfilled",
                    "final_status": "unfilled",
                    "order_id": order_id,
                    "market_id": market_id,
                    "side": clob_side,
                    "outcome": outcome_side,
                    "size_usd": size_usd,
                    "entry_price": limit_price,
                    "order_size": order_size,
                    "order_type": order_type_upper,
                    "fill_size_shares": 0.0,
                    "filled_size_usd": 0.0,
                    "unfilled_size_usd": size_usd,
                    "fill_ratio": 0.0,
                    "error_class": "fok_not_filled",
                    "error": err_str,
                    "wallet_copy_chase": dict(chase_metadata),
                    **(
                        {}
                        if skip_post_submit_book_snapshot
                        else {
                            "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                                token_id=token_id,
                                copied_limit=copied_limit_price,
                            )
                        }
                    ),
                }
            if self._is_clob_request_exception(err_str):
                logger.warning(
                    "CLOB request exception while submitting order: market=%s outcome=%s "
                    "limit=%.3f size=$%.2f order_type=%s error=%s",
                    market_id,
                    outcome_side,
                    limit_price,
                    size_usd,
                    order_type_upper,
                    err_str,
                )
                return {
                    "status": "error",
                    "final_status": "execution_error",
                    "order_id": "",
                    "market_id": market_id,
                    "side": clob_side,
                    "outcome": outcome_side,
                    "size_usd": size_usd,
                    "entry_price": limit_price,
                    "order_size": order_size,
                    "order_type": order_type_upper,
                    "fill_size_shares": 0.0,
                    "filled_size_usd": 0.0,
                    "unfilled_size_usd": size_usd,
                    "fill_ratio": 0.0,
                    "error_class": "clob_request_exception",
                    "error": err_str,
                    "wallet_copy_chase": dict(chase_metadata),
                    **(
                        {}
                        if skip_post_submit_book_snapshot
                        else {
                            "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                                token_id=token_id,
                                copied_limit=copied_limit_price,
                            )
                        }
                    ),
                }
            logger.error(f"Trade execution failed: {e}", exc_info=True)
            return {
                "status": "error",
                "order_id": "",
                "market_id": market_id,
                "side": clob_side,
                "outcome": outcome_side,
                "size_usd": size_usd,
                "entry_price": limit_price,
                "order_type": order_type_upper,
                "error": str(e),
                "wallet_copy_chase": dict(chase_metadata),
                **(
                    {}
                    if skip_post_submit_book_snapshot
                    else {
                        "wallet_copy_post_submit_book": await self._wallet_copy_post_submit_book_snapshot(
                            token_id=token_id,
                            copied_limit=copied_limit_price,
                        )
                    }
                ),
            }

    async def _execute_mock_trade(
        self,
        market_id: str,
        side: str,
        size_usd: float,
        limit_price: float,
        order_size: float,
    ) -> Dict[str, Any]:
        """Execute trade in mock mode."""
        order_id = f"mock_{market_id[:8]}_{datetime.now(timezone.utc).timestamp():.0f}"
        result = {
            "status": "mock",
            "order_id": order_id,
            "market_id": market_id,
            "side": side,
            "size_usd": size_usd,
            "entry_price": limit_price,
            "order_size": order_size,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self._save_positions()
        logger.info(f"Mock trade submitted: {order_id}")
        self._positions.append(result)
        return result

    async def _execute_real_trade(
        self,
        market_id: str,
        side: str,
        size_usd: float,
        limit_price: float,
        order_size: float,
        order_type_str: Optional[str] = None,
        post_only_strict: bool = False,
        market_order_amount: Optional[float] = None,
        market_order_amount_precision: int = 2,
    ) -> Dict[str, Any]:
        """Execute trade against live Polymarket API.

        Raises ClobServerError on HTTP 5xx so the @retry_async decorator
        retries the submission.  Client-side errors (4xx, validation) are
        NOT wrapped and will NOT be retried.
        """
        if not self.client:
            raise RuntimeError("ClobClient not initialized")

        # Resolve order type (default GTC for backwards-compat with contrarian flow)
        order_type_map = {
            "GTC": OrderType.GTC,
            "FOK": OrderType.FOK,
            "FAK": OrderType.FAK,
            "GTD": OrderType.GTD,
        }
        order_type = order_type_map.get((order_type_str or "GTC").upper(), OrderType.GTC)

        loop = asyncio.get_event_loop()

        # FAK/FOK market orders use MarketOrderArgs. Polymarket interprets
        # amount as USDC for BUY and shares for SELL.
        # GTC/GTD use OrderArgs (size = shares)
        # This is because Polymarket's market-buy path requires USDC-denominated amount
        # with 2-decimal precision, which share-based OrderArgs can violate.
        is_market_order = order_type in (OrderType.FAK, OrderType.FOK)

        # ── E2E timing: measure sign + post latency ──
        t0 = _time.monotonic()

        # Create order (synchronous)
        def _create_order() -> Any:
            if not self.client:
                raise RuntimeError("ClobClient not initialized")
            side_str = BUY if side.upper() == "BUY" else SELL
            if is_market_order:
                amount = float(market_order_amount) if market_order_amount is not None else float(size_usd)
                market_args = MarketOrderArgs(
                    token_id=market_id,  # already resolved token_id
                    amount=round(amount, market_order_amount_precision),
                    side=side_str,
                    price=limit_price,
                )
                return self.client.create_market_order(market_args)
            order_args = OrderArgs(
                price=limit_price,
                size=order_size,
                side=side_str,
                token_id=market_id,  # Here market_id is actually the resolved token_id
            )
            return self.client.create_order(order_args)

        signed_order = await loop.run_in_executor(None, _create_order)
        t_signed = _time.monotonic()
        logger.debug(f"Order created for {market_id} (type={order_type}, market={is_market_order})")

        # Post order — GTC/GTD use post_only=True for maker status. If post_only
        # is rejected, strict maker routes return unfilled; legacy routes fall
        # back to a normal taker submission so the trade still goes through.
        # FOK/FAK are never post_only (Polymarket rejects that combo).
        use_post_only = order_type in (OrderType.GTC, OrderType.GTD)

        def _post_order(post_only: bool = False) -> Dict[str, Any]:
            if not self.client:
                raise RuntimeError("ClobClient not initialized")
            return self.client.post_order(signed_order, order_type, post_only=post_only)

        response = None
        post_attempts = 0
        last_post_err = None
        _post_only_rejected = False

        for _attempt in range(3):  # max 3 attempts: post_only → fallback → retry on 5xx
            post_attempts = _attempt + 1
            try:
                _po = use_post_only and not _post_only_rejected
                response = await loop.run_in_executor(None, lambda po=_po: _post_order(po))
                last_post_err = None
                if _po:
                    logger.debug("Order posted as post_only (maker, 0 fee)")
                break  # success
            except Exception as e:
                err_str = str(e).lower()

                # HTTP 425 = matching engine restarting (Tuesdays ~7 AM ET)
                is_engine_restart = "status_code=425" in err_str or "too early" in err_str
                if is_engine_restart:
                    logger.warning(f"Matching engine restarting (HTTP 425), waiting 5s... (attempt {_attempt+1})")
                    last_post_err = e
                    await asyncio.sleep(5.0)
                    continue

                # Post-only rejection: "order crosses book" — retry as normal taker
                is_post_only_reject = (
                    use_post_only and not _post_only_rejected
                    and ("crosses" in err_str or "post-only" in err_str or "post_only" in err_str)
                )
                if is_post_only_reject:
                    if post_only_strict:
                        logger.info(f"Post-only rejected (crosses book), strict maker mode skips: {e}")
                        return {
                            "status": "unfilled",
                            "final_status": "post_only_rejected",
                            "order_id": "",
                            "market_id": market_id,
                            "side": side,
                            "size_usd": size_usd,
                            "entry_price": limit_price,
                            "order_size": order_size,
                            "order_type": (order_type_str or "GTC").upper(),
                            "fill_size_shares": 0.0,
                            "filled_size_usd": 0.0,
                            "unfilled_size_usd": size_usd,
                            "fill_ratio": 0.0,
                            "error_class": "post_only_rejected",
                            "error": str(e),
                            "maker": False,
                        }
                    _post_only_rejected = True
                    logger.info(f"Post-only rejected (crosses book), retrying as taker: {e}")
                    last_post_err = e
                    continue  # immediate retry, no sleep

                # FOK rejection is definitive; wallet-copy records it as
                # unfilled instead of turning it into a new strategy decision.
                is_fok_reject = "fully filled" in err_str or "fok order" in err_str
                if is_fok_reject:
                    raise

                # 5xx server error — retry once
                # Match "status_code=5XX" to avoid false positives from order IDs containing "500"/"502"
                is_server_error = ("status_code=500" in err_str or "status_code=502" in err_str
                                   or "status_code=503" in err_str or "server error" in err_str)
                if is_server_error and _attempt < 2:
                    logger.warning(f"post_order 5xx (attempt {_attempt+1}), retrying: {e}")
                    last_post_err = e
                    await asyncio.sleep(0.3)
                    continue
                raise  # 4xx or final failure — bubble up immediately

        if last_post_err is not None:
            # All attempts failed
            raise ClobServerError(f"CLOB server error after {post_attempts} attempts: {last_post_err}") from last_post_err

        t_posted = _time.monotonic()
        order_id = response.get("orderID", "")

        if not order_id:
            raise ValueError("No orderID returned from API")

        post_fill_truth = self._post_response_fill_truth(response, side, limit_price)

        # E2E timing breakdown (ms)
        exec_sign_ms = int((t_signed - t0) * 1000)
        exec_post_ms = int((t_posted - t_signed) * 1000)
        exec_total_ms = int((t_posted - t0) * 1000)

        # Track whether we got maker status (post_only succeeded)
        _was_maker = use_post_only and not _post_only_rejected

        result = {
            "status": "submitted",
            "order_id": order_id,
            "market_id": market_id,
            "side": side,
            "size_usd": size_usd,
            "entry_price": limit_price,
            "order_size": order_size,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "details": response,
            "order_type": (order_type_str or "GTC").upper(),
            **post_fill_truth,
            # E2E execution timing
            "exec_sign_ms": exec_sign_ms,
            "exec_post_ms": exec_post_ms,
            "exec_total_ms": exec_total_ms,
            "maker": _was_maker,
        }

        self._positions.append(result)
        await self._save_positions()
        logger.info(
            f"Trade submitted: order_id={order_id}, market={market_id[:16]}, "
            f"{'MAKER' if _was_maker else 'TAKER'}, "
            f"exec={exec_total_ms}ms (sign={exec_sign_ms}ms + post={exec_post_ms}ms)"
        )
        return result

    async def get_positions(self) -> List[Dict[str, Any]]:
        """
        Get current open positions.

        Returns:
            List of position dicts with keys: market_id, side, size_usd, entry_price, etc.
        """
        return self._positions.copy()

    async def get_live_balance(self) -> float:
        """
        Fetch current USDC balance from Polymarket CLOB API.
        Returns balance in USD, or -1.0 on failure.
        """
        if self.mock_mode or not self.client:
            return -1.0
        try:
            loop = asyncio.get_event_loop()
            try:
                from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
            except ImportError:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            def _get_bal():
                params = BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=1 if self.config.polymarket_proxy else 0
                )
                return self.client.get_balance_allowance(params=params)

            bal = await loop.run_in_executor(None, _get_bal)
            live_balance = int(bal.get("balance", 0)) / 1e6
            async with self._balance_guard_lock:
                self._cached_live_balance_usd = live_balance
            return live_balance
        except Exception as e:
            logger.debug(f"Balance fetch failed: {e}")
            return -1.0

    async def _reserve_balance_for_order(
        self,
        *,
        decision: Dict[str, Any],
        market_id: str,
        outcome_side: str,
        effective_order_usd: float,
        limit_price: float,
        order_type: str,
    ) -> Optional[float]:
        """Reserve cached CLOB balance for wallet-copy inventory orders.

        This is intentionally cache-based so order execution stays fast. The
        adapter refreshes live balance each cycle, and this local decrement
        prevents same-cycle wallet-copy inventory bursts from overspending it.
        """
        if str(decision.get("adaptive_mode") or "") != "wallet_copy_inventory":
            return 0.0

        min_available = float(
            getattr(self.config, "wallet_copy_inventory_min_available_balance_usd", 0.0)
            or 0.0
        )
        if min_available <= 0:
            return 0.0

        async with self._balance_guard_lock:
            cached = self._cached_live_balance_usd
            if cached is None or cached < 0:
                return 0.0
            projected = cached - float(effective_order_usd)
            if projected < min_available:
                logger.warning(
                    "Balance guard skipped wallet-copy inventory order: "
                    "market=%s outcome=%s order_type=%s price=%.3f "
                    "effective_order=$%.2f cached_balance=$%.2f reserve=$%.2f",
                    market_id,
                    outcome_side,
                    order_type,
                    limit_price,
                    effective_order_usd,
                    cached,
                    min_available,
                )
                return None
            self._cached_live_balance_usd = projected
            return float(effective_order_usd)

    async def _release_balance_reservation(self, reserved_usd: Optional[float]) -> None:
        if not reserved_usd:
            return
        async with self._balance_guard_lock:
            if self._cached_live_balance_usd is not None:
                self._cached_live_balance_usd += float(reserved_usd)

    async def get_active_order_count(self) -> int:
        """
        Fetch count of currently active (live/unmatched) orders from CLOB.
        Used to sync open_positions counter.
        """
        if self.mock_mode or not self.client:
            return 0
        try:
            loop = asyncio.get_event_loop()
            def _get_orders():
                if hasattr(self.client, "get_orders"):
                    try:
                        from py_clob_client_v2.clob_types import OpenOrderParams
                    except ImportError:
                        from py_clob_client.clob_types import OpenOrderParams
                    return self.client.get_orders(OpenOrderParams())
                if hasattr(self.client, "get_open_orders"):
                    return self.client.get_open_orders()
                return []

            orders = await loop.run_in_executor(None, _get_orders)
            return len(orders) if orders else 0
        except Exception as e:
            logger.debug(f"Active orders fetch failed: {e}")
            return 0

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """
        Fetch status of a specific order from CLOB and normalize into a structured dict.

        Returns:
            Dict with:
                final_status: one of "filled" | "partial" | "live" | "canceled" | "unfilled" | "unknown" | "mock" | "error"
                size_matched: shares actually filled (float, 0 if none)
                size_original: shares originally requested (float, 0 if unknown)
                price: average fill price (float, 0 if no fill)
                raw: the raw CLOB response dict for debugging (or None in mock/error)

        Semantics:
            - filled: entire order matched
            - partial: some filled, remainder killed/canceled (common with FAK/FOK partial)
            - live: still open in book, may fill later
            - canceled: explicitly canceled by user or system, no fill
            - unfilled: order no longer in book, nothing matched (e.g. FAK killed)
            - unknown: API responded but shape unexpected
            - mock: running without live client, not queryable
            - error: network or auth failure

        Does NOT raise — all failures return status="error" with empty fields.
        """
        if self.mock_mode or not self.client:
            return {
                "final_status": "mock",
                "size_matched": 0.0,
                "size_original": 0.0,
                "price": 0.0,
                "raw": None,
            }
        if not order_id:
            return {
                "final_status": "error",
                "size_matched": 0.0,
                "size_original": 0.0,
                "price": 0.0,
                "raw": None,
            }
        try:
            loop = asyncio.get_event_loop()

            def _get_order() -> Any:
                if not self.client:
                    raise RuntimeError("ClobClient not initialized")
                return self.client.get_order(order_id)

            raw = await loop.run_in_executor(None, _get_order)

            # CLOB response shape is documented as: {id, status, size_matched, original_size, price, ...}
            # Status vocabulary observed: LIVE, FILLED, MATCHED, CANCELED, UNFILLED
            status_raw = str(raw.get("status", "") if isinstance(raw, dict) else "").upper()
            try:
                size_matched = float(raw.get("size_matched", 0) or 0)
            except (TypeError, ValueError):
                size_matched = 0.0
            try:
                size_original = float(raw.get("original_size", raw.get("size", 0)) or 0)
            except (TypeError, ValueError):
                size_original = 0.0
            try:
                price = float(raw.get("price", 0) or 0)
            except (TypeError, ValueError):
                price = 0.0

            # Map CLOB status → normalized final_status.
            # "MATCHED" in CLOB means the order fully matched (equivalent to filled for our purposes).
            if status_raw in ("FILLED", "MATCHED"):
                final_status = "filled"
            elif status_raw == "LIVE":
                # Distinguish resting-unfilled vs partially-filled-still-live
                final_status = "partial" if size_matched > 0 else "live"
            elif status_raw.startswith("CANCELED") or status_raw.startswith("CANCELLED"):
                final_status = "partial" if size_matched > 0 else "canceled"
            elif status_raw == "UNFILLED":
                final_status = "unfilled"
            else:
                # Unknown CLOB status — log it for future mapping, infer from data
                logger.warning(
                    f"Unknown CLOB status '{status_raw}' for order {order_id[:16]} "
                    f"(size_matched={size_matched}, price={price})"
                )
                if size_matched > 0:
                    final_status = "filled"  # has fill data → treat as filled
                else:
                    final_status = "unknown"

            return {
                "final_status": final_status,
                "size_matched": size_matched,
                "size_original": size_original,
                "price": price,
                "raw": raw if isinstance(raw, dict) else None,
            }
        except Exception as e:
            logger.debug(f"get_order_status({order_id[:16]}) failed: {e}")
            return {
                "final_status": "error",
                "size_matched": 0.0,
                "size_original": 0.0,
                "price": 0.0,
                "raw": None,
            }

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order on CLOB. Returns True if canceled or already gone."""
        if self.mock_mode or not self.client or not order_id:
            return True
        try:
            loop = asyncio.get_event_loop()
            def _cancel():
                if hasattr(self.client, "cancel"):
                    return self.client.cancel(order_id=order_id)
                if hasattr(self.client, "cancel_orders"):
                    return self.client.cancel_orders([order_id])
                if hasattr(self.client, "cancel_order"):
                    try:
                        return self.client.cancel_order(order_id=order_id)
                    except TypeError:
                        from py_clob_client_v2.clob_types import OrderPayload
                        return self.client.cancel_order(OrderPayload(orderID=order_id))
                raise AttributeError("CLOB client has no supported cancel method")
            await loop.run_in_executor(None, _cancel)
            logger.info(f"Order {order_id[:16]} canceled")
            return True
        except Exception as e:
            # May already be filled/expired — not a real error
            logger.debug(f"cancel_order({order_id[:16]}) failed (may be filled): {e}")
            return False

    @retry_async(max_attempts=2, delay=0.5)
    async def close_position(self, market_id: str) -> Dict[str, Any]:
        """
        Close a position by selling tokens back to the market.

        Args:
            market_id: Market ID of position to close

        Returns:
            Dict with status and result details
        """
        # Find open position
        position = next((p for p in self._positions if p["market_id"] == market_id), None)
        if not position:
            logger.warning(f"No open position found for {market_id}")
            return {"status": "error", "error": f"No position found for {market_id}"}

        # Determine opposite side for closing
        close_side = "SELL" if position["side"].upper() == "BUY" else "BUY"

        logger.info(f"Closing position: market={market_id[:16]}, original_side={position['side']}")

        if self.mock_mode:
            order_id = f"close_mock_{market_id[:8]}_{datetime.now(timezone.utc).timestamp():.0f}"
            result = {
                "status": "mock",
                "order_id": order_id,
                "market_id": market_id,
                "side": close_side,
                "position_closed": True,
            }
            self._positions = [p for p in self._positions if p["market_id"] != market_id]
            await self._save_positions()
            return result

        try:
            if not self.client:
                raise RuntimeError("ClobClient not initialized")

            loop = asyncio.get_event_loop()

            # Create closing order
            def _create_close_order() -> Any:
                if not self.client:
                    raise RuntimeError("ClobClient not initialized")
                side_str = SELL if close_side == "SELL" else BUY
                order_args = OrderArgs(
                    price=position.get("entry_price", 0.5),
                    size=position.get("order_size", position["size_usd"] / 0.5),
                    side=side_str,
                    token_id=market_id,
                )
                return self.client.create_order(order_args)

            close_order = await loop.run_in_executor(None, _create_close_order)

            def _post_close_order() -> Dict[str, Any]:
                if not self.client:
                    raise RuntimeError("ClobClient not initialized")
                return self.client.post_order(close_order, OrderType.GTC)

            response = await loop.run_in_executor(None, _post_close_order)
            close_order_id = response.get("orderID", "")

            if close_order_id:
                self._positions = [p for p in self._positions if p["market_id"] != market_id]
                await self._save_positions()
                logger.info(f"Position closed: order_id={close_order_id}, market={market_id[:16]}")
                return {
                    "status": "submitted",
                    "order_id": close_order_id,
                    "market_id": market_id,
                    "side": close_side,
                    "position_closed": True,
                }
            else:
                raise ValueError("No orderID returned for close order")

        except Exception as e:
            logger.error(f"Failed to close position {market_id}: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}

    @retry_async(max_attempts=2, delay=0.5)
    async def wallet_approve(self) -> bool:
        """
        Perform one-time CTF (Conditional Tokens Framework) contract approval.

        Must be called before first trade in real mode.
        Returns immediately (True) in mock mode.

        Returns:
            True if approval successful or in mock mode, False otherwise
        """
        if self.mock_mode:
            logger.info("Mock mode: wallet_approve returns True")
            return True

        if not self.client:
            logger.error("ClobClient not initialized for approval")
            return False

        try:
            loop = asyncio.get_event_loop()

            def _approve() -> bool:
                if not self.client:
                    return False
                # The client's initialization already handles approvals
                # This method is for explicit re-approval if needed
                logger.info("Executing wallet approval")
                return True

            result = await loop.run_in_executor(None, _approve)
            logger.info("Wallet approval completed")
            return result

        except Exception as e:
            logger.error(f"Wallet approval failed: {e}", exc_info=True)
            return False

    async def _save_positions(self) -> None:
        """Persist positions to JSON file (async)."""
        positions_path = Path(self.config.positions_path)
        try:
            positions_path.parent.mkdir(parents=True, exist_ok=True)
            loop = asyncio.get_event_loop()

            def _write_json() -> None:
                tmp_path = positions_path.with_name(
                    f".{positions_path.name}.{os.getpid()}.{_time.time_ns()}.tmp"
                )
                with open(tmp_path, "w") as f:
                    json.dump(self._positions, f, indent=2)
                    f.write("\n")
                tmp_path.replace(positions_path)

            async with self._positions_save_lock:
                await loop.run_in_executor(None, _write_json)
            logger.debug(f"Saved {len(self._positions)} positions to {positions_path}")
        except Exception as e:
            logger.error(f"Error saving positions to {positions_path}: {e}")

    # ═══════════════════════════════════════════════════════════════════════
    # Heartbeat — dead man's switch for open orders
    # ═══════════════════════════════════════════════════════════════════════

    async def start_heartbeat(self) -> None:
        """
        Start background heartbeat loop.

        Sends POST /v1/heartbeats every 5 seconds.  If the bot crashes or this
        loop stops, the CLOB will auto-cancel ALL open orders within ~10-15
        seconds (10s timeout + 5s buffer).  This prevents orphaned GTC orders
        sitting on book after unexpected shutdown.

        Safe to call multiple times — only one loop will run.
        """
        if self.mock_mode or not self.client:
            return
        if getattr(self, "_heartbeat_task", None) and not self._heartbeat_task.done():
            return  # already running
        self._heartbeat_id = ""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Heartbeat started (5s interval, 10s dead man's switch)")

    async def stop_heartbeat(self) -> None:
        """Stop heartbeat loop gracefully."""
        task = getattr(self, "_heartbeat_task", None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            logger.info("Heartbeat stopped")

    async def _heartbeat_loop(self) -> None:
        """Internal: send heartbeats every 5 seconds."""
        loop = asyncio.get_event_loop()
        consecutive_failures = 0
        while True:
            try:
                hb_id = self._heartbeat_id

                def _send_hb():
                    return self.client.post_heartbeat(hb_id)

                resp = await loop.run_in_executor(None, _send_hb)

                # Update heartbeat_id for next request
                if isinstance(resp, dict):
                    new_id = resp.get("heartbeat_id", "")
                    if new_id:
                        self._heartbeat_id = new_id
                consecutive_failures = 0

            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                err_str = str(e).lower()
                # 400 = invalid/expired heartbeat_id — server provides correct one
                if "400" in err_str or "bad request" in err_str:
                    # Try to extract correct heartbeat_id from error response
                    logger.debug(f"Heartbeat 400 (stale id), resetting: {e}")
                    self._heartbeat_id = ""  # reset, server will give us a fresh one
                else:
                    logger.warning(f"Heartbeat failed ({consecutive_failures}x): {e}")
                if consecutive_failures > 10:
                    logger.error("Heartbeat: 10 consecutive failures, stopping")
                    return

            await asyncio.sleep(5)

    async def shutdown(self) -> None:
        """Graceful shutdown - stop heartbeat, save positions, close client."""
        await self.stop_heartbeat()
        try:
            await self._save_positions()
            logger.info("TradeExecutor positions saved")
        except Exception as e:
            logger.error(f"Error during TradeExecutor shutdown: {e}")
