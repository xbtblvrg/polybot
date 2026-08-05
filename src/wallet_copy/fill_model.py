"""Executable paper fill model for wallet-copy intents.

The model keeps the 1:1 wallet-copy contract, but it does not pretend every
source wallet trade is instantly reproducible at the same price. When CLOB book
evidence is attached to an intent, that evidence wins. Otherwise the model uses
a conservative source-price-plus-slippage fallback so offline replay remains
deterministic.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from src.wallet_copy.models import CopyIntent, num


@dataclass(frozen=True)
class FillModelConfig:
    model_id: str = "executable_copy_fill_v1"
    fallback_slippage_bps: float = 250.0
    min_fill_ratio: float = 0.999
    max_api_latency_s: float = 0.0
    max_copy_price: float = 0.99
    allow_fallback_without_book: bool = True

    def asdict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "fallback_slippage_bps": self.fallback_slippage_bps,
            "min_fill_ratio": self.min_fill_ratio,
            "max_api_latency_s": self.max_api_latency_s,
            "max_copy_price": self.max_copy_price,
            "allow_fallback_without_book": self.allow_fallback_without_book,
        }


def _fill_id(*parts: Any, length: int = 24) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"fm_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:length]}"


def _clob_book_summary(intent: CopyIntent) -> dict[str, Any]:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    evidence = metadata.get("live_tracking_evidence")
    if not isinstance(evidence, dict):
        return {}
    book = evidence.get("clob_book")
    return book if isinstance(book, dict) else {}


def _reject_reason(blockers: list[str]) -> str | None:
    if not blockers:
        return None
    priority = [
        "zero_requested_size",
        "missing_clob_book_evidence",
        "api_latency_above_fill_model_cap",
        "clob_no_ask_liquidity",
        "clob_price_above_slippage_cap",
        "clob_insufficient_depth_within_slippage_cap",
        "clob_fill_ratio_below_minimum",
        "clob_avg_price_above_max_copy_price",
    ]
    for item in priority:
        if item in blockers:
            return item
    return blockers[0]


def _book_diagnostics(book: dict[str, Any], *, source_price: float, requested_usd: float) -> dict[str, Any]:
    return {
        "book_status": book.get("status"),
        "instant_fill_status": book.get("instant_fill_status"),
        "blocking_reason": book.get("blocking_reason"),
        "token_id": book.get("token_id") or book.get("asset_id"),
        "book_market": book.get("book_market"),
        "best_bid": book.get("best_bid"),
        "best_ask": book.get("best_ask"),
        "spread": book.get("spread"),
        "source_price": round(float(source_price), 6),
        "max_copy_price": book.get("max_copy_price"),
        "requested_size_usd": round(float(requested_usd), 6),
        "copy_size_usd": book.get("copy_size_usd"),
        "fillable_usd": book.get("fillable_usd"),
        "fillable_shares": book.get("fillable_shares"),
        "remaining_usd": book.get("remaining_usd"),
        "avg_fill_price": book.get("avg_fill_price"),
        "fill_ratio": book.get("fill_ratio"),
        "levels_used": book.get("levels_used"),
        "book_hash": book.get("book_hash"),
        "book_timestamp": book.get("book_timestamp"),
        "error": book.get("error"),
    }


def _missing_book_blocking_reason(book_status: str) -> str:
    status = str(book_status or "").strip().upper()
    if status == "BOOK_NOT_FOUND_OR_CLOSED":
        return "book_not_found_or_closed"
    if status == "FETCH_BUDGET_EXHAUSTED":
        return "book_fetch_budget_exhausted"
    if status == "MISSING_TOKEN":
        return "missing_token"
    if status == "ERROR":
        return "book_fetch_error"
    return "missing_clob_book_evidence"


def estimate_executable_fill(intent: CopyIntent, config: FillModelConfig | None = None) -> dict[str, Any]:
    """Return a deterministic executable fill estimate for a copy intent."""

    cfg = config or FillModelConfig()
    requested_usd = max(0.0, float(intent.copy_size_usd))
    source_price = max(0.000001, min(float(cfg.max_copy_price), float(intent.limit_price)))
    api_latency = intent.api_latency_s
    blockers: list[str] = []
    if requested_usd <= 0:
        blockers.append("zero_requested_size")
    if api_latency is not None and float(cfg.max_api_latency_s) > 0 and num(api_latency) > float(cfg.max_api_latency_s):
        blockers.append("api_latency_above_fill_model_cap")

    book = _clob_book_summary(intent)
    book_status = str(book.get("status") or "")
    if book and book_status == "OK":
        fill_ratio = num(book.get("fill_ratio"))
        filled_size = min(requested_usd, max(0.0, num(book.get("fillable_usd"))))
        avg_price = num(book.get("avg_fill_price"), source_price)
        blocking_reason = str(book.get("blocking_reason") or "")
        if fill_ratio < float(cfg.min_fill_ratio):
            blockers.append("clob_fill_ratio_below_minimum")
            if blocking_reason and blocking_reason != "none":
                blockers.append(f"clob_{blocking_reason}")
        if avg_price > float(cfg.max_copy_price):
            blockers.append("clob_avg_price_above_max_copy_price")
        status = "FILLED" if not blockers else "REJECTED"
        if status == "REJECTED":
            filled_size = 0.0
        filled_shares = round(filled_size / avg_price, 6) if avg_price > 0 and status == "FILLED" else 0.0
        reject_reason = _reject_reason(blockers)
        return {
            "fill_id": _fill_id(intent.intent_id, cfg.model_id, "book", book.get("book_hash") or ""),
            "fill_model": cfg.model_id,
            "source": "clob_book_evidence",
            "status": status,
            "blockers": blockers,
            "reject_reason": reject_reason,
            "reject_stage": "clob_book_fillability" if reject_reason else None,
            "reject_details": _book_diagnostics(book, source_price=source_price, requested_usd=requested_usd),
            "requested_size_usd": round(requested_usd, 6),
            "filled_size_usd": round(filled_size, 6),
            "filled_shares": filled_shares,
            "effective_price": round(avg_price, 6),
            "fill_ratio": round(fill_ratio, 6),
            "config": cfg.asdict(),
            "book": {
                "best_bid": book.get("best_bid"),
                "best_ask": book.get("best_ask"),
                "spread": book.get("spread"),
                "source_price": book.get("source_price"),
                "max_copy_price": book.get("max_copy_price"),
                "fillable_usd": book.get("fillable_usd"),
                "fillable_shares": book.get("fillable_shares"),
                "remaining_usd": book.get("remaining_usd"),
                "blocking_reason": book.get("blocking_reason"),
                "instant_fill_status": book.get("instant_fill_status"),
                "levels_used": book.get("levels_used"),
                "book_hash": book.get("book_hash"),
                "book_timestamp": book.get("book_timestamp"),
            },
        }

    if not cfg.allow_fallback_without_book:
        blockers.append("missing_clob_book_evidence")
        reject_reason = _reject_reason(blockers)
        blocking_reason = _missing_book_blocking_reason(book_status)
        return {
            "fill_id": _fill_id(intent.intent_id, cfg.model_id, "missing_book"),
            "fill_model": cfg.model_id,
            "source": "no_book_rejected",
            "status": "REJECTED",
            "blockers": blockers,
            "blocking_reason": blocking_reason,
            "reject_reason": reject_reason,
            "reject_stage": "missing_execution_evidence",
            "reject_details": {
                "book_status": book_status or "MISSING",
                "blocking_reason": blocking_reason,
                "token_id": intent.token_id,
                "source_price": round(source_price, 6),
                "requested_size_usd": round(requested_usd, 6),
            },
            "requested_size_usd": round(requested_usd, 6),
            "filled_size_usd": 0.0,
            "filled_shares": 0.0,
            "effective_price": 0.0,
            "fill_ratio": 0.0,
            "config": cfg.asdict(),
        }

    effective_price = source_price * (1.0 + max(0.0, float(cfg.fallback_slippage_bps)) / 10_000.0)
    effective_price = max(0.000001, min(float(cfg.max_copy_price), effective_price))
    status = "FILLED" if not blockers else "REJECTED"
    filled_size = requested_usd if status == "FILLED" else 0.0
    reject_reason = _reject_reason(blockers)
    return {
        "fill_id": _fill_id(intent.intent_id, cfg.model_id, "fallback", cfg.fallback_slippage_bps),
        "fill_model": cfg.model_id,
        "source": "source_price_plus_slippage_fallback",
        "status": status,
        "blockers": blockers,
        "reject_reason": reject_reason,
        "reject_stage": "fallback_source_price" if reject_reason else None,
        "reject_details": {
            "book_status": book_status or "MISSING",
            "token_id": intent.token_id,
            "source_price": round(source_price, 6),
            "effective_price": round(effective_price, 6),
            "requested_size_usd": round(requested_usd, 6),
            "fallback_slippage_bps": round(float(cfg.fallback_slippage_bps), 6),
            "latency_basis": "legacy_api_latency_s_from_wallet_event_age",
            "api_latency_s": api_latency,
        },
        "requested_size_usd": round(requested_usd, 6),
        "filled_size_usd": round(filled_size, 6),
        "filled_shares": round(filled_size / effective_price, 6) if effective_price > 0 and status == "FILLED" else 0.0,
        "effective_price": round(effective_price, 6) if status == "FILLED" else 0.0,
        "fill_ratio": 1.0 if status == "FILLED" and requested_usd > 0 else 0.0,
        "config": cfg.asdict(),
    }
