"""Read-only realtime wallet-copy detection normalization.

Realtime feeds are detection evidence only. They do not create CopyIntents and
must not mutate paper or live trading state.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from .models import parse_ts, stable_id
from .polymarket_addresses import EXCHANGE_ADDRESSES


@dataclass(frozen=True)
class RealtimeTradeEvent:
    source: str
    source_wallet: str
    side: str
    asset: str
    condition_id: str
    market_slug: str
    price: float | None
    size: float | None
    event_ts: float | None
    received_at_s: float
    transaction_hash: str = ""
    maker: str = ""
    taker: str = ""
    maker_side: str = ""
    raw: dict[str, Any] | None = None

    def asdict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_id"] = stable_id(
            "rt",
            {
                "source": self.source,
                "wallet": self.source_wallet,
                "side": self.side,
                "condition_id": self.condition_id,
                "market_slug": self.market_slug,
                "transaction_hash": self.transaction_hash,
                "event_ts": self.event_ts,
            },
        )
        return payload


def _addr(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def _num_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_rtds_trade_frame(raw: str | dict[str, Any], *, received_at_s: float) -> list[RealtimeTradeEvent]:
    payload: Any
    if isinstance(raw, str):
        payload = json.loads(raw)
    else:
        payload = raw
    messages = payload if isinstance(payload, list) else [payload]
    events: list[RealtimeTradeEvent] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        topic = str(message.get("topic") or "").lower()
        msg_type = str(message.get("type") or message.get("event") or "").lower()
        body = message.get("payload") if isinstance(message.get("payload"), dict) else message
        if topic and topic != "activity":
            continue
        if msg_type and msg_type not in {"trades", "trade", "activity", "orders_matched"}:
            continue
        wallet = _addr(body.get("proxyWallet") or body.get("wallet") or body.get("maker") or body.get("taker"))
        if not wallet:
            continue
        events.append(
            RealtimeTradeEvent(
                source="rtds_activity",
                source_wallet=wallet,
                side=str(body.get("side") or body.get("action") or "").upper(),
                asset=str(body.get("asset") or body.get("tokenId") or body.get("assetId") or ""),
                condition_id=str(body.get("conditionId") or body.get("condition_id") or ""),
                market_slug=str(body.get("marketSlug") or body.get("market_slug") or body.get("slug") or body.get("eventSlug") or ""),
                price=_num_or_none(body.get("price")),
                size=_num_or_none(body.get("size") or body.get("amount")),
                event_ts=parse_ts(body.get("timestamp") or body.get("createdAt") or body.get("created_at")),
                received_at_s=received_at_s,
                transaction_hash=str(body.get("transactionHash") or body.get("transaction_hash") or ""),
                maker=_addr(body.get("maker")),
                taker=_addr(body.get("taker")),
                maker_side="",
                raw=body,
            )
        )
    return events


def normalize_polygon_orderfilled_row(row: dict[str, Any]) -> RealtimeTradeEvent | None:
    maker = _addr(row.get("maker"))
    taker = _addr(row.get("taker"))
    wallet = maker or taker
    if not wallet:
        return None
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return RealtimeTradeEvent(
        source="polygon_orderfilled",
        source_wallet=_addr(row.get("selected_wallet")) or wallet,
        side=str(decoded.get("side") or ""),
        asset=str(decoded.get("asset") or ""),
        condition_id=str(decoded.get("condition_id") or ""),
        market_slug="",
        price=_num_or_none(decoded.get("price")),
        size=_num_or_none(decoded.get("size")),
        event_ts=parse_ts(row.get("block_ts") or decoded.get("event_ts")),
        received_at_s=float(row.get("received_at_s") or 0.0),
        transaction_hash=str(row.get("transaction_hash") or ""),
        maker=maker,
        taker=taker,
        maker_side=str(decoded.get("maker_side") or "").upper(),
        raw=row,
    )


def decode_polygon_orderfilled_v2(row: dict[str, Any], *, exchange_addresses: set[str] | None = None) -> dict[str, Any]:
    """Decode the observed 2026 V2 OrderFilled layout.

    Empirically validated on 2026-07-03 against Data API transactions
    0x86f3e6657a5e38785d1c838c6071830257efffd2818662d089168fba02a0d750
    (maker BUY, price 0.5299999427, size 6.452829) and
    0x300b2a15be98ad72c061ac90b261cdad0f0babfbf527dfc68084d64d67d3d50c
    (maker SELL, price 0.48, size 22.26), plus same-direction spot checks
    0x4777b1db1e96586af97c78b022a0cb70a87953a04d1bf7cf90a4e712ed0f4318,
    0x4dc4542bec0e52c9ea2af41766426547b46584a448c1b2f71e0bfd4ce69ea747,
    0xd0a5de19653154a41ca64bbb9ede352befb3e4c8adc1a92a34da7c8cf17153e1,
    0xfb2d619b5002926df9cfe9b191f8ec683ff737225692933682427a8de02437d7,
    0x6b6d5594eda4dae7e26f98a387dcc0214645d470bc357f0847831c7e76a309e5,
    and 0xc810b9d51647357bda04f1d702eee5dcaa5a2cbb0ce784e427bb74d6cce9ead1.
    Topic1 is orderHash, topic2 is maker, topic3 is taker.
    """

    data = str(row.get("data") or "")
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 64 * 7:
        return {"decode_status": "UNSUPPORTED_DATA_LENGTH", "data_words": len(data) // 64}
    try:
        words = [int(data[index : index + 64], 16) for index in range(0, len(data), 64)]
    except ValueError:
        return {"decode_status": "INVALID_HEX_DATA"}
    if len(words) < 7:
        return {"decode_status": "UNSUPPORTED_DATA_WORD_COUNT", "data_words": len(words)}
    order_hash = str(row.get("order_hash") or row.get("topic1") or "")
    maker = _addr(row.get("maker"))
    taker = _addr(row.get("taker"))
    topic3 = _addr(row.get("topic3"))
    selected_wallet = _addr(row.get("selected_wallet"))
    exchange_set = {str(item).lower() for item in (exchange_addresses or EXCHANGE_ADDRESSES)}
    maker_side = "BUY" if words[0] == 0 else "SELL"
    selected_side = maker_side
    if selected_wallet:
        if selected_wallet == maker:
            selected_side = maker_side
        elif selected_wallet == taker:
            selected_side = "SELL" if maker_side == "BUY" else "BUY"
    if maker_side == "BUY":
        asset = words[1]
        price = (words[2] / words[3]) if words[3] else None
        size = words[3] / 1_000_000.0 if words[3] else None
    else:
        asset = words[1] or words[0]
        price = (words[3] / words[2]) if words[2] else None
        size = words[2] / 1_000_000.0 if words[2] else None
    return {
        "decode_status": "OK",
        "layout": "v2_orderfilled_7word_empirical_directional",
        "side": selected_side,
        "maker_side": maker_side,
        "asset": str(asset),
        "price": price,
        "size": size,
        "maker_amount_raw": words[2],
        "taker_amount_raw": words[3],
        "fee_raw": words[4],
        "order_hash": order_hash,
        "maker": maker,
        "taker": taker,
        "topic3": topic3,
        "maker_is_exchange": maker in exchange_set,
        "taker_is_exchange": taker in exchange_set,
    }
