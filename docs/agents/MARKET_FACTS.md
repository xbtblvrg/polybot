# Market Facts

Verification date: 2026-07-06 UTC.

Authority: this file records platform facts used by execution design. When a
fact affects live sizing, fees, ticks, rate limits, or order admissibility,
verify it here first and cite the source.

## Polymarket Trading Facts

- Fees are not a fixed per-order charge. Polymarket applies taker fees on
  certain markets at match time; makers are not charged fees. For crypto, the
  documented taker fee rate is `0.07`, maker fee rate is `0`, and the fee is
  proportional to `shares * feeRate * price * (1 - price)`.
  Source: https://docs.polymarket.com/trading/fees
- BTC 5-minute crypto markets launched with taker fees enabled; fees follow
  the same crypto curve as 15-minute markets. Drip implication: splitting a
  target into several orders does not add a flat per-order fee, but each taker
  fill can still incur the normal size/price-based fee.
  Source: https://docs.polymarket.com/changelog
- All Polymarket orders are expressed as limit orders. Market orders are
  marketable limit orders; FAK/FOK execute against resting liquidity
  immediately.
  Source: https://docs.polymarket.com/trading/orders/create
- Tick sizes vary by market. The docs explicitly say to fetch a market's tick
  size instead of assuming one; rejected orders can include
  `INVALID_ORDER_MIN_TICK_SIZE`.
  Source: https://docs.polymarket.com/trading/orders/overview
- CLOB order placement has explicit platform rate limits. The 2026-06-01
  changelog lists `POST /order` at `120000` every 10 minutes (`200/s`)
  sustained. Runtime code should still honor live API responses and backoff on
  429s.
  Source: https://docs.polymarket.com/changelog
- Runtime minimums in this repo: market BUY FAK/FOK uses a precision-safe
  USDC amount with `min_amount_usd=1.0`; non-market share-sized orders use a
  5-share floor unless a strategy explicitly allows a smaller sell. This is
  implementation policy in `src/trade_executor.py`, not a replacement for
  fetching market-specific exchange metadata.
  Source: src/trade_executor.py

## Current Design Consequences

- The v3 drip model is valid against fees because it avoids a fixed per-order
  tax while still accounting for taker fee, spread, price drift, fill
  probability, min size, min shares, tick size, and rate limits.
- Maker-first or post-only variants remain valuable where queue position and
  fill probability justify them, because maker fee rate is zero and maker
  rebates may apply on eligible markets.
