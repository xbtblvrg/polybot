# Maker-First BTC-5m Experiment Design

Status: TRACK2/OBSERVE/PROMOTE paper design, not armed live.
Updated: 2026-07-05

## Decision Target

BTC-5m taker-copy is losing because the copied edge decays faster than our
submit path. The maker-first experiment tests the opposite execution role:
quote where fast takers cross into us, measure fills and PnL in paper, then
promote only if the paper lane clears the normal 50-resolved-fill live gate.

## Invariants

- Paper-only until Fable promotion: `live_orders_allowed=false`.
- No second live submitter. Promotion, if earned, routes through
  `scripts/run_wallet_copy_live_guard.py`.
- CopyIntent parity is preserved: every hypothetical maker quote is derived
  from the same observed wallet/consensus signal that would create a paper
  intent.
- Existing live maker primitives are reused; do not create a separate live
  order path. The relevant code already supports post-only GTC maker orders
  and attribution in `src/wallet_copy/execution.py`, `src/trade_executor.py`,
  and `scripts/run_wallet_copy_live_execution.py`.

## Signal Inputs

Use three paper signal streams, ranked by freshness and observed edge:

1. Whale-consensus BTC-5m direction when its score exceeds the active paper
   threshold.
2. Active-set replacement candidates when they generate fresh BTC-5m BUY flow
   but taker-copy rejects on spread movement.
3. Full-pool BTC-5m shortlist wallets whose source BUY lands inside the
   Fable band but is not immediately taker-copyable.

## Paper Quote Rule

For each signal on a live BTC-5m market:

- Side: quote the signal side only; no opposing inventory hedge inside this
  first experiment.
- Price: post one tick inside or at the current best bid for YES when the
  signal is BUY YES; symmetric for NO using the corresponding token.
- Ceiling: never quote above the source wallet VWAP plus one tick, and never
  above `max_price=0.50` for this experiment.
- Size: start at `$1` min notional, cap at `$8`, with a one-open-maker-order
  per market/side limit.
- Cancel: cancel-at-paper-window-end, and cancel earlier if signal flips,
  market reaches final 30 seconds, or quote is more than two ticks stale.
- Fill model: paper-fill only when a later observed RTDS/Polygon trade crosses
  our resting price after our quote timestamp. Same-timestamp or earlier rows
  do not count.

## Evidence Artifacts

Target files:

- `data/research/maker_first_btc5m_paper_state.json`
- `data/research/maker_first_btc5m_paper_events.jsonl`
- `data/research/maker_first_btc5m_resolution_state.json`

Required fields:

- signal source, market slug, token id, side, quote price, quote timestamp
- top-of-book at quote time and route report
- later crossing fill evidence: trade timestamp, price, size, transaction hash
- cancel reason if unfilled
- realized PnL after resolution
- maker-vs-taker counterfactual: what the equivalent FAK copy would have done

## Promotion Gate

Paper gate to small live allocation:

- `>=50` resolved paper maker fills
- cumulative realized paper PnL `>0`
- maker fill rate `>=20%` of posted quotes
- no unresolved inventory older than one market window
- no CopyIntent parity violations

If the gate passes, Fable decides the promotion packet. The live mutation is
then one ordered step: mission update, guard restart, regenerated guard-state
verification. Until then this lane is only an evidence producer.

## Immediate Build Step

Build `scripts/run_maker_first_btc5m_paper_lane.py` as a paper-only runner that
reads whale-consensus and BTC-5m replacement evidence, emits the artifacts
above, and exits nonzero only on structural IO errors. It should be launchd
supervised after one bounded smoke pass, using the same pattern as the slow
market paper qualification runner.
