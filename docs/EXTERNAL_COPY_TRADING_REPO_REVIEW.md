# External Polymarket Copy-Trading Repo Review

Date: 2026-06-26

Scope: BTC 5-minute wallet-copy only. External repositories are research input,
not live-admission evidence. Nothing from these repos is executed directly and
binary ZIP/EXE artifacts are ignored.

## Reviewed Sources

- llogiq33/Polymarket-Copy-Trade
- unitmargaretaustin/Polymarket-copy-trading-bot
- dexorynlabs/polymarket-copy-trading-bot-v2.0
- realfishsam/Polymarket-Copy-Trader
- Dadel-1/Polymarket-CopyTrading
- mahmoooud194/Polymarket-Copytrading-Bot
- kalliimmunosuppressive504/polymarket-copy-trading-bot
- sherlineconsubstantial217/Polymarket-Trading-Bot
- zioerenkl/Polymarket-Copytrading-Bot

## Safe Patterns Adopted

The useful ideas were copied as local design patterns, not as code imports:

- Data API activity polling with event-key dedupe.
- CLOB book evidence before any paper fill is treated as executable.
- Micro-trade aggregation for copied BUYs below realistic minimum order size.
- Best-ask slippage classification before attempting a more aggressive copy.
- Depth/fill-ratio diagnosis before reducing copy size or slicing orders.
- Position-delta reconciliation for SELL, MERGE, and REDEEM lifecycle gaps.
- Processed/rejected/error taxonomy so the workflow can correct instead of
  silently repeating the same failure.

Local implementation:

- `src/wallet_copy/copy_tactics.py`
- `LiveWalletTracker.poll_once().summary.all_order_exact_copy.execution_corrections`
- `LiveWalletTracker.poll_once().summary.all_order_exact_copy.current_poll_execution_corrections`
- `LiveWalletTracker.poll_once().summary.all_order_exact_copy.cumulative_execution_corrections`
- `tests/test_wallet_copy_core.py`

## Rejected Patterns

The following were not imported:

- Browser automation as a trading source of truth.
- Direct live execution paths that bypass the local `CopyIntent` lifecycle.
- Market-order-first copier logic without CLOB-backed copyability evidence.
- Installer archives, ZIP artifacts, or opaque binary payloads.
- Marketing-only repositories without auditable source code.
- MongoDB-dependent queue architecture as a required runtime dependency.

## Resulting Workflow Rule

Every all-order exact-copy paper run now produces an execution-correction
advisory. If paper copy fails, the state must say which next correction is
needed: micro-aggregate, refresh hot-lane CLOB evidence, reduce latency, slice
by depth, test measured aggressive limit pricing, or reconcile position deltas.

This keeps live readiness strict while making the development loop active:
`PASS` means no paper execution correction is needed; `ANALYZE` means a sharper
measurement is needed; `CORRECTION` means a concrete copy/fill/lifecycle fix is
required.

All-order proof is explicitly paper proof. A CLOB-filled all-order paper BUY is
not live-admission truth unless the same current-poll source BUY also has
accepted copyability/freshness evidence. Historical paper-ledger corrections are
reported separately from current-poll corrections so old rejects remain visible
without being mistaken for the current poll result.
