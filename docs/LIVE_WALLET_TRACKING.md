# Live Wallet Tracking

Date: 2026-06-24

Active scope: BTC 5-minute external-wallet copy trading only.

## Evidence Layers

1. Wallet-attributed truth: `data-api.polymarket.com/trades` and
   `data-api.polymarket.com/activity` filtered by tracked wallet.
2. Fast market context: CLOB `/book` and public market WebSocket rows by token
   ID. These are not wallet-attributed.
3. Settlement evidence: Polygon transaction receipts for the Data API
   `transactionHash`.

The bot may copy a wallet move only after the wallet-attributed truth layer
confirms it. Public CLOB market rows can become preconfirm candidates, but they
must be truth-looped back to the wallet API/onchain receipt before being called
wallet-copy evidence.

External-wallet copy quality must be scored separately from strategy ROI:
source timestamp/price/size versus our observation timestamp, intended price,
executable fill/reject, fill price, fill size, and lifecycle PnL. This is the
copy-efficiency layer; a filtered ROI candidate is not live-ready without it.

## Implemented Entry Points

- `scripts/run_wallet_live_tracker.py`: polls registered wallets, enriches new
  moves with CLOB book and optional onchain receipt evidence, writes paper
  `CopyIntent`s, and records lifecycle events.
- `scripts/run_clob_market_ws_capture.py`: captures public CLOB market
  WebSocket rows for token IDs into JSONL. Feed this file back to the live
  tracker with `--market-ws-jsonl`.

## Copy Contract

- Wallet rows are replayed in source event order. BUY intents and
  SELL/MERGE/REDEEM lifecycle rows go through the same paper ledger sequence
  that live execution would have seen; the system must not let a later BUY
  create inventory for an earlier lifecycle action.
- BUY: create a `CopyIntent` and apply it to paper.
- BUY fillability: the paper order uses `executable_copy_fill_v1`; CLOB book
  evidence can fill or reject it, and rejection is reported as
  `MIRRORED_BUY_TO_PAPER_REJECTED_BY_FILL_MODEL`.
- SELL: reduce matching paper inventory.
- MERGE: reduce paired paper inventory on the same condition.
- REDEEM: reduce matching paper inventory.
- Every normalized BTC 5-minute wallet move must end with either
  `MIRRORED_*` or `COVERAGE_VIOLATION` in `mirror_result`.
- When `--profit-policy-state` points to a passing profit-engine state, BUYs
  outside that admitted policy are explicitly marked
  `FILTERED_BY_PROFIT_POLICY`; accepted BUYs and all lifecycle rows remain under
  strict coverage.
- `--strict-mirror-coverage` is enabled by default. A coverage violation makes
  `scripts/run_wallet_live_tracker.py` exit non-zero instead of silently
  continuing.
- The tracker summary reports `mirror_required_events`, `mirrored_events`,
  `mirror_coverage_pct`, `mirror_coverage_status`, latency, fill-model
  outcomes through paper summary, and per-layer evidence status counts.
- Restarted or one-shot tracker runs may use `--seed-before-poll
  --seed-history-state <history_state.json>` to reconstruct the paper ledger
  from already-ingested wallet history before scoring the current poll batch.
  This is a continuity measurement, not live-admission truth. The seed loads
  only events outside the current poll batch, keeps `paper_only=true` and
  `live_orders_allowed=false`, and records `summary.seed` with counts for
  seeded BUYs, lifecycle rows, skipped current-batch rows, and new paper
  ledger entries. It can make SELL/MERGE/REDEEM coverage more realistic after
  a restart, but it cannot satisfy CLOB-backed fresh BUY copyability.
- For leaderboard-sized registries, do not poll every enabled wallet with CLOB
  books on every heartbeat. Use profit-search scope and caps:

  ```bash
  python3 scripts/run_wallet_live_tracker.py --registry configs/wallet_copy/wallets.json --profit-policy-state data/research/wallet_copy_profit_engine_state.json --limit 5 --pages 1 --iterations 1 --enable-clob-books --strict-mirror-coverage --use-profit-search-scope --max-wallets 5 --clob-timeout-s 1.5 --gamma-timeout-s 1.5
  ```

  The persisted state must report `summary.tracker_scope`; a scoped tracker run
  is not a full-registry tracker run.
- Live execution remains disabled unless the separate execution gate is
  explicitly opened by the operator.

## Live Admission Separation

The profit engine may find a paper candidate, but live admission is separate.
The candidate remains paper/research unless the persisted
`wallet_copy_live_tracking_state.json` has strict mirror coverage and a real
paper order lifecycle plus `copy_efficiency.status=PASS` for accepted moves. A
filtered PASS without CLOB-backed fillability, latency, slippage, fill-ratio,
missed-copy evidence, and event scores matching the current best candidate
policy is only a research candidate, not a live-ready bot.

For single-wallet candidates, live-tracker truth must match both policy id and
`source_wallet`. Policy-id-only evidence is insufficient because another wallet
can use the same policy shape. Multi-wallet consensus/inventory candidates must
eventually match an explicit participant/fingerprint set before admission.

Fallback source-price fills are intentionally non-admissible. They can prove
that the mirror path and paper lifecycle are wired, but they cannot prove that
we could actually copy the wallet in the live CLOB at the observed time.
Seeded paper orders have the same limitation: they are useful for lifecycle
continuity and blocker diagnosis, but live admission still requires fresh
accepted BUY events with CLOB-backed fillability and copy-efficiency PASS.

Coverage failures are retryable. The tracker keeps
`failed_source_fingerprints` and structured `failed_source_details` separate
from `seen_source_fingerprints`, so a later zero-event run cannot make an
unresolved mirror violation disappear. Failed lifecycle rows are retried with a
bounded backoff because later inventory can make them copyable; non-lifecycle
coverage failures are retained as blockers instead of being repeatedly logged as
new work every poll.

## Practical Limit

For someone else's wallet, Polymarket's authenticated user WebSocket cannot be
used unless we own that API key. Therefore the fastest safe external-wallet
copy loop is:

```text
public CLOB market event (fast, anonymous)
  -> wallet Data API row (wallet-attributed truth)
  -> paper CopyIntent / lifecycle update
  -> optional onchain receipt confirmation
  -> copy-efficiency scorecard
  -> gated live adapter only after admission
```
