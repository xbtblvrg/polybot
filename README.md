# Polymarket Wallet-Copy System

This repo is being reset around one primary path: copy profitable external
Polymarket wallets with maximum observability, minimum latency, and paper/live
parity.

The non-deviation framework is documented in
`docs/WALLET_COPY_OPERATING_FRAMEWORK.md` and mirrored in the machine-readable
mission contract at `src/wallet_copy/mission.py`. Use that framework as the
active workflow contract before changing strategy direction, heartbeat behavior,
or live-readiness gates.

Old direct-signal, autonomous ML, fair-value, SOL, and replacement-runner paths
are legacy research only unless fresh source-of-truth evidence proves otherwise.
The active build direction is:

1. ingest known profitable external wallets' BTC 5-minute history,
2. normalize every wallet action into a replayable event stream,
3. generate deterministic copy intents,
4. run those intents through a complete paper order lifecycle,
5. compare multiple wallets and train/reverse-engineer them offline,
6. only then allow live execution by sending the exact same intents to the live
   adapter behind explicit operator gates.

Live execution is never a separate strategy. It is paper mode with permission to
submit the same copy intent.

## Current Canonical Stack

- `src/wallet_copy/models.py` - wallet event, copy intent, paper order, lifecycle contracts.
- `src/wallet_copy/registry.py` - operator-provided wallet registry for one or many target wallets.
- `src/wallet_copy/ingest.py` - generic Polymarket wallet-history ingestion.
- `src/wallet_copy/strategy.py` - exact-copy sizing and filtering policies.
- `src/wallet_copy/paper.py` - paper execution and order lifecycle ledger.
- `src/wallet_copy/fill_model.py` - executable paper fill model shared by replay and live-tracker evidence.
- `src/wallet_copy/consensus.py` - multi-wallet agreement and inventory signals.
- `src/wallet_copy/inventory.py` - same-market/same-outcome multi-wallet inventory plans.
- `src/wallet_copy/features.py` - wallet/window feature rows for research and ML.
- `src/wallet_copy/performance.py` - resolution-backed PnL scorecards and live admission blockers.
- `src/wallet_copy/live_tracker.py` - multi-source live wallet tracking evidence and paper replay.
- `src/wallet_copy/research.py` - cross-wallet windows and train rows.
- `src/wallet_copy/execution.py` - shared paper/live adapter boundary.
- `src/wallet_copy/weird_peak.py` - bridge to the existing Weird-Peak exact-copy paper flow.

External bot and paste-note research has been distilled into
`docs/EXTERNAL_WALLET_COPY_RESEARCH.md` and the machine-readable registry at
`configs/wallet_copy/external_bot_sources.json`. The practical additions are
copy-efficiency scoring, BTC-5m wallet discovery scoring, OBI/CVD/depth/spread
context around copied wallet moves, depth/circuit guards, order-lifecycle
reconciliation, and paper-only sweeper research until executable fill modeling
proves it.

## Paper-First Commands

Run the existing low-latency Weird-Peak exact paper flow:

```bash
python3 scripts/run_wallet_copy_pipeline.py --weird-peak-exact-flow
```

Register the first seed wallet:

```bash
python3 scripts/register_wallet.py \
  --wallet 0x9f5ffe76a818dce37c70f947998b52b70671a008 \
  --name weird_peak \
  --tag seed \
  --tag weird_peak \
  --notes "Initial wallet-copy-only seed wallet"
```

Onboard any operator-provided profitable-bot candidate into the full
paper/research/ML/admission workflow:

```bash
python3 scripts/onboard_operator_wallets.py \
  --wallet baloneigh=0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b \
  --tag externally_claimed_profitable \
  --notes "Operator-provided profitable BTC-5m bot candidate"
```

This registers the wallet, refreshes BTC-5m history, replays paper exact-copy,
builds cross-wallet research/consensus/inventory analysis, exports ML labels,
runs the profit engine, and polls the paper live-tracker with CLOB-backed
copy-efficiency evidence. It never enables live execution.

Run generic wallet-history exact-copy paper ingestion:

```bash
python3 scripts/run_wallet_copy_pipeline.py \
  --wallets-config configs/wallet_copy/wallets.json \
  --wallet-fraction 1.0 \
  --pages 0 \
  --limit 500
```

`--pages 0` paginates until the data API returns an empty page. Use a smaller
positive page count for quick local smoke checks.

Analyze saved wallet histories and build multi-wallet consensus candidates:

```bash
python3 scripts/analyze_wallet_copy_research.py \
  --history-state data/research/wallet_copy_history_state.json \
  --paper-state data/research/wallet_copy_paper_state.json \
  --inventory-paper-state data/research/wallet_copy_inventory_paper_state.json
```

Report the reviewed external bot sources and the concrete wallet-copy import
actions:

```bash
python3 scripts/report_external_bot_sources.py
```

Search for profitable single-wallet, consensus, and inventory candidates with
walk-forward admission. The default is intentionally strict: unresolved ratio is
capped at `0.5`, fallback slippage is `250bps`, filtered candidates are checked
against the same-wallet raw/full baseline, and live admission stays in
`ANALYZE`/`CORRECTION` until persisted live-tracker paper truth is present.
Additionally, profitable fallback-only historical slices are blocked from
candidate PASS unless `--allow-candidate-fallback-fill-evidence` is explicitly
used for research-only optimistic replay.

```bash
python3 scripts/run_wallet_copy_profit_engine.py \
  --history-state data/research/wallet_copy_history_state.json \
  --resolutions data/research/btc_resolutions_from_btcusdt_ticks.jsonl \
  --output data/research/wallet_copy_profit_engine_state.json \
  --max-unresolved-ratio 0.5 \
  --slippage-bps 250
```

Export feature rows and paper labels for ML/reverse-engineering:

```bash
python3 scripts/export_wallet_copy_dataset.py \
  --history-state data/research/wallet_copy_history_state.json \
  --paper-state data/research/wallet_copy_paper_state.json \
  --include-unresolved
```

Track registered wallets live, enrich with CLOB/onchain evidence, and replay to
paper:

```bash
python3 scripts/run_wallet_live_tracker.py \
  --registry configs/wallet_copy/wallets.json \
  --profit-policy-state data/research/wallet_copy_profit_engine_state.json \
  --limit 50 \
  --enable-clob-books \
  --enable-onchain-receipts
```

This runs with strict mirror coverage by default. With a passing profit-policy
state, BUYs outside the admitted policy are written as
`FILTERED_BY_PROFIT_POLICY`; accepted BUYs and lifecycle events must become a
paper `CopyIntent` or paper lifecycle event. Any unhandled accepted move is
written as `COVERAGE_VIOLATION` and the command exits non-zero. Accepted BUYs
also receive event-level `copy_efficiency` rows; CLOB-book-backed fills can pass
admission, while fallback-only fills remain paper/research evidence and block
live admission.

Run the autonomous paper-only audit/repair loop used by heartbeats:

```bash
python3 scripts/run_wallet_copy_autonomous_repair.py
```

This performs a pre-audit, refreshes leaderboard/history/research/profit/tracker
states only when the audit says they are stale or non-green, writes
`data/research/wallet_copy_autonomous_repair_state.json`, and keeps unresolved
blockers in `data/research/wallet_copy_autonomous_backlog.json`. Tracker
copy-efficiency failures remain blockers, not process-green success.

Capture public CLOB market WebSocket rows for later preconfirm correlation:

```bash
python3 scripts/run_clob_market_ws_capture.py \
  --asset-id <clob-token-id> \
  --duration-s 60 \
  --output data/research/clob_market_ws_events.jsonl
```

The research output includes wallet scorecards, feature payloads,
resolution-backed paper PnL, inventory plans, and an admission report. A blocked
admission is expected until enough resolved, profitable paper evidence exists.

## Profitability Admission Contract

A candidate is not treated as a profitable copy bot just because a filtered
slice wins. The workflow keeps these truths separate:

- full/raw wallet-copy baseline remains visible even when it loses,
- source-vs-our copy efficiency is measured before admission,
- filtered candidates must beat the raw baseline by a meaningful delta when the
  raw baseline is negative,
- a negative raw baseline forces a larger filtered resolved sample before PASS,
- fills are scored through `executable_copy_fill_v1`, not optimistic source
  fills,
- fallback source-price fills are not live-admission truth; accepted BUYs need
  CLOB-backed fillability, latency, slippage, fill-ratio, and missed-reason
  evidence,
- SELL/MERGE/REDEEM lifecycle events produce realized PnL accounting,
- multi-wallet consensus/inventory excludes same-wallet hedged/exit posture from
  clean agreement,
- live admission remains separate from paper candidate PASS and requires
  persisted `wallet_copy_live_tracking_state.json` mirror and copy-efficiency
  truth scoped to the current best candidate policy.

## Safety Rules

- BTC 5-minute markets are the only active domain for now.
- SOL and prior direct/ML/fair-value strategies are not active development
  paths.
- Paper runners are always `paper_only=true`, `can_trade=false`,
  `live_orders_allowed=false`.
- Live execution requires explicit operator go, live permission, dry-run off,
  and CLOB token mapping for the same `CopyIntent`.
- SOL live remains disabled in the reset state.
- A low wallet balance may be an intentional capital stop; it is not refill
  advice.
- Full/raw losing copy evidence must stay visible and must not be renamed as
  live-ready.

## Legacy Boundary

See `docs/WALLET_COPY_ONLY_RESET.md`. The old systems can remain importable for
tests and forensic analysis, but they are not the active profit path.

For the forward architecture, see `docs/WALLET_COPY_TRADING_BLUEPRINT.md`.
For live tracking evidence layers, see `docs/LIVE_WALLET_TRACKING.md`.
