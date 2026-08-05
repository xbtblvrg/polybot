# Wallet-Copy Trading Blueprint

Date: 2026-06-24

## Goal

Build the bot around one primary edge: copy externally profitable Polymarket
wallets on BTC 5-minute markets with enough detail, speed, and accounting
discipline that paper and live execution are the same pipeline.

The mandatory operating framework and non-deviation contract are in
`docs/WALLET_COPY_OPERATING_FRAMEWORK.md`. If this blueprint and the framework
appear to conflict, use the framework plus `src/wallet_copy/mission.py` as the
current authority.

Code-level mission contract:

- `src/wallet_copy/mission.py` is the machine-readable source of truth for the
  project objective.
- Primary goal:
  `find_profitable_direction_by_copying_external_wallet_orders_on_btc_5m_markets`.
- Active strategy authority: `wallet_order_copying`.
- AI/ML/research may rank, explain, filter, or reverse-engineer copied wallet
  events, but may not become an independent live trading authority.
- A system state is not GREEN unless this mission contract, copyability,
  copy-efficiency, paper lifecycle truth, and no-green-by-removal guards are
  all represented in source-of-truth logs.

## System Shape

```text
wallet registry
  -> leaderboard/operator wallet discovery
  -> paginated Polymarket wallet history ingest
  -> normalized WalletEvent stream
  -> CLOB/orderbook/onchain context capture
  -> source-fingerprint dedupe
  -> CopyIntent generation
  -> executable fill model
  -> paper lifecycle ledger with realized lifecycle PnL
  -> copy-efficiency scorecard versus source wallet
  -> wallet/window feature payloads
  -> resolution-backed scorecards
  -> multi-wallet consensus/inventory candidates
  -> raw-baseline-guarded profitability search and walk-forward policy admission
  -> admission report
  -> persisted live-tracker paper truth
  -> gated live adapter using the same CopyIntent
```

## Brainstormed Strategy Families

1. Exact single-wallet BTC 5-minute copy
   - Use one proven wallet.
   - Copy each BUY into paper through the executable fill model and
     wallet-fraction sizing.
   - Track every lifecycle action: BUY, SELL, MERGE, REDEEM.
   - This is the baseline and should stay visible even when it loses.

2. Filtered exact copy
   - Same source wallet, but with explicit filters: asset, duration, price band,
     latency, min/max order size, and window caps.
   - This must never hide the full/raw result.
   - If the full/raw baseline is negative, a thin filtered slice is blocked
     until it has enough resolved evidence and a real ROI delta over raw.
   - Filters must come from a replayed profit-engine state, not prompt memory.

3. Multi-wallet external-wallet consensus
   - Group normalized events by market/window/outcome.
   - Only form a candidate when multiple distinct wallets agree on the same
     side.
   - A wallet that appears on both sides in the same market is hedged/exit
     posture, not a clean consensus vote.
   - Block if opposing wallets are present unless the run is explicitly an
     exploratory research pass.

4. Inventory copy
   - Build exposure across several wallet orders in one market.
   - Use consensus sizing caps so one wallet cannot dominate the whole window.
   - Keep paired inventory accounting separate from wallet scorecards.

5. Research/reverse engineering
   - Produce train rows from WalletEvent data only.
   - Join wallets by identical market windows.
   - ML can rank/filter wallet-copy candidates, but cannot become a separate
     live decision path.

6. Order-flow contextual copy
   - Add CVD, OBI, depth, spread, and price-jump context around source wallet
     events.
   - These features explain and filter copy quality; they do not create an
     independent non-wallet live strategy.
   - Every context feature must be timestamped and measured against fill
     quality, source PnL, and our paper PnL.

7. Sweeper / resolution-sniper research
   - Paper-only until tick-level book snapshots, queue/fill probability,
     latency, and rare total-loss outcomes are modeled.
   - This is a research lane attached to copied wallet behavior, not a live
     shortcut.

## Direction Selection

The system must not choose a live direction from ROI alone. The strategy
selector ranks four active lanes from source-of-truth state:

1. Proof-led single-wallet/policy copyability unlock
   - Choose the wallet+policy where paper WR/ROI/resolved evidence intersects
     with clean candidate-scoped CLOB proof.
   - This is the best next development lane when profit candidates and runtime
     proof are split across different wallets.
   - A persisted proof row is not enough by itself: if the proof candidate is
     not attached to the current profit-engine ranked candidates, the state is
     `CORRECTION`, not live-ready.

2. Weighted multi-wallet inventory with multi-wallet filtering
   - This is the target live architecture once current-poll proof catches up.
   - It needs 70%+ WR, positive train/validation ROI, enough resolved orders,
     at least two agreeing wallets per window, CLOB-backed candidate fills, and
     current-poll evidence. Tracker-time replay remains ranking evidence only.

3. Multi-wallet all-order exact copy
   - This stays as an execution diagnostic lane while reject/slippage rates are
     high. It is not the default live architecture unless ordinary CopyIntent
     and exact no-overcopy micro-batch evidence both pass with near-zero rejects.

4. Multi-wallet consensus/filter-only orders
   - This is the bridge from single-wallet proof to inventory, but only after
     current-poll has two or more eligible wallets agreeing inside the same BTC
     5-minute window and producing CLOB-backed paper fills.

The machine-readable report is
`data/research/wallet_copy_strategy_direction_state.json`, regenerated by
`scripts/select_wallet_copy_strategy_direction.py` and by the autonomous repair
loop. Heartbeats must report its `decision`, top direction, runtime proof, and
blockers.

## Live Admission

Live readiness requires an objective admission report:

- paper state is `paper_only=true`,
- `live_orders_allowed=false` during paper scoring,
- enough resolved paper orders,
- positive ROI and acceptable win rate,
- unresolved ratio below cap,
- latency below cap,
- positive copy-efficiency delta versus the source wallet,
- live adapter receives the exact same `CopyIntent`,
- filtered policy has passed the raw-baseline guard,
- persisted live-tracker paper truth shows strict mirror coverage,
- executable fill accounting does not reject the accepted paper orders.

If any blocker remains, the system stays paper/research only.

## Current Implementation

- `src/wallet_copy/registry.py`: wallet registry for operator-provided proven
  addresses.
- `src/wallet_copy/leaderboard.py`: Polymarket Data API CRYPTO leaderboard
  intake, WEEK/MONTH top-50 dedupe, and safe registry merge without overwriting
  named operator wallets.
- `src/wallet_copy/ingest.py`: paginated history ingest with source-fingerprint
  dedupe.
- `src/wallet_copy/strategy.py`: deterministic CopyIntent generation.
- `src/wallet_copy/paper.py`: paper lifecycle ledger with ordered
  wallet-event replay so BUY and SELL/MERGE/REDEEM are measured in the same
  source chronology that a live copy runner would face.
- `src/wallet_copy/fill_model.py`: executable source-price/CLOB fill model.
- `src/wallet_copy/features.py`: event/window features for research and ML.
- `src/wallet_copy/performance.py`: resolution-backed PnL, scorecards, admission.
- `src/wallet_copy/profit_engine.py`: policy grid search, train/validation
  admission, raw-baseline guard, and single-wallet/consensus/inventory ranking.
- `src/wallet_copy/strategy_selection.py`: explicit direction selection across
  single-wallet proof, all-order exact copy, weighted inventory, and
  multi-wallet filter lanes.
- `src/wallet_copy/consensus.py`: multi-wallet agreement candidates.
- `src/wallet_copy/inventory.py`: same-market/same-outcome inventory plans.
- `scripts/register_wallet.py`: register/update profitable target wallets.
- `scripts/onboard_leaderboard_crypto_wallets.py`: fetch weekly/monthly
  CRYPTO PnL leaderboard wallets, upsert them into the BTC-5m wallet-copy
  registry, and optionally run bounded paper/research/ML/profit refresh.
- `scripts/run_wallet_copy_pipeline.py`: paper-first ingest and copy runner.
- `scripts/analyze_wallet_copy_research.py`: research, consensus, scorecards.
- `scripts/run_wallet_copy_profit_engine.py`: profitable policy search and
  paper-only profit-policy state export.
- `scripts/select_wallet_copy_strategy_direction.py`: strategy-lane ranking and
  next-action decision state.
- `scripts/export_wallet_copy_dataset.py`: JSONL feature and label export.
- `scripts/run_wallet_live_tracker.py --seed-before-poll --seed-history-state
  <history_state.json>`: restart-safe paper lifecycle continuity mode. It
  seeds prior non-current-batch history into paper before scoring fresh wallet
  moves, which prevents false lifecycle failures after a tracker restart while
  keeping seeded orders out of live-admission truth.

## External Research Extraction

See `docs/EXTERNAL_WALLET_COPY_RESEARCH.md` for the review of the
operator-provided paste notes and public bot repositories. The structured
source registry is `configs/wallet_copy/external_bot_sources.json`, and the
current review can be regenerated with `scripts/report_external_bot_sources.py`.
The extracted implementation priorities are:

- copy-efficiency scorecards for source-vs-our latency, price, fill, and PnL;
- wallet discovery scoring based on BTC-5m resolved performance and copyability;
- OBI/CVD/depth/spread context around wallet moves;
- strict depth/circuit/trade-floor guards even in paper;
- source-wallet SELL/MERGE/REDEEM lifecycle mirror as mandatory evidence;
- sweeper/resolution-sniper research only after executable fill modeling.
- post-resolution sweeper wallet detection with strict queue/fill/latency
  modeling before any live claim.

Current external-source priority split:

- P0: `MrFadiAi/Polymarket-bot` for smart-money wallet discovery,
  `HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits` for onchain
  `OrderFilled` tracking and execution-core patterns, and
  `lihanyu81/polymarket_lp_tool` for order lifecycle/fill monitoring, plus the
  Punisher sweeper article for post-resolution queue/fill modeling and the
  `punisher_sweeper_btc5m` public wallet seed.
- P1: `alsk1992/CloddsBot` for audit/execution architecture and
  `aarora4/Awesome-Prediction-Market-Tools`, `antpalkin`'s tool-stack article,
  and `kirillk_web3`'s OpenClaw/Simmer article for ongoing tool and wallet
  discovery.
- Research-only: weather/Kalshi repos for evidence-chain discipline.
- Blocked: `Composio-HQ/polymarket-kalshi-arbitrage-bot` until a cloneable
  source is verified.

The external-source report is intentionally allowed to be non-green on
capability coverage even when syntactic validation passes. As of the current
review, the missing/partial P0 capabilities are smart-money wallet discovery,
read-only CTF `OrderFilled` tracking, and richer order lifecycle/fill
monitoring.

## Next Expansion Points

- Refresh leaderboard discovery with:

  ```bash
  python3 scripts/onboard_leaderboard_crypto_wallets.py --weekly-limit 50 --monthly-limit 50 --leaderboard-pages 1
  ```

  Then run the heartbeat-safe refresh:

  ```bash
  python3 scripts/onboard_leaderboard_crypto_wallets.py --weekly-limit 50 --monthly-limit 50 --run-pipeline --skip-live-tracker --limit 500 --pages 1 --policy-preset fast --max-wallets-for-search 25 --max-single-wallet-candidate-intents 500 --max-multi-wallet-base-intents 1000
  ```

- Heartbeats should prefer the autonomous bounded repair loop:

  ```bash
  python3 scripts/run_wallet_copy_autonomous_repair.py
  ```

  It runs pre/post learning audits, executes the bounded refreshes implied by
  non-green checks, tolerates strict tracker return code `2` as copy-efficiency
  evidence, and persists remaining code-level blockers to
  `data/research/wallet_copy_autonomous_backlog.json`.

  Use `--pages 0` and `--policy-preset default --max-wallets-for-search 0
  --max-single-wallet-candidate-intents 0 --max-multi-wallet-base-intents 0`
  only for a separately scheduled deep research run; do not pretend that a fast
  beam search is exhaustive.
- Add more known profitable wallets to `configs/wallet_copy/*.json`.
- Keep `configs/wallet_copy/external_bot_sources.json` current whenever the
  operator sends a new bot/tool source; do not let an external dry-run claim
  count as local paper/live-admission evidence.
- Onboard every operator-provided relevant bot address through
  `scripts/onboard_operator_wallets.py`: registry upsert, history ingest, paper
  exact-copy, cross-wallet AI/research, ML dataset export, profit engine, and
  paper live-tracker copy-efficiency evidence before any live-admission claim.
- Add a hot-lane tracker preset for active candidate wallets: tiny
  limit/pages, short Data API/CLOB timeouts, seed-history continuity, and
  repeated sub-minute polling so accepted BUY copyability is measured at
  seconds-level age instead of catch-up batches that are already minutes old.
- Increase ingest depth with `--pages 0` for full pagination.
- Keep the active market domain BTC 5-minute only until this copy system proves
  itself.
- Add CLOB market WS and Polygon log preconfirm sources only after they are
  truth-looped against wallet-attributed Data API rows and receipts.
- Keep the implemented copy-efficiency report as a live-admission gate:
  accepted BUYs need CLOB-backed fillability, latency, slippage, fill-ratio, and
  missed-reason evidence; fallback-only fills stay paper/research.
- Keep the profit-engine strict by default:
  `require_candidate_clob_fill_evidence=true` blocks profitable fallback-only
  historical slices from candidate PASS. Use
  `--allow-candidate-fallback-fill-evidence` only for explicitly labeled
  optimistic research replay.
- For large wallet universes, the profit engine has two modes:
  heartbeat-safe fast search (`--policy-preset fast`, wallet/intent caps, and
  optional `--no-inventory-search`) and deep exhaustive research. The state
  records `wallet_search_summary`, `searched_history_events`, and
  `skipped_candidate_counts`; these fields are part of the evidence and must
  not be omitted from reporting.
- Multi-wallet inventory remains required for the strategy direction, but it is
  too expensive for every heartbeat over a leaderboard-sized registry. It must
  run as a bounded/deep research job with explicit base-intent limits, candidate
  counts, and runtime budget until the inventory builder has a dedicated beam
  search.
- Deep research must add consensus/inventory raw guards before any multi-wallet
  PASS can be considered live-admissible: union-all-buys baseline,
  per-participant wallet raw baselines, same-window opposite-side conflict
  checks, and a candidate fingerprint that the live tracker can match.
- Fast search can nominate candidates only. Live admission must block bounded
  search (`fast_or_bounded_search_not_live_admissible`) until a full-history or
  explicitly equivalent deep replay agrees.
- Run `python3 scripts/analyze_wallet_sweeper_profile.py` after adding
  high-price near-close wallets; a strong sweeper signature is research
  evidence, not live-admission evidence.
- Treat unmatched lifecycle exits as mirror failures: SELL/MERGE/REDEEM only
  count as mirrored when the paper lifecycle reduces a matching position.
- Add ML ranking only on top of normalized feature rows, labels, and scorecards.
- Add live adapter token mapping for inventory plans only after paper admission
  passes and the operator explicitly enables live execution.
