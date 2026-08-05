# External Wallet-Copy Research Notes

Date: 2026-06-24

Scope: BTC 5-minute wallet-copy trading only. This note summarizes the
operator-provided paste notes and public GitHub repositories reviewed for ideas
that should strengthen this repo's wallet-copy system. It is not strategy
authority and does not make any live-order permission.

Machine-readable source registry:
`configs/wallet_copy/external_bot_sources.json`.

Review command:

```bash
python3 scripts/report_external_bot_sources.py
```

## Built-In Leaderboard Discovery

Polymarket exposes a public Data API leaderboard endpoint that can be queried
for CRYPTO category, WEEK/MONTH time periods, PNL ordering, and limit 50. This
repo now treats that leaderboard as a first-class wallet discovery source:

```bash
python3 scripts/onboard_leaderboard_crypto_wallets.py --weekly-limit 50 --monthly-limit 50
```

Current local intake result from the first run:

- 100 raw leaderboard rows: WEEK top 50 plus MONTH top 50;
- 84 unique wallets after dedupe;
- 16 wallets appeared in both weekly and monthly top 50;
- 82 new wallets were added to `configs/wallet_copy/wallets.json`;
- 2 existing operator wallets were preserved and only enriched with leaderboard
  tags/notes;
- subsequent bounded history/paper/research/ML refresh produced BTC-5m history
  for 39 wallets, 12,591 normalized wallet events, and 14,931 paper orders.

Admission remains ANALYZE/CORRECTION. The fast profit search found a best leaderboard
candidate, but it stayed blocked by ROI threshold, unresolved ratio, negative
raw baseline guard, and missing CLOB-backed fill evidence. The scoped live
tracker achieved 100% mirror coverage on its measured subset, but
copy-efficiency failed because BUY copies were fallback-filled rather than
CLOB-backed and API event age was above the live-admission cap.

## Bottom Line

The useful signal across the material is consistent:

- do not add another stack of price-only indicators as the primary edge;
- copy externally proven wallets first, then measure whether our fill price,
  latency, and lifecycle PnL match the source wallet;
- if we add signals, use them as execution/context filters around copy:
  orderbook imbalance, cumulative volume delta, depth, spread, and latency;
- dry-run/paper must be executable, not a synthetic success stub;
- every copied source wallet needs a raw/full baseline, filtered policy result,
  and copy-efficiency delta versus the source wallet.

## Reviewed Inputs

### Operator Paste Notes

Key claims and usable takeaways:

- The "2 indicators" note argues for CVD and OBI rather than 10+ derived
  indicators. For this repo, CVD/OBI should be added as context features and
  execution guards, not as a separate autonomous live strategy.
- The "6 bot types" note maps common Up/Down strategies: pure arb, directional
  arb, repricing/fair-value, cross-timeframe, imbalance, and near-resolution.
  For our reset, these become research labels around wallet-copy events.
- The sweeper note is useful only if backtested with real book snapshots, queue
  priority, latency, fill probability, and rare-full-loss accounting.
- The "1000 bots" note reinforces the existing raw-baseline guard: an AI vote
  or optimistic entry-vs-resolution backtest is worthless without real fill,
  slippage, depth, and latency evidence.

### Punisher X Article: Post-Resolution Sweeper Bot

Source:

- https://x.com/0x_Punisher/status/1911846164636762470
- attached article text from the operator in this workspace

Public wallet:

- profile URL from article:
  `https://polymarket.com/@0x13f0bcec1e2e60ec9acc3bee4d2da2fe9694a50f-1774334442364?r=punisher`
- resolved proxy wallet:
  `0x13f0bcec1e2e60ec9acc3bee4d2da2fe9694a50f`

What is useful:

- this is not predictive wallet-copy in the normal sense; it is a
  post-resolution or near-resolution queue-position strategy;
- the core claims are measurable: high-price bids near close, FIFO queue race,
  reference-exchange close-clock inference, CLOB order-book confirmation,
  redeem lifecycle PnL, idle-capital cost, and rare flip/full-loss accounting;
- the public wallet is BTC-5m relevant enough to onboard as a paper-copy and
  sweeper research candidate.

Local verification:

- profile HTML confirmed proxy wallet
  `0x13f0bcec1e2e60ec9acc3bee4d2da2fe9694a50f`;
- Data API sample of 1500 rows found 810 BTC-5m rows, 205 other crypto rows,
  and 485 other rows; latest sampled event was `2026-06-24T15:30:38Z`;
- `scripts/onboard_operator_wallets.py` registered it as
  `punisher_sweeper_btc5m` and completed history/paper/research/ML/profit
  stages; the live-tracker stage intentionally returned non-zero because
  copy-efficiency did not pass;
- `scripts/analyze_wallet_sweeper_profile.py` identified a strong sweeper
  signature in the local history: 526 BTC-5m BUY events, 526 at price >= 0.95,
  and 475 high-price near/post-close buys.

Current admission status:

- paper-only, `live_orders_allowed=false`;
- mirror coverage passed in the live tracker, but copy-efficiency failed;
- fresh tracker smoke: 51 wallet events, 45 BUY copy events, 45/45 filled in
  paper, 0 missed, 0 rejected, but only 6 CLOB-backed fills and 39 fallback
  fills;
- profit engine selected this wallet as the best candidate but kept it
  `BLOCKED`: unresolved ratio remains around 97%, resolved sample is too small,
  and candidate fill evidence is fallback-only rather than CLOB-backed.

Action for this repo:

- keep `punisher_sweeper_btc5m` active in paper/research registry;
- use `src/wallet_copy/sweeper.py` and
  `scripts/analyze_wallet_sweeper_profile.py` to detect post-resolution wallet
  patterns across future operator-provided wallets;
- implement the missing P0 capability:
  `post_resolution_sweeper_queue_model`, including tick-level BTC reference
  price, exact window-close clock, CLOB snapshots, FIFO queue approximation,
  fill probability, redeem lifecycle PnL, idle-capital cost, and rare
  flip/full-loss modeling;
- do not treat the article's PnL claim, paid-partnership framing, or
  source-price fallback replay as live-ready evidence.

Claude-style reviewer notes:

- split sweeper detection from sweeper trading: `analyze_sweeper_profiles()`
  detects wallet behavior, while the next layer should add
  `build_sweeper_candidate_events()` / `score_sweeper_event()` with
  `pre_close_queue`, `at_close`, `post_close`, `redeem_only`, and
  `too_late_to_copy` tags;
- add a real queue-fill lane in `src/wallet_copy/fill_model.py`: separate
  `clob_taker_fill`, `clob_maker_queue_fill`, and `fallback_replay`; sweeper
  admission must reject fallback by default;
- extend `src/wallet_copy/live_tracker.py` to capture pre-close, close, and
  post-close book snapshots and improve token resolution when Data API rows are
  missing usable token IDs;
- split latency in `src/wallet_copy/copy_efficiency.py` into source-to-API lag,
  poll-to-decision, close-to-observed, book age, and decision-to-fill;
- attach BTC close-clock/reference-price truth to sweeper events before
  treating a high-price BUY as valid;
- make redeem PnL lot-based in paper/performance accounting, including idle
  capital seconds, orphan redeems, duplicate redeem idempotency, and rare
  flip/full-loss cases;
- add sweeper-specific admission guards in `src/wallet_copy/profit_engine.py`:
  strategy lane, min/max seconds-to-close, reference-side requirement,
  queue-fill evidence requirement, and matured-vs-too-fresh resolution ratio.

### alsk1992/CloddsBot

Useful:

- broad architecture pattern: signal bus -> risk layer -> single execution
  route -> audit/ledger;
- decision ledger/audit-trail mindset;
- feature vocabulary around orderbook, liquidity, whale tracking, copy trading,
  backtesting, and risk limits.

Do not import as-is:

- it is a broad multi-market agent platform, not a focused BTC 5-minute
  wallet-copy engine;
- its copy module defaults include delay and synthetic dry-run fill behavior,
  which is not acceptable for 1:1 mirror verification.

Action for this repo:

- keep the single `CopyIntent` execution boundary and expand audit output to
  include copy-efficiency deltas;
- do not let any non-wallet signal bypass the wallet-copy intent path.

### lihanyu81/polymarket_lp_tool

Useful:

- WS-first order state, REST reconciliation, cancel/repost idempotency;
- vanished-order fill inference using recent trades and user WS cumulative
  matched size;
- anti-sniping ideas: midpoint jump pause, stable-mid confirmation, cooldown,
  max reprice distance;
- concrete order-manager telemetry around cancel/repost attempts.

Do not import as-is:

- it is a liquidity-reward/open-order manager, not copy trading;
- it assumes we manage our own resting orders, while source-wallet trades are
  externally observed.

Action for this repo:

- extend live-tracker/paper evidence with `our_fill_truth_source`, order vanish
  classification, and cancel/repost latency where live adapter is later gated;
- add depth/spread/jump/cooldown guard fields to copy-efficiency scoring.

### MrFadiAi/Polymarket-bot

Useful:

- leaderboard-based smart-money discovery;
- wallet profile, activity, positions, category breakdown, PnL, win rate,
  consistency, profit factor, and whale-trade concentration filters;
- explicit auto-copy controls: target addresses, top-N, size scale, max trade
  size, slippage, FOK/FAK, min trade size, dry-run;
- sell-ratio tracking for exit detection across one or more wallets.

Do not import as-is:

- its dry-run can be a plain success response instead of an executable fill
  simulation;
- top leaderboard PnL is not enough for our narrow BTC 5-minute scope;
- copy execution needs lifecycle parity, not only market-order success.

Action for this repo:

- implement a wallet discovery scorer with BTC-5m-specific filters:
  resolved ROI, resolved count, recent stability, copyability, fillability,
  latency, trade concentration, unresolved ratio, drawdown, and market-domain
  purity;
- add group sell-ratio and source-wallet unwind detection as lifecycle signals.
- P0 external-source registry tag:
  `mrfadiai_polymarket_bot` / `smart_money_wallet_discovery`.

### HarrierOnChain/Prediction-Markets-Trading-Bot-Toolkits

Useful:

- clean shared engine pattern: ingestion -> parse -> eligibility -> sizing ->
  exposure caps -> risk -> execute -> position monitor;
- onchain `OrderFilled` log subscription filtered by watched wallet as a low
  latency path to research;
- risk guard: circuit breaker, consecutive large trade halt, orderbook depth
  check, trade floor, dry-run through full path;
- strategy taxonomy maps well to our labels: copy trading, BTC arb, directional
  arb, market making, OBI, resolution sniper.

Do not import as-is:

- copy bot ignores whale SELL by design and delegates exits to TP/SL. Our goal
  is exact lifecycle mirror, so SELL/MERGE/REDEEM must be mirrored and scored;
- OBI/resolution-sniper modules are placeholders in the inspected source;
- advertised latency/performance numbers need our own measurement.

Action for this repo:

- evaluate a Polygon log watcher as a future preconfirm candidate, but only
  after truth-looping it back to Data API wallet rows and receipts;
- add per-token depth guard and circuit-breaker blocker reasons to live-tracker
  state even in paper mode;
- keep source-wallet SELL as mandatory lifecycle evidence, never a no-op.
- P0 external-source registry tag:
  `harrier_prediction_market_toolkits` /
  `low_latency_onchain_tracking_and_execution_core`.

### Weather Repositories

Reviewed:

- AruneshDev/Automated-Trading-System-Kalshi-Weather-Model
- yangyuan-zhen/PolyWeather

Useful:

- multi-source data reconciliation, error correction, and settlement-specific
  evidence chains;
- clear distinction between forecast/model context and official settlement
  observations;
- production observability, health endpoints, replayable events, and cache
  invalidation in PolyWeather.

Do not import as-is:

- domain is weather, not BTC 5-minute orderflow;
- not a direct wallet-copy implementation.

Action for this repo:

- borrow the evidence-chain discipline: every wallet-copy decision should state
  source wallet event, market/orderbook context, fill model, lifecycle status,
  and settlement/resolution evidence.

### Awesome Prediction Market Tools

Useful discovery categories:

- wallet analyzers and copy scores;
- real-time wallet alerts;
- tick-level orderbook/backtest data providers;
- cross-venue APIs and dashboards.

Action for this repo:

- define external data slots, but keep local source-of-truth first:
  wallet history, CLOB book snapshots, live tracker, paper fills, resolutions.
- if an external provider is added, log provider latency, coverage, and
  disagreement versus our first-party state.

### antpalkin X Article: Polymarket $1M Bot Tool Stack

Reviewed:

- https://x.com/antpalkin/status/2046654122892403188

Useful:

- the public teaser frames a Polymarket bot stack as 28 repositories across six
  layers;
- this is relevant as a tool-discovery and architecture-benchmark lead.

Do not import as-is:

- the full concrete tool list was not verifiably extracted in this run;
- the claim is not wallet evidence, dry-run evidence, or live-admission
  evidence.

Action for this repo:

- keep this as a P1 discovery source until the full article/tool list is
  accessible;
- when concrete repositories or wallets are recovered, add each one as its own
  reviewed registry row with an explicit priority, blockers, and local
  capability map if applicable.

### kirillk_web3 X Article: OpenClaw Clawdbot Guide

Reviewed:

- https://x.com/kirillk_web3/status/2025933391003066796
- local pasted text:
  `/Users/belavarga/.codex/attachments/3c462eac-3be3-4c0b-9036-781054f0226b/pasted-text.txt`

Useful:

- provides concrete Polymarket wallet/profile leads and an OpenClaw/Simmer
  tool-stack description;
- mentions Simmer skills for copytrading, signal sniper, weather trading,
  AI divergence, and BTC 5m/15m fast-loop execution;
- reinforces that execution speed, risk limits, and repeatable structure matter
  more than model confidence.

Wallet triage:

- `0x1d0034134e339a309700ff2d34e99fa2d48b0313`: resolved from the short
  `@0x1d0034134e` Polymarket profile. Sampled Data API rows showed mixed
  short-duration activity with `34` BTC-5m rows and `317` BTC-15m rows in the
  sampled window. Onboarded paper-only as `kirill_claimed_350k`.
- `0xde79cc7660d5c05b4cd2f4e72cae30cde2583d9a`: sampled rows showed `593`
  BTC-5m rows and `385` BTC-15m rows. Onboarded paper-only as
  `kirill_micro_btc15`.
- `0x594edb9112f526fa6a80b8f858a6379c8a2c1c11`: sampled rows were
  weather-dominated with no BTC-5m rows, so it remains research-only and was
  not added to the BTC-5m wallet registry.

Current local result:

- `scripts/onboard_operator_wallets.py` completed history/paper/research/ML
  export/profit-admission commands for the two BTC-relevant full addresses.
- The onboarding pipeline status was `PASS` as a workflow, but strategy
  admission remains `CORRECTION/BLOCKED`.
- Current `wallet_copy_research_state.json` has `3` wallets, `1044` BTC-5m
  events, `922` copy intents, `0` consensus PASS, `0` inventory PASS, and
  no admitted wallet.
- The two Kirill wallets are stale/unresolved in the current local resolution
  index, so they are not live-ready and must not be promoted.

Do not import as-is:

- OpenClaw/Simmer setup instructions are tool-stack ideas, not proof that any
  strategy is profitable through our execution path;
- the article includes weather and 15-minute BTC material outside the current
  BTC-5m-only live scope;
- affiliate/hosting/install guidance is irrelevant to copy-trading admission.

Action for this repo:

- keep the two BTC-relevant wallets in paper/research/ML analysis only until
  resolution coverage and CLOB-backed copy-efficiency exist;
- use OpenClaw/Simmer as source ideas for skill orchestration and copytrading
  UX, not as an execution bypass;
- prioritize smart-money wallet discovery so this kind of article can be
  converted into scored wallet candidates automatically.

### Composio-HQ/polymarket-kalshi-arbitrage-bot

The provided repository URL returned GitHub 404 during clone. Treat the link as
unverified until a working repository URL is provided or found.

## Implementation Backlog

### P0: External-Source Capability Pressure

The reviewed GitHub sources are now tracked in
`configs/wallet_copy/external_bot_sources.json`. The registry must stay
machine-readable so the heartbeat can distinguish:

- implemented local capability,
- partial local capability,
- missing local capability with a concrete file-level backlog.

Current P0 capability gaps:

- `smart_money_discovery`: add `src/wallet_copy/discovery.py` and
  `scripts/discover_wallet_copy_candidates.py` to rank wallets by BTC-5m
  resolved ROI, drawdown, market purity, activity freshness, unresolved ratio,
  CLOB fillability, and copyability instead of generic leaderboard PnL.
- `ctf_orderfilled_tracker`: add `src/wallet_copy/onchain_ctf.py` as a
  read-only CTF `OrderFilled` decoder and Data API latency benchmark. It must
  never create a `CopyIntent` without wallet-attributed Data API truth.
- `lifecycle_fill_monitoring`: extend live-tracker output with own-order
  vanished-order, cancel/repost latency, and fill reconciliation metrics once a
  gated adapter path exists.

### P0: Copy-Efficiency Scorecard

Add a wallet/window/order-level report:

- source wallet event timestamp, side, outcome, price, size;
- our observed time, intended price, executable fill/reject, fill price, fill
  size, slippage bps, latency ms;
- source PnL versus our paper PnL;
- missed-copy reason: no token mapping, stale event, depth guard, spread guard,
  fill model rejection, policy filter, lifecycle coverage violation.

Admission must require positive copy efficiency, not only positive filtered ROI.
Fallback-only source-price fills are optimistic replay evidence; they are not
candidate PASS evidence when `require_candidate_clob_fill_evidence=true`.

### P0: Multi-Source Tracker Truth

For each followed wallet:

- Data API trades/activity is wallet-attributed truth;
- CLOB market WS is fast anonymous context;
- Polygon receipts/logs are confirmation/preconfirm candidates;
- user WS is only for our own orders, never external wallet truth.

The fastest external-wallet path is:

```text
CLOB/orderbook context -> wallet-attributed Data API row -> optional onchain
receipt/log confirmation -> CopyIntent -> executable paper fill -> lifecycle PnL
```

### P1: Wallet Discovery Scorer

Rank wallets for BTC 5-minute copy by:

- resolved ROI and realized PnL;
- number of resolved BTC 5m windows;
- raw/full baseline, not only filtered slices;
- copyability: public activity freshness, event count, market purity, average
  source size, order type behavior inferred from fills;
- fillability: source price distance to book, depth, spread, rejection rate;
- concentration risk and drawdown;
- stable recent versus all-time performance.

### P1: OBI/CVD Context

Compute and persist:

- orderbook imbalance by side and token;
- cumulative volume delta in the current 5-minute window;
- book depth and spread at source event time;
- pre/post source-trade price movement.

Use these to explain and filter wallet-copy candidates. They must not become an
independent live strategy.

### P1: Inventory And Consensus

For multiple followed wallets:

- classify each wallet's net posture per window/outcome;
- ignore hedged/exit posture as clean consensus;
- require agreement strength and wallet diversity;
- size by our wallet fraction/cap, not raw source notional;
- track paired Up/Down inventory cost and unwind/redeem lifecycle.

### P2: Sweeper Research Lane

Keep sweeper/resolution-sniper paper-only until:

- tick-level orderbook snapshots exist around resolution;
- queue priority and fill probability are modeled;
- rare total-loss outcomes are included;
- paper fills are executable, not assumed.

Current sweeper research seed:

- `punisher_sweeper_btc5m`
  (`0x13f0bcec1e2e60ec9acc3bee4d2da2fe9694a50f`) has a strong local
  high-price near-close signature, but live admission is still blocked by
  fallback CLOB evidence and unresolved-ratio gates.
- Regenerate with:

```bash
python3 scripts/analyze_wallet_sweeper_profile.py
```

## Workflow Consequences

- The heartbeat must report copy effectiveness, not just process health.
- The heartbeat must report missing P0 external-source capabilities from
  `scripts/report_external_bot_sources.py`, not only whether the registry is
  syntactically valid.
- The profit engine must keep raw/full losing copy visible.
- The profit engine must keep optimistic replay numbers visible, but a
  fallback-only profitable slice is blocked from candidate PASS until it has
  CLOB-backed fill evidence.
- The live tracker must distinguish historical event age from live observation
  latency.
- The live tracker must treat unmatched SELL/MERGE/REDEEM as coverage
  violations, not as mirrored lifecycle events.
- Research/ML can rank wallets or filters, but cannot bypass source-wallet
  evidence and the `CopyIntent` paper/live parity contract.
