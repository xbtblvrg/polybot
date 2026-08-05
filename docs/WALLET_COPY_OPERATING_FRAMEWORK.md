# Wallet-Copy Operating Framework

Date: 2026-07-01

This is the operating contract for the active Polymarket bot work. It exists so
the system does not drift back into old strategy families or call the workflow
green by removing the failing evidence source.

The machine-readable source is `src/wallet_copy/mission.py`. This document is
the human runbook for the same contract.

## Collateral Truth

Polymarket CLOB cash reconciliation uses Polymarket USD (`pUSD`) on Polygon,
not Polygon USDC.e or native USDC:

```text
token: pUSD / Polymarket USD
address: 0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb
decimals: 6
code constant: src/wallet_copy/collateral.py
```

Read-only balance audits must query this token for the proxy wallet. Empty
USDC.e/native-USDC balances are not evidence of missing funds.

## Mission Lock

Primary objective:

```text
Find a profitable BTC 5-minute trading direction by copying external wallet
orders with maximum attribution, minimum latency, full paper lifecycle tracking,
and live execution through the same CopyIntent path for the selected
policy-eligible intents.
```

Active development objective:

```text
Copy profitable external wallet order-flow as efficiently as possible, operate
the approved first-live single-wallet guard after explicit operator approval,
keep background and multi-wallet development paper-only, and keep repairing
latency, slippage, fill, and lifecycle defects without creating a second live
authority.
```

Active scope:

- BTC 5-minute Polymarket markets only.
- External wallet order copying is the active strategy authority.
- Operator-provided wallets and CRYPTO leaderboard wallets are primary source
  candidates.
- ML, AI review, clustering, and reverse engineering may rank or explain wallet
  copy candidates, but they cannot become independent live trading authority.
- SOL, direct ML signals, fair-value signals, replacement/watchdog promotion,
  and generic non-wallet strategies are legacy or research-only.

## Layered Live Progression

The operating architecture is layered. These layers run in parallel, but live
promotion is ordered:

1. **Leaderboard wallet scanner**

   Continuously scan Polymarket CRYPTO `WEEK` and `MONTH` PnL leaderboards,
   preserve previous and registry wallets, and add every fetched BTC-5m-relevant
   wallet to paper observation. This layer is discovery only and has no live
   trading authority.

2. **Single-wallet paper copy factory**

   Copy every candidate wallet in paper through the same CopyIntent lifecycle,
   rank wallets by source profitability plus our actual current copyability, and
   maintain a backup pool. This layer must copy as much of the wallet flow as
   the live gates require, but remains paper-only until one wallet passes.

3. **Single-wallet live promotion gate**

   The first live-ready target is one best paper-proven, CLOB-copyable
   wallet/policy. Live is profitability-filtered and policy-sized, not a strict
   1-to-1 mirror of every source order. The selected intents still use the same
   CopyIntent body and lifecycle with execution permission added behind an
   explicit operator gate. The rest of the wallet universe continues in paper.

4. **Background backup wallet rotation**

   While a primary live wallet is running, continue paper-copying all observed
   leaderboard/operator wallets and keep ranked alternates ready. If the live
   wallet degrades, replacement is chosen from the same paper/live parity proof,
   not from a new strategy family.

5. **Multi-wallet paper strategy builder**

   In parallel, build a separate paper strategy that copies many wallets in each
   eligible BTC-5m window, constructs weighted inventory, and compares its
   paper performance and reliability against the single-wallet live baseline.

6. **Multi-wallet upgrade gate**

   Multi-wallet copy becomes live only after it is more profitable or more
   reliable than the single-wallet lane and independently passes current-poll
   candidate-specific CLOB truth, zero fallback/reject/miss BUYs, paper
   profitability, and selected-intent CopyIntent parity.

This means the multi-wallet system is the target upgrade architecture, not a
blocking prerequisite for the first live-ready single-wallet copy bot. The
first live-ready path must never stop the background wallet scanner or the
multi-wallet paper strategy builder.

## Current Live Gate Phase

As of 2026-07-03, the first live lane is the guarded single-wallet copy runner.
Paper/live admission is `PASS`; the live runner is allowed to execute only
through the guarded CopyIntent path and only while the operator gate remains
present:

```text
primary live candidate: wcp_5b5e0a28740729cc99f64e59
primary source wallet: 0x9412cdfc1e3171e1aabb013d0f494986445d0cd0
primary policy: fast_wf_0.05_cap_2_all_prices_minusd_0_all_window
runtime guard: scripts/run_wallet_copy_live_guard.py
current expected guard status: LIVE_GUARD_RUNNING
current expected runtime live_orders_allowed: true
copy mode: profitability_filtered_single_wallet_copy
copy style: policy_filtered_not_strict_1_to_1
strict source-order 1:1 required: false
```

This live lane was rotated under `OP-LIVE-20260703-BELA` /
`OP-AUTONOMY-20260703-BELA` after the prior pinned wallet stayed dormant and
RTDS/CLOB proof selected the 0x9412 wallet as the best-evidenced active BTC-5m
source.

The live guard is the only workflow owner for live orders in this phase. It must
stop sending orders immediately if the profit engine or strategy direction says
`live_ready=false`, `profitability_proven=false`, or
`runtime_live_orders_allowed=false`. It must stay pinned to the approved
single-wallet candidate unless a later operator-approved rotation updates the
mission contract, strategy state, and heartbeat together. The paper proof states
remain paper-only evidence; only the guarded live execution arm may show
`paper_only=false` and `live_orders_allowed=true`.

Any `src/wallet_copy/mission.py` mutation that changes live-set membership or
live policy requires restarting the existing live guard in the same ordered
step, because the running guard snapshots the mission contract at import time.

In this phase "copy trading" means the profitable, policy-eligible slice of the
pinned source wallet: freshness, minimum/maximum size, wallet fraction, policy
selection, token mapping, and ledger dedupe may intentionally skip or resize
source orders. This is not a permission to trade a separate strategy; every live
order must still originate from a selected CopyIntent under the approved
candidate and live guard.

Mechanical protection rules are scoped separately:

- live member rotation uses the code-operative rolling 20 resolved-fill rule:
  rotate/demote a member when its rolling 20 resolved PnL is at or below the
  configured loss threshold (`-8.0` in `promotion_rotation.py` and
  `report_fill_quality.py` unless a later committed config changes it);
- sizing ramp pause uses the flow-contract rolling 100-fill rule in
  `docs/agents/AUTONOMOUS_FLOW.md`;
- scorecards must name which rule fired and must not use the rolling-100 ramp
  metric as a substitute for the rolling-20 member-rotation metric.

Heartbeat behavior in this phase is inspection-first:

- verify whether the live guard is running, blocked, or live-admissible,
- inspect live execution arm and lifecycle ledger state,
- report new submitted, filled, or rejected live orders,
- keep leaderboard discovery, backup wallet paper-copy, and multi-wallet paper
  strategy development visible,
- do not claim live trading is running while the live guard, arm, profit engine,
  and strategy direction disagree,
- trigger repair/backlog when the live guard blocks, loses proof, loses token
  parity, or strategy/live-ready truth remains non-green.

`NOT OK`, `CORRECTION`, `BUG_SUSPECT`, and `actual_live_trading=false` are not
report-only outcomes. The autonomous loop must identify the proximate blocker
from guard, arm, ledger, profit, strategy, and process state, then apply the
safest available fix in the same run when it can do so without violating live
safety. Safe fixes include code/config/dependency repairs, sharper measurement,
derived-state refreshes, and test-backed backlog changes. If the required fix
would restart the live guard, rotate the primary wallet, place an order, start a
second live runner, flip `live_orders_allowed`, or bypass profit/strategy/operator
gates, the heartbeat must not do it silently; it must report the exact gated
operator action needed and keep the system single-authority.

## Non-Deviation Rule

Do not leave this framework unless one of these is true:

- the operator explicitly changes the mission,
- source-of-truth evidence proves a replacement wallet-copy path covers the same
  objective better,
- the mission contract, docs, tests, and persisted state are updated together.

It is forbidden to call the workflow green because a source, process, log,
wallet, strategy, or failing metric was deleted, disabled, hidden, narrowed
away, or overwritten. Green requires replacement coverage plus evidence.

Passive `HOLD` is not an operating status. Use:

- `GREEN` or `PASS`: evidence-backed and no relevant blocker remains,
- `WATCH`: active measurement is running and no code repair is required now,
- `ANALYZE`: evidence is thin, stale, bounded, missing, unresolved, or
  research-only,
- `CORRECTION`: a concrete copy, fill, identity, lifecycle, proof, or parity
  defect must be repaired,
- `BUG_SUSPECT`: source-of-truth states contradict or a blocker repeats without
  progress.

Non-green status is an action loop, not a parking state. If the workflow is not
globally green, the current run must produce one concrete progress action:
repair a bounded defect, add sharper measurement, or write a code-level backlog
item with file/function and verification command. Repeating `WATCH` requires new
evidence from the active measurement, not just another idle cycle.
For `NOT OK` or `CORRECTION`, a plain notification is insufficient: first try to
find and apply the safest concrete repair, then report what changed, what was
verified, and what still requires an operator gate.
Autonomous repair recurrence must continue until both `copy_trading_green=true`
and `live_ready=true`, unless the operator explicitly stops or changes the
mission. After the operator opens the live gate, the heartbeat switches to live
gate monitoring; it must not launch duplicate live writers and it must not
report live trading as active while paper/live admission is blocked.

## Green Semantics

Subsystem `PASS` is local evidence only. It is not global green.

Copy-trading is green for the first live path only when the primary
single-wallet/policy copy succeeds:

- source route is healthy for required Data API, Gamma, and CLOB evidence,
- candidate-scoped current-poll CLOB truth is present,
- every required BUY has a CLOB-backed CopyIntent fill,
- fallback-filled, rejected, and missed BUY copy events are all zero.

Multi-wallet upgrade green is separate and stricter:

- multi-wallet all-order exact-copy passes for observed BTC-5m BUY source
  events,
- weighted inventory has candidate-specific current-poll CLOB truth,
- paper performance is better or more reliable than the live single-wallet
  baseline,
- fallback-filled, rejected, and missed BUY copy events are all zero.

The bot is green only when it is live-ready:

- `copy_trading_green=true`,
- paper profitability and validation gates pass,
- live-readiness gates pass with no blockers,
- paper/live CopyIntent parity is proven,
- live submit remains behind an explicit operator gate.

If these conditions are not all true, report `ANALYZE`, `CORRECTION`, or
`BUG_SUSPECT`; do not report global `GREEN`, even if active-hotlane selection,
runtime proof, replay, or an individual tracker check has `PASS`.

## System Workflow

The required workflow is linear for admission, even if implementation jobs run
in parallel.

1. Discover and register wallets

   Sources:

   - `configs/wallet_copy/wallets.json`
   - `data/research/wallet_copy_leaderboard_crypto_state.json`
   - operator-provided Polymarket profiles or wallet addresses

   Required output: every relevant wallet has a stable registry identity,
   provenance, enabled flag, and BTC-5m scope.

   Leaderboard discovery must maximize the bounded observation universe:

   - fetch both Polymarket CRYPTO `WEEK` and `MONTH` PnL leaderboards,
   - use full pagination until the API is exhausted or the configured max-page
     cap is reached,
   - copy every fetched wallet into the paper-only registry unless an explicit
     bounded smoke run is requested,
   - preserve previously seen and registry-known leaderboard wallets so a
     rotating leaderboard cannot make evidence disappear,
   - mark the state `ANALYZE` or `CORRECTION` and initialize a change action if
     the wider wallet universe still does not produce a live-admissible
     candidate.

   Every development lane has a finite logical budget. The strategy direction
   state must track each lane's previous blockers and progress metrics, then
   force reevaluation when a lane repeats without improvement:

   - covered lanes: profitable wallet copy efficiency, best single-wallet
     copyability, WR repair, all-order exact-copy diagnostics, weighted
     multi-wallet inventory, and multi-wallet current-poll filter,
   - default max: three no-improvement cycles or three same-blocker cycles,
   - short diagnostic lanes max: two no-improvement or same-blocker cycles,
   - progress means better current-poll source events, more CLOB/book-filled
     copy events, fewer reject/fallback events, lower runtime p95 age, more
     resolved orders, better paper WR/ROI/validation WR, better average orders
     per window, better candidate-specific CLOB fill rate, or fewer blockers,
   - if a logical limit is hit, the next action must rerank, rebuild, rotate
     cohorts, repair CopyIntent lifecycle, or write a code-level backlog before
     repeating the lane,
   - a limit hit can never be solved by hiding a source, narrowing observation
     to make metrics green, or relaxing live-readiness gates.

   The strategy direction state must also run a higher-level development
   program review. This review exists to avoid searching for a non-existent
   solution inside the wrong lane. It must trigger a full rethink when evidence
   shows any of these proof splits:

   - single-wallet copyability passes, but weighted multi-wallet inventory
     cannot promote that proof,
   - the weekly/monthly leaderboard universe is already large, but copy
     execution still loses edge through latency, reject, or slippage defects,
   - paper inventory looks profitable, but current-poll candidate-specific CLOB
     truth is missing or fallback-filled,
   - tracker-time inventory replay exists, but current-poll consensus does not
     produce fresh eligible multi-wallet signals,
   - all-order CopyIntent lifecycle still has fallback or rejected BUY truth,
   - any development lane hits its logical limit.

   A full rethink must output hypothesis statuses, strategic traps, a
   stop-doing list, one `next_major_change_action`, and a code-level
   implementation backlog with file/function/verification command. It must not
   continue wallet discovery as the primary unlock when copy execution is the
   bottleneck, and it must not continue proving the same single-wallet lane when
   the missing work is the bridge into multi-wallet inventory.

2. Ingest history and normalize every move

   Sources:

   - `data/research/wallet_copy_history_state.json`
   - raw wallet event JSONL logs

   Required output: normalized wallet events with source wallet, market window,
   outcome, side, price, size, timestamp, transaction hash, and fingerprint.

3. Generate deterministic CopyIntents

   Code:

   - `src/wallet_copy/strategy.py`
   - `src/wallet_copy/models.py`

   Required output: BUY events become stable CopyIntent objects. SELL, MERGE,
   and REDEEM events remain lifecycle events attached to paper inventory.

4. Execute paper through the live-equivalent path

   Code:

   - `src/wallet_copy/fill_model.py`
   - `src/wallet_copy/paper.py`
   - `src/wallet_copy/execution.py`

   Required output: paper orders use executable fill logic, record filled,
   rejected, missed, and lifecycle results, and keep `paper_only=true` and
   `live_orders_allowed=false`.

5. Measure current-poll copyability

   Sources:

   - `data/research/wallet_copy_active_hotlane_live_tracking_state.json`
   - `data/research/wallet_copy_candidate_forward_live_tracking_state.json`
   - CLOB book and market websocket JSONL evidence
   - optional onchain receipt evidence

   Required output: candidate-scoped, current-poll, CLOB-backed required BUY
   copy events. Fallback-only fills, seeded orders, and tracker-time replay are
   useful diagnostics but not live admission truth.

6. Run profit and strategy selection

   Sources:

   - `data/research/wallet_copy_profit_engine_state.json`
   - `data/research/wallet_copy_strategy_direction_state.json`
   - `data/research/wallet_copy_candidate_runtime_proof_index.json`

   Required output: ranked candidates where paper profitability and runtime
   copy proof are attached to the same candidate, source wallet, policy, and
   window logic.

7. Choose the development lane

   Current intended ranking:

   - first: profitable-wallet copy-efficiency development in paper,
   - next: proof-led single-wallet/policy copyability unlock for the first live
     promotion candidate,
   - always in background: leaderboard wallet scanner and paper backup pool,
   - parallel research: weighted multi-wallet inventory by window with
     multi-wallet filter,
   - upgrade target: multi-wallet copy only after it beats or improves
     reliability over the live single-wallet baseline,
   - diagnostic: all-order exact copy and micro-batch probes,
   - research: adaptive bot, ML, tracker-time replay, sweeper tactics.

   The strict live gate is not relaxed just because the current best wallet is
   rejected by age or slippage. Those rejects become paper-only development
   evidence: measure strict policy beside relaxed shadow profiles, improve the
   source path, and only promote a threshold change after large-sample paper
   PnL/WR/ROI proves it is still profitable.

8. Prove live parity

   Live is not a new strategy. Live is the same CopyIntent body with execution
   permission added.

   Required proof:

   - same CopyIntent identity and body except mode, live flag, and approval id,
   - token mapping guard rejects wrong outcome-token mapping,
   - sizing, cap, order type, and policy are unchanged,
   - durable live lifecycle ledger exists for submitted, filled, rejected, and
     missed live orders,
   - explicit operator approval id is recorded before live mode can submit.

## Live-Ready Gates

Do not ask for wallet funding until all relevant gates pass.

Global gates:

- paper proof was collected with `paper_only=true`,
- `live_orders_allowed=false` during proof,
- WR >= 70%,
- validation WR >= 70%,
- ROI >= 2% and PnL positive,
- resolved orders >= 100,
- unique windows >= 10,
- average orders per window >= 2.0 for the target multi-wallet architecture,
- unresolved ratio below cap,
- no fallback BUY fills,
- no rejected BUY fills,
- no missed BUY copy events,
- no research-only resolution dependence for the admission claim.

Proof-led single-wallet gates:

- >= 10 distinct required BUY source events,
- >= 3 market windows,
- p95 event age <= 10 seconds,
- candidate proof is attached to current profit-engine ranked candidates,
- same source wallet and policy id in profit state and live-tracker truth.
- this is the primary first-live path, while background wallet copy continues.

Weighted multi-wallet inventory gates:

- at least two agreeing wallets in the same BTC-5m window,
- current-poll consensus, not only tracker-time replay,
- candidate-specific CLOB-backed CopyIntent lifecycle truth,
- positive train and validation performance,
- enough resolved paper orders and no hidden fallback path.
- this is an upgrade path, not a first-live prerequisite.
- live upgrade requires better or more reliable paper performance than the
  single-wallet live baseline.

## Heartbeat Workflow

Every autonomous heartbeat must begin from source-of-truth state, not stale
prompt text.

Primary command:

```bash
python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240
```

Then inspect or refresh:

```bash
python3 scripts/report_wallet_copy_wallets.py --top 20
python3 scripts/select_wallet_copy_strategy_direction.py
```

Mandatory heartbeat report fields:

- strategy direction and top lane,
- pipeline resume coverage,
- wallet coverage and high-source wallet copy quality,
- paper orders, fills, rejections, resolved orders, PnL, ROI, WR, drawdown,
- current-poll CLOB proof: required, filled, fallback, rejected, missed BUYs,
- candidate runtime proof: source wallet, policy, windows, event age,
- live-readiness blockers,
- next progress action.

If the status is non-green, exactly one progress action must be present:

- bounded safe fix,
- sharper measurement,
- code-level backlog item with file, function, and verification command.

## Current Live Architecture Target

The primary first-live architecture is single-wallet copy promotion:

```text
leaderboard/operator wallets
  -> paper CopyIntent copy per wallet
  -> rank by source profitability and our current copyability
  -> choose one best single wallet/policy
  -> prove paper profitability and CLOB-backed current-poll copy truth
  -> same CopyIntent live adapter after operator approval
  -> keep all other wallets copied in paper as backup candidates
```

The target upgrade architecture is weighted multi-wallet inventory by BTC-5m
window with a multi-wallet filter:

```text
wallet events
  -> same market window grouping
  -> same outcome agreement
  -> source wallet quality weights
  -> CopyIntent inventory plan
  -> CLOB-backed fillability check
  -> paper lifecycle
  -> profit/admission gate
  -> same CopyIntent live adapter after operator approval
```

The immediate development lane can still be single-wallet proof-led copyability
when it is the shortest path to the first live-ready candidate. That is not a
mission change; it is the primary live promotion path. The multi-wallet
architecture remains a parallel paper strategy and later upgrade path.

The research/development page for this architecture is:

```text
multi_wallet_copy_trader_inventory_builder
```

Its job is to watch the widest available weekly/monthly CRYPTO leaderboard
wallet universe, copy all observed BTC-5m BUY flow into paper through
CopyIntent, rank wallets by source profitability and current copyability, and
build weighted inventory candidates in every eligible window. If results are
not improving toward live-ready gates, the heartbeat must initialize a concrete
change: expand or resume coverage, re-rank the wallet universe, pin a better
copyable wallet for current-poll burn-in, rebuild the inventory search, or
write a code-level backlog item.

## What Must Stay Visible

These facts must never be hidden by a green summary:

- full/raw wallet-copy baseline,
- filtered candidate performance,
- fallback-filled order counts,
- rejected and missed copy events,
- unresolved and research-only labels,
- active versus seeded versus tracker-time replay evidence,
- candidate-source-wallet attribution,
- identity contamination or wrong token mapping,
- source wallet profitability versus our copied paper profitability.

## Done Definition

The system is done for live-money readiness only when:

- the live-readiness certificate says live-ready behind operator gate,
- profitability is proven by paper results under the gates above,
- current-poll candidate-specific CLOB-backed copy truth passes,
- paper/live parity passes through the same CopyIntent body,
- live lifecycle ledger and token mapping guard pass,
- blockers are empty,
- `paper_only=true` and `live_orders_allowed=false` remain true until the
  operator explicitly opens the gate.

Until then, the correct output is `ANALYZE` or `CORRECTION` with the blocker and
next fix, not a live-ready claim.

After the operator gate is opened and paper/live admission is still `PASS`, the
correct operational target is not another live-ready repair loop; it is a
healthy pinned single-wallet live guard:

- `data/research/wallet_copy_live_guard_state.json` reports
  `LIVE_GUARD_RUNNING`,
- the pinned candidate and source wallet match the approved single-wallet lane,
- live execution arm has no proof, intent, token mapping, or operator-gate
  blockers,
- the live lifecycle ledger records every submit/fill/reject,
- background wallet-copy and multi-wallet upgrade lanes remain paper-only until
  separately proven and operator-approved.

If paper/live admission later flips non-green, the correct operational target
becomes a blocked live gate:

- `data/research/wallet_copy_live_guard_state.json` reports
  `LIVE_GUARD_BLOCKED`,
- live execution arm reports `paper_only=true`, `live_orders_allowed=false`,
  and zero new submitted orders,
- profit engine and strategy direction show the blocker that prevents live
  submission,
- heartbeat/reporting must not describe live copy trading as running until
  those gates return to `PASS`.
