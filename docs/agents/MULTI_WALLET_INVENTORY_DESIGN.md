# Multi-Wallet Inventory Design

Status: TRACK2 paper/proof design, not a live execution lane.
Updated: 2026-07-03

## Operating Contract

Track 1 remains the only live lane: a single mission-pinned wallet copied through `scripts/run_wallet_copy_live_guard.py`.
Track 2 may discover, observe, score, and package candidates, but it must not place live orders, start a second live runner, flip `live_orders_allowed`, or write mission changes.

Any Track 2 promotion requires an evidence package plus an explicit operator/Fable decision that updates `src/wallet_copy/mission.py`, strategy state, and heartbeat wording together.

## Inputs

- Leaderboard discovery state: `data/research/wallet_copy_leaderboard_scan_state.json`
- Leaderboard discovery events: `data/research/wallet_copy_leaderboard_scan_events.jsonl`
- Realtime wallet events: RTDS/VPN JSONL capture outputs
- Onchain fill evidence: Polygon WSS first, HTTP `getLogs` fallback when WSS is unavailable
- Market context: Gamma current BTC 5m token mapping plus CLOB `/book` snapshots
- Existing wallet-copy paper/proof states and CopyIntent ledgers

## Lane Design

1. DISCOVER
   - Run the hourly LaunchAgent-backed leaderboard scanner as a paper-only writer.
   - Preserve the last known wallet registry when a route returns degraded 5xx/503.
   - Record VPN/egress preflight and retry evidence so route failure is measurable, not silent.

2. OBSERVE
   - For each candidate wallet, persist wallet-attributed events into the same CopyIntent model used by Track 1.
   - Prefer RTDS/Polygon timing for trigger evidence; use Data API only for reconciliation/backfill.
   - Keep all outcomes paper-only until an operator promotion updates the mission.

3. SCORE
   - Score wallets by copyability at our measured latency, not only historical win rate.
   - Required features: event freshness, CLOB book availability, rejected/fillable quote reason, alpha decay after 1s/2s/5s/30s, effective copied limit, and market resolution class.
   - Penalize candidates whose edge disappears before the guard can submit.

4. PROMOTE
   - Write a candidate evidence package containing wallet id, policy id, realtime rows, CLOB snapshot rows, paper CopyIntent outcomes, alpha decay, and live-readiness deltas versus the current pinned lane.
   - Do not modify mission state from this lane.

5. GUARD INTERACTION
   - The live guard consumes only the mission-pinned lane.
   - Track 2 outputs are advisory paper/proof artifacts until promoted by operator/Fable.

## Acceptance Criteria

- Discovery can run unattended as a single paper-only LaunchAgent job.
- Preserved wallet inventory remains usable during source-route degradation.
- At least one candidate package can be produced without enabling live execution.
- Candidate packages include enough evidence to decide whether rotation is safer than staying on Track 1.
- No Track 2 command can place or arm a live order.

## Timebox And Escalation

- If discovery route is degraded for two heartbeats, continue from the preserved registry and narrow to the most recently active wallets.
- If no paper CopyIntent evidence appears for the narrowed set within two active cycles, refresh token mapping and switch reconciliation to HTTP `getLogs` fallback.
- If a non-pinned wallet beats the pinned lane on latency, fillability, and alpha decay, prepare a rotation package and stop before mission mutation.
- If no candidate beats the pinned lane, keep Track 1 live and use Track 2 only to improve inventory freshness.

## Immediate Next Actions

1. Continue hourly leaderboard discovery and inspect preserved-wallet coverage.
2. Build the first paper-only narrowed candidate package from the preserved registry.
3. Feed package metrics into strategy selection without changing live mission state.
