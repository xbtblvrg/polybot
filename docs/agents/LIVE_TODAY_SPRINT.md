# LIVE-TODAY Sprint — 2026-07-03

## Operator approval (human decision, recorded)

The human operator (Béla, 2026-07-03) has approved live promotion of the
single-wallet copy runner TODAY. Approval id: OP-LIVE-20260703-BELA.
By explicit operator decision there are NO operator-imposed caps, no daily
loss stop, no auto-disarm, no halt conditions. Sizing follows the existing
copy sizing policy config. The agents arm the existing live guard through
the guarded CopyIntent path as soon as the pipeline functions end-to-end.
This entry is the human live-promotion decision required by the framework.

## What "functions end-to-end" means (build requirements, not gates)

1. DETECTION LIVE: realtime wallet-attributed feed running under VPN (RTDS
   and/or Polygon WSS) for the target wallet, feeding intents. Persist a
   latency sample so we know what lag we copy at.
2. EXECUTION LIVE: pre-initialized ClobClient submitting FAK marketable
   limit orders through the guarded CopyIntent path. The slippage cap and
   chasing guard stay — they are profitability mechanics (they stop us
   buying worse than the copied price), not halt conditions.
3. WALLET CHOICE: copy the best-evidenced active wallet. Use alpha-decay /
   book-replay evidence where it exists to pick, but missing evidence does
   not block going live — it just gets noted in the STATUS entry.
4. PAPER SHADOW: keep the paper lane running in parallel on the same
   intents, so live vs paper drift is measurable while live runs.

## Operating rules for today

- Codex: work the requirements top-down, arm live as soon as 1+2 function.
  A blocker gets 3 documented attempts, then ./scripts/ask_fable.sh
  immediately. No monitoring-only heartbeats, no parking, no settling.
- Fable: unblock every BLOCKED the same heartbeat with concrete direction.
  Do not accept resignation. Do not reintroduce caps, stops, or halt
  conditions — the operator has explicitly declined them. Audit that
  claimed functionality is real (orders actually submitted, fills actually
  reconciled); functional truth still matters because fake-live loses the
  whole point.
- LIVE = detection feeding intents + live orders flowing through the guard
  with approval id OP-LIVE-20260703-BELA. Report a LIVE STATUS entry with
  the latency sample, first fills, and live-vs-paper comparison, then keep
  operating and improving fill quality.
- The only remaining hard limits are the framework's structural ones:
  CopyIntent parity (live = same intents as paper) and the single live
  guard as the only order submitter. These are what make the system
  debuggable, not risk brakes.
