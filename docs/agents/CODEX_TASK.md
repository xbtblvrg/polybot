# Codex Task: Low-Latency Wallet-Copy Rebuild

STANDING MISSION: docs/agents/AUTONOMOUS_FLOW.md is the flow contract and
overrides this document on any conflict. This document remains as the
detailed spec for the detection/execution build items referenced by the
flow's build order.

## Mission

Replace Data-API polling as the copy trigger with real-time wallet-attributed
detection, so a profitable external wallet can be copied live within a 1-2s
end-to-end latency budget.

## Build order (do in sequence, each with measured evidence)

1. **Real-time detection layer** (`src/wallet_copy/realtime_feed.py`)
   - Primary: subscribe to Polymarket's realtime activity websocket
     (`wss://ws-live-data.polymarket.com`, the feed behind the site's
     /activity page). Emit wallet-attributed trade events for registered
     wallets with a receive timestamp.
   - Fallback + verification: Polygon WSS node (Alchemy/QuickNode),
     `eth_subscribe` logs on the CTF Exchange contract, decode OrderFilled,
     filter by maker/taker address. This also bypasses the Data-API
     route-reset problem.
   - Data API becomes backfill/reconciliation only, never the trigger.
   - Evidence: side-by-side latency log (WS receive time vs Data-API poll
     detection time) for the same fills, persisted to
     data/research/detection_latency_report.json.

2. **Alpha-decay wallet selection** (`src/wallet_copy/alpha_decay.py`)
   - For each candidate wallet fill, replay the CLOB book/WS capture and
     measure price 1s/2s/5s/30s after their fill.
   - Output per-wallet "copyable at latency L" expectancy. This becomes the
     primary admission metric alongside existing gates.
   - Evidence: per-wallet decay curves in data/research/alpha_decay_report.json.

3. **Fast execution path**
   - Pre-initialized ClobClient (cached API creds, persistent session,
     pre-fetched tick sizes / token metadata at market registration).
   - Marketable limit (FAK) with slippage cap vs the copied price.
   - Chasing guard: skip if book already moved > configured bps from the
     copied price at decision time.
   - Evidence: submit-latency histogram (decision -> order accepted).

4. **Single long-running runner** (`scripts/run_realtime_copy_runner.py`)
   - WS listener -> intent builder -> always paper ledger -> live adapter
     behind the existing guard. One process, one config.
   - Evidence: 24h paper run with copy-latency histogram and paper PnL.

## Operating mode: no idle loops

- THE BLOCKER CONCEPT IS RETIRED (operator, 2026-07-04). An obstacle is an
  OPEN DEFECT with a mandatory next action — never a resting state.
  Attempt at least 3 concrete alternatives with logged evidence; if the
  defect still stands, log it as "defect: <what> | attempts: <3+> |
  next: <concrete action>" and immediately continue with the next
  parallelizable task. Never wait idle, never leave a defect without a
  next action.
- Before picking your next task, read docs/agents/HANDOFF.md for the latest
  DIRECTION entry from the operator (fable) and follow it if present. The
  operator's DIRECTION entries override your own task ordering.
- Every milestone or session end: append a STATUS entry to HANDOFF.md with
  what was done, the verify commands you ran, and their results.
- For every change: python3 -m pytest -q and python3 -m compileall -q src
  scripts tests must pass.

## HANDOFF.md entry format

    ## [ISO timestamp] codex STATUS
    - done: ...
    - evidence: commands + key results
    - defect (if any): what | attempts: the 3+ alternatives with results |
      next: the concrete action that continues it
    - next: ...

## Hard limits

- Structural invariants: CopyIntent parity (live = same intents as paper)
  and the single live guard (scripts/run_wallet_copy_live_guard.py) as the
  only order submitter.
- Live operates under recorded operator decisions OP-LIVE-20260703-BELA and
  OP-AUTONOMY-20260703-BELA (see docs/agents/AUTONOMOUS_FLOW.md and
  LIVE_TODAY_SPRINT.md): promotion/rotation/threshold decisions are Fable's,
  logged with evidence in HANDOFF.md. You never change them unilaterally —
  ask Fable via ./scripts/ask_fable.sh.
