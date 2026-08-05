# Wallet-Copy-Only Reset

Date: 2026-06-24

## Decision

The repo is reset around BTC 5-minute wallet-copy trading. Prior attempts based
on direct signals, autonomous ML promotion, fair-value selection, broad
replacement supervisors, and SOL live operation did not establish a reliable
live profit path. They must not be treated as active strategy authority.

The primary live-profit hypothesis is now:

1. identify externally profitable BTC 5-minute wallets,
2. ingest their complete relevant Polymarket history,
3. mirror wallet actions 1:1 in paper with full lifecycle tracking,
4. compare wallets across the same market windows,
5. build consensus or hedged inventory when multiple wallets agree,
6. train and reverse-engineer only as a research aid,
7. enable live only when it sends the exact same intents paper has already
   generated and tracked.

This is intentionally narrower than "build a good trading bot." The code-level
goal is to find and validate a profitable BTC 5-minute direction by copying
external wallet orders. If an analysis, feature, model, or automation does not
improve wallet-order copying, copyability, copy-efficiency, lifecycle tracking,
or profit admission for that scope, it is research-only or out of scope.

## Source Of Truth

The canonical new implementation lives under `src/wallet_copy/`.

The machine-readable objective is `src/wallet_copy/mission.py`; audit and
autonomous repair states must include its `mission_contract` so a heartbeat
cannot silently drift back to SOL, direct ML, fair-value, or generic strategy
authority.

The first seed wallet is Weird-Peak:

```text
0x9f5ffe76a818dce37c70f947998b52b70671a008
```

The existing Weird-Peak exact-copy paper flow is preserved because it already
contains useful wallet attribution, history replay, lifecycle, and low-latency
fast-preconfirm machinery. New generic wallet-copy code wraps and extends it
instead of reviving old ML/direct live runners.

## What Is Legacy

The following are legacy or secondary research unless fresh source-of-truth
paper/live evidence proves them useful:

- direct BTC/SOL ML live runners,
- all SOL-only or SOL-live operation,
- autonomous replacement/watchdog promotion as primary strategy authority,
- fair-value and signal-driven strategies,
- broad paper inventory experiments not tied to wallet-copy evidence,
- old cap-ramp authority from stale prompt fragments.

## Paper/Live Contract

Paper and live must share the same `CopyIntent` contract.

Paper mode:

- writes every order lifecycle event,
- fills according to an explicit paper fill model,
- records wallet attribution and source event identity,
- never submits live orders.

Live mode:

- may only adapt the same `CopyIntent`,
- requires explicit operator go,
- requires `live_orders_allowed=true`,
- requires dry-run off,
- requires CLOB token IDs,
- may not create a separate strategy decision path.

## Research Contract

Research is allowed and expected, but it is subordinate:

- train rows come from normalized wallet events,
- cross-analysis groups multiple wallets by market/window/outcome,
- consensus candidates require multiple wallets agreeing on the same side,
- losing full/raw copy slices remain visible and cannot be hidden by filtered
  candidate labels.

## Cleanup Rule

Do not delete historical data that is useful for attribution, accounting, or
postmortem analysis. Quarantine old strategy authority in docs and new entry
points first; remove files only when their replacement and tests are present.
