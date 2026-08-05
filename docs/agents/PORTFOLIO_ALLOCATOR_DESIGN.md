# Portfolio Allocator Design

Flow stages: LIVE / PROMOTE / SELF-DEV.

Purpose: support the mass-copy target state where many vetted members can fire
in the same window without one member consuming all cash or exposure. The
allocator is a pure CopyIntent transform; it does not submit orders and does
not change the single live guard invariant.

## Inputs

- CopyIntents from vetted members, already passed by their copy recipes.
- Member scores from the full-universe copyability / promotion surface.
- Spendable cash, reserve cash, current open exposure, and recycle credits
  expected to become spendable inside the next 300 seconds.
- Per-cycle constraints: minimum tranche, optional per-member cap, optional
  concurrent book cap.

## Rule

1. Compute portfolio budget:
   `available_cash - reserve + recycle_credits_within_300s`, capped by
   remaining concurrent book room when configured.
2. Group intents by member id.
3. Give each eligible member a minimum floor allocation in score order when
   cash can fund that floor.
4. Allocate the remaining budget score-weighted pro-rata across unmet demand.
5. Split each member's allocation across its intents, preserving CopyIntent
   parity and recording `metadata.portfolio_allocation`.
6. Emit deferred rows for members that could not receive a valid tranche; those
   rows are evidence for the throughput/capital decision, not silent skips.

## Live Adoption Gate

The allocator is live-ready as a deterministic build component after tests, but
it should be wired first into a paper shadow lane. Live adoption requires a
Fable direction citing measured cycle timers and no parity drift versus the
same paper intents.

## Throughput Audit

`scripts/audit_wallet_copy_dispatch_throughput.py` synthesizes a 100-member
burst, runs the allocator, and estimates drain time under the single guard's
serial dispatch loop. It reports cycle count, API request rate, fairness, and
whether all members receive a floor allocation when cash is sufficient.
