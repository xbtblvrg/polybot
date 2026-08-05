# Operator Package - 2026-07-07 14:15Z

Status: SHIPPED at the 14:15Z true-UTC package point.

Source contract:
- Latest authority: 2026-07-07T14:10Z Fable DIRECTION.
- Package order confirmed by 2026-07-07T12:36Z Fable DIRECTION.
- DATA LAYER v1, DR push, stale-inventory user-channel work, pulse-lock fix,
  and allocator live wiring are after this package, not before it.

## Opening Gaps

- LIVE trading-continuity gap: fresh digest at 2026-07-07T14:16:58Z shows
  windows_filled=83/288 and submitted=89/288 vs OP-VOLUME target >=144/288.
- LIVE daily-PnL gap: day_pnl_usd=-29.622921; since-topup canonical=-27.159952,
  actual=-24.910006, verdict=NOT_PRODUCING, recon=MISMATCH.
- Actual live trading is running: single guard pid=33390, can_trade=true,
  live_orders_allowed=true, latest ledger at inspection 1528 orders / 716
  fills / 812 rejects / 0 submitted, latest_order_ts=2026-07-07T14:06:16Z.

## Package Order

1. LIVE/PROMOTE E4 verdict + rotation/promotion on the adjudicated actual
   basis.
   - DECISION: NOT_PRODUCING. The 13:30Z -> 14:14Z loss path was
     -2.259790 -> -16.392599 -> -29.622921 day PnL.
   - Member attribution at the pre-ship read:
     - 0x927f...d215: -12.972974 day PnL, 98 orders / 59 fills / 39 rejects.
     - 0x251c...541f: -6.614222 day PnL, 84 orders / 57 fills / 27 rejects.
     - 0x8bc1...b473: -10.035725 day PnL, 24 orders / 15 fills / 9 rejects.
   - EXECUTED ROTATION: demoted 0x8bc1...b473 outright per Fable 14:10, and
     demoted 0x927f...d215 because it remained <= -12 and the latest toxicity
     artifact still names it as worst wallet.
   - KEPT: 0x251c...541f stays on watch; it is negative but still best
     economics of the three active live contributors.
   - BACKFILLS: runtime active set now includes queue backfills
     0x4d8b...a00d, 0xe290...a33f, and 0xe505...e3bf behind the same single
     live guard path. A fourth overlay candidate 0xa689...559b is recorded in
     the overlay but is beyond the current runtime cap.
   - Queue caveat: refreshed queue remains ready_for_live=0, queue_depth=10,
     positive_copy=104. This is a Fable-directed emergency rotation, not a
     lowered evidence-gate claim; ranked-queue refresh remains required after
     package.

2. LIVE toxicity-aware pricing decision + proof-of-fire status.
   - Current artifact: wallet_copy_fill_toxicity_latest.json generated
     2026-07-07T11:58:30Z.
   - Verdict=TOXIC_FILLS: active-set historical signals ROI=+3.527525% over
     3976 resolved signals, but live fills ROI=-1.210696% over 111 resolved
     live fills; toxicity=-4.738221 cents per $1 copied.
   - Worst wallet remains 0x927f...d215, supporting the E4 demotion.
   - No eligible denied-cell proof-fire was manufactured pre-ship.

3. LEARN decompiler predictability readout.
   - Current artifact: wallet_copy_strategy_decompiler_intake_latest.json
     generated 2026-07-07T11:45:51Z.
   - Intake summary: selected_wallets=10, eligible_wallets=12,
     events_scanned=60496, wallets_seen=33.
   - Decision: readout-only for this package; it does not override the
     live-loss rotation decision.

4. LIVE notification bundle.
   - Accounting wording from Fable 12:36, required:
     "equation-closed, |unexplained| = $0.0986 under the $1.00 threshold;
     24/62 candidate rows confirmed exact per-row duplicates of guard orders
     at 1e-6 tolerance; the remaining 38 show same-condition/outcome guard
     orders near event time with size deltas consistent with order-to-fill
     slicing (36/38 order >= fill)".
   - Do not say "all 62 confirmed duplicates".
   - +27.13 never appears as missing PnL.
   - Current canonical-vs-actual gap is about $2.25 at the 14:16 digest, while
     self-feed/ledger classification still flags the true-unrecorded class as
     P0 input for tonight's DR + DATA LAYER v1.
   - b2 tripwire remains armed: any confirmed on-chain fill with no guard-side
     evidence and no ledger row escalates immediately at any hour.

## Allocator Appendix

"Portfolio allocator: designed, synthetically audited, and real-arrival replay
PASS at both densest-100 and full-stream scale (3217 recorded shadow events
over ~13.6h; the simulated guard kept pace with arrivals - post-last-arrival
backlog drain 0.65s vs 47s gate, p95 wait 1.42s, max wait 3.50s, starved
members 0). Coverage limits stated plainly: the shadow stream carries only 8
unique members, so fairness at 288-member scale is unproven until the stream
itself widens; starvation was measured with paper-only zero allocation floors,
so the final pre-wiring replay will re-run with production floors. Live wiring
is slotted after tonight's DR and stale-inventory work, allocator as a pure
pre-guard transform with the single guard as sole submitter."

## After Package

- First: pulse-lock stale-holder fix from Fable 14:10 decision 5.
- Then: DR push + DATA LAYER v1.
- Then: user-channel WSS + own-impact monitor; this lane must name/fix the
  stale-inventory miss cause.
- Last: allocator live wiring, gated on production-floor replay in the same
  wiring commit.
