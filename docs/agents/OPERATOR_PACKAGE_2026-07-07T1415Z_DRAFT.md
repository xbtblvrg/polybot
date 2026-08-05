# Operator Package Draft - 2026-07-07 14:15Z

Status: FINAL SNAPSHOT prepared from the 2026-07-07T14:11:19Z digest for the
14:15Z true-UTC ship point. Do not ship early from local-time-stamped HANDOFF
entries.

Source contract:
- Latest authority: 2026-07-07T14:10Z Fable DIRECTION.
- Package order confirmed by 2026-07-07T12:36Z Fable DIRECTION.
- DATA LAYER v1, DR push, stale-inventory user-channel work, and allocator
  live wiring are after this package, not inside this package.

## Opening Gaps

- LIVE trading-continuity gap: current digest at 2026-07-07T14:11:19Z shows
  windows_filled=83/288 and submitted=89/288 vs OP-VOLUME target >=144/288.
- LIVE daily-PnL gap: day_pnl_usd=-29.622921; since-topup canonical=-27.159952,
  actual=-24.910006, verdict=NOT_PRODUCING, recon=MISMATCH.
- Actual live trading is running: single guard pid=33390, can_trade=true,
  live_orders_allowed=true, latest ledger at inspection 1528 orders / 716
  fills / 812 rejects / 0 submitted, latest_order_ts=2026-07-07T14:06:16Z.

## Package Order

1. LIVE/PROMOTE E4 verdict + rotation/promotion on the adjudicated actual
   basis.
   - Binding constraints: continuity remains 83/288 vs 144/288 target, and
     day PnL is -29.622921 on the fresh 14:11Z digest/scorecard snapshot.
   - E4 decision executed per Fable 14:10Z: 0x927f...d215 demoted by threshold
     (-12.972974, 98 orders, 59 fills) and 0x8bc1...b473 demoted outright
     (-10.035725, 24 orders, 15 fills, worst per-fill economics/no incremental
     production).
   - Guard-active backfills are 0x4d8b...a00d / rtds_live_band_529b8ae1a00d
     (paper_pnl_usd=0.861238, copyable_buy_events=132, resolved_orders=125),
     0xe290...a33f (paper_pnl_usd=4.429266, copyable_buy_events=128,
     resolved_orders=91), and 0xe505...e3bf (paper_pnl_usd=1.520861,
     copyable_buy_events=37, resolved_orders=37).
   - Single guard picked up the overlay by 2026-07-07T14:17:42Z; active set
     count is 8 and both 0x927f...d215 and 0x8bc1...b473 are absent. No second
     submitter or guard reload.
   - Use verified full-universe scoring:
     registry_wallets=16370, wallets_scored=16410, positive_copy_pnl_wallets=104,
     ranked_queue_depth=10, staged/unbounded queue depth from digest=139.
   - Top queue examples for the 14:15Z decision pass:
     - rtds_live_band_dcb8ddd1541f: paper_pnl_usd=48.800815,
       copyable_buy_events=40, resolved_orders=33, copyability_score=150.196417.
     - rtds_live_band_28d1c141d215: paper_pnl_usd=68.043715,
       copyable_buy_events=46, resolved_orders=42, copyability_score=100.586505.
     - rtds_live_band_529b8ae1a00d: paper_pnl_usd=0.861238,
       copyable_buy_events=132, resolved_orders=125, copyability_score=40.461238.
   - Do not lower evidence gates; drain queue per verified evidence.

2. LIVE toxicity-aware pricing decision + proof-of-fire status.
   - Current artifact: wallet_copy_fill_toxicity_latest.json generated
     2026-07-07T11:58:30Z.
   - Verdict=TOXIC_FILLS: active-set historical signals ROI=+3.527525% over
     3976 resolved signals, but live fills ROI=-1.210696% over 111 resolved
     live fills; toxicity=-4.738221 cents per $1 copied.
   - Guard-side toxicity gate is enabled with 5 configured cells, but the
     fresh guard state has input_intents=0 and blocked_intents=0 for the gate.
     Proof-of-fire is therefore input starvation, not a fired reject.

3. LEARN decompiler predictability readout.
   - Current artifact: wallet_copy_strategy_decompiler_intake_latest.json
     generated 2026-07-07T11:45:51Z.
   - Intake summary: selected_wallets=10, eligible_wallets=12,
     events_scanned=60496, wallets_seen=33.
   - Package should state whether decompiler output changes rotation or stays
     readout-only pending the next modeled rule pass.

4. LIVE notification bundle.
   - Accounting wording from Fable 12:36, required:
     "equation-closed, |unexplained| = $0.0986 under the $1.00 threshold;
     24/62 candidate rows confirmed exact per-row duplicates of guard orders
     at 1e-6 tolerance; the remaining 38 show same-condition/outcome guard
     orders near event time with size deltas consistent with order-to-fill
     slicing (36/38 order >= fill)".
   - Do not say "all 62 confirmed duplicates".
   - +27.13 never appears as missing PnL.
   - Current recon remains MISMATCH: raw actual-vs-expected gap is +$2.249946,
     point-in-time adjusted gap is +$26.330610, and unjoined_actual_gap_usd is
     $24.080664. p0_guard_fill_recording_audit_required remains a tonight DR +
     DATA LAYER priority; the 12:26 aggregate closure is not reopened here.
   - b2 tripwire remains armed: any confirmed on-chain fill with no guard-side
     evidence and no ledger row escalates immediately at any hour.

## Allocator Appendix

Use Fable 13:27 final wording without softening caveats:

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

Fable 14:10 ordering after package:
- Pulse-lock stale handling first, before DR push.
- DR push + DATA LAYER v1 next.
- Then user-channel WSS + own-impact monitor; this lane must name/fix the
  stale-inventory miss cause (latest taxonomy stale=15 in Fable audit).
- Allocator live wiring last, gated on production-floor replay in the same
  wiring commit.
