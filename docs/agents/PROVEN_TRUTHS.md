# Proven Truths Registry

Append-only registry of facts this system has proven with evidence. These
entries protect working knowledge from silent overwrite; they do not restrict
new hypotheses. Each truth is scoped to its market, method, latency regime, and
wallet set. A truth may only be superseded by a later ruling that names the old
entry and cites stronger evidence.

Format per entry:
- truth: the fact that is proven.
- scope: the exact regime where the truth binds.
- evidence: artifact/date/sample or ledger reference.
- guard: the mechanism that keeps the system from silently violating it.

## 2026-07-11 Seed Entries

1. truth: The 60s freshness gate is the live copy profit switch.
   scope: BTC-5m wallet-copy member policies under the current Data-API/poller
   latency regime.
   evidence: first green live day credited to the freshness-gated lane; see
   HANDOFF entries around 2026-07-04 and the live ledger/day scorecards.
   guard: member policies plus FLOW TRUTH in docs/agents/AUTONOMOUS_FLOW.md.

2. truth: Sub-25c longshot copies are toxic in the measured copy regime.
   scope: BTC-5m wallet-copy fills in sub-25c price cells under current live
   execution latency and sizing.
   evidence: toxicity audits found -100% cells; see the regenerated toxicity
   denylist and state digest sub25/toxicity sections.
   guard: toxicity denylist regenerated with tightened criteria before
   promotion decisions.

3. truth: Naive taker-chase is dead at the measured latency.
   scope: copy-all taker-chase for BTC-5m at current infrastructure latency,
   not future sub-second infrastructure or other market structures.
   evidence: alpha-decay branch B over roughly 54k events showed negative
   copyability for naive chasing; see preregistration and alpha-decay artifacts.
   guard: selective-lane doctrine and experiment preregistration records.

4. truth: Selective copy using freshness, cells, and wallet selection can
   produce green live days.
   scope: BTC-5m wallet-copy selected lanes using the current live guard and
   member policies.
   evidence: two frozen green days and the golden snapshot recorded in HANDOFF
   and live ledger artifacts.
   guard: golden snapshot plus weekday seat admission and rotation rules.

5. truth: Weekend regime differs materially from weekday regime.
   scope: BTC-5m wallet-copy wallets with observed day-of-week profitability
   splits and weekend roster actions.
   evidence: 2026-07-11 live evidence and weekend roster alignment rulings for
   0xe6db, 0x5e4a, 0xac05, 0xe476, 0x960b, and 0x3725.
   guard: regime labels, temporal profitability state, and schedule-dont-fire
   bench/return logic.

6. truth: Paper fills are flatter than live fills.
   scope: BTC-5m wallet-copy paper twin versus actual live fill accounting
   under current execution and pricing assumptions.
   evidence: measured optimism gap in paper-twin/control and account-value
   residual audits.
   guard: paper-twin control plus our-fill pricing doctrine before promotion.

## 2026-07-24 Causal Shadow Ruling

7. truth: The 0.50-0.70 entry price band gate has positive causal attribution.
   scope: BTC-5m copy-trading live guard f418 execution policy.
   evidence: f418_post_band_gate_residual_loss_causal_shadow_latest.json on 2026-07-24 showed that the accepted-live cohort earned +$2.844858 (33 resolved rows) while the denied-counterfactual cohort lost -$3.454987 (13 resolved rows). Chronological holdout aggregate is -$0.555644 (10 rows) and train aggregate is -$0.054485 (36 rows), confirming that the raw copy-trading stream is negative-expectancy and the band gate successfully filtered out the toxic sub-cohort.
   guard: live execution policy fast_wf_0.10_cap_4_all_prices_minusd_0_all_window active on f418.
