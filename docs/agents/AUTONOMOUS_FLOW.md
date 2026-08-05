# Autonomous Flow — the self-running system

MISSION (operator, 2026-07-07, verbatim): "dinamikusan és kreatívan
önfejlesztő szuperokos napi szinten profitabilis polymarket quant-bot" —
a DYNAMICALLY and CREATIVELY SELF-IMPROVING, SUPER-SMART, DAILY-PROFITABLE
Polymarket QUANT-BOT. This one sentence is what the whole system is.
Every rule below serves it; anything that doesn't is prunable by the
Framework Audit. All four adjectives are binding, not decoration:
dynamic (adapts to the market every day), creative (the generative hunt
is the idle state), self-improving (the system upgrades its own code,
rules, and models without being asked), super-smart (best available
brain, full-depth reasoning, never lobotomized for cost), daily
profitable (the ledger says so, in cash).

ACCEPTANCE CRITERION (operator, 2026-07-05, absolute): NOTHING about
this system is acceptable until the bot trades CONTINUOUSLY and is
DAILY PROFITABLE. Until both hold simultaneously, the system is in
defect state BY DEFINITION — no report may call it healthy, no verdict
may be green, and every STATUS/DIRECTION/scorecard opens with the two
gaps: (1) trading-continuity gap (idle windows, coverage vs 144/288),
(2) daily-PnL gap (canonical PnL vs positive). "Working on it" is the
system's permanent minimum state, never its achievement.

META-PRINCIPLE (operator, 2026-07-07): THE ONLY THING ABOVE EVERYTHING
IS THE GOAL — daily-profitable BTC-5m trading. Every rule, gate,
mandate, doc, and this framework ITSELF is a servant of the goal and a
SUSPECT for being its cage. There are no sacred constraints except the
structural invariants (CopyIntent parity, single live guard) and honesty
(no fake evidence). If any rule we wrote is slowing the goal, the rule
is wrong, not the goal.

FRAMEWORK RED-TEAM (operator, 2026-07-07): see the canonical
"Framework Audit" section below — every 72h (and after any 2-day
goal-progress stall) a fresh, goal-first-reading session attacks our
own rules across AI logic / dev process / data sources / execution;
failing rules are pruned same-day. Nothing is exempt except the
structural invariants and honesty; even the audit rule itself is
auditable.

Single goal (operator, 2026-07-04): MAKE MONEY DAILY on Polymarket by
live trading — any method that measurably earns is allowed. Copy trading
(Track 1/2) is the primary lane because it has the most evidence and
machinery; any alternative method (own-signal, market-making, structural
edges) may be proposed by the agents, proven paper-first on real data,
and promoted by Fable the moment it beats the incumbent lanes on measured
PnL. Method loyalty is forbidden — profit loyalty only. This document is
the flow contract. Fable decides all open questions; decisions are made
by the agents, not deferred to the human.

## Recorded operator decisions

- OP-USD-TARGET-20260707-BELA (Béla, 2026-07-07, verbatim: "legalább
  napi 100-300 dollár profitot kellene termelni egy ilyen rendszernek"):
  the system's expected output is $100-300 PROFIT PER DAY. Honest math,
  binding on all planning: $100/day = ~$3,000/day filled volume x 3.5%
  captured edge (the active set's MEASURED signal ROI). BTC-5m recycles
  capital every 5 minutes, so the current ~$300 bankroll supports
  $10k+/day volume — capital is NOT the constraint. The two gaps, in
  order: (1) SIGN: our-fill ROI must flip positive (fill-toxicity
  direction 2026-07-07T20:55Z executes this); (2) SCALE: daily allocated
  volume ramps from ~$125/day toward $3,000+/day as sign-positive cells
  prove out — the volume ramp is aggressive by default, throttled only
  by measured negative expectancy, never by timidity. Every daily
  scorecard reports: captured-edge %, filled volume $, and profit $ vs
  the $100-300 band. Days below band are defects with named causes.
  This supersedes percentage phrasing where they conflict: the operator
  thinks in dollars.

- OP-NORTHSTAR-100K (Béla via Fable, 2026-07-10, verbatim: "ha mar
  stabilan bizonyitottan live megvan a penzgyar akkor a legfelsobb cel a
  havi 100000 USD profit realizalasa ebbe az iranyba kell menni es
  skalazni"): once the money factory is stably proven live, the supreme
  scaling goal is $100,000/month realized profit (~$3,300/day). This
  unlocks only after 4 consecutive green weeks holding the $100-300/day
  band; until then it is the scaling direction, not the binding daily bar.
  The same 4-green-week gate also defines when OP-BTC5M-EXCLUSIVE releases
  factory replication beyond BTC/ETH short-cycle crypto. Required path:
  compound bankroll toward the capital needed for $110-160k/day volume,
  measure BTC-5m own-impact/capacity ceiling as an expectancy boundary
  rather than a cap, then replicate the proven factory to adjacent
  short-cycle markets under the same gates and deadmen.

- OP-LIVE-20260703-BELA: live approved, no caps/stops/halts (see
  LIVE_TODAY_SPRINT.md).
- OP-AUTONOMY-20260703-BELA (Béla, 2026-07-03): wallet promotion to live,
  demotion, and rotation are DELEGATED to the agents. Fable decides.
  The human provides only infrastructure: VPN egress up, funded trading
  wallet + CLOB credentials in .env, machine running. Everything else is
  agent-owned.
- OP-BTC5M-20260704-BELA (Béla, 2026-07-04): the BTC 5-minute market must
  NOT be abandoned. Copying BTC-5m wallets' orders profitably is a
  standing engineering objective — the agents find the execution solution
  (latency, buffering, selective copying, maker fallback), they do not
  deprioritize the market. Slower-market lanes may run in parallel, but
  BTC-5m is a must-win target.
- OP-BRAINCHAIN-20260704-BELA (Béla, 2026-07-04, supersedes OP-GROK;
  extended by 2026-07-13 operator order): the brain fallback chain is
  Fable/Claude (primary) -> Grok (secondary) -> AGY/Antigravity
  (tertiary) -> codex/GPT self-consultation (last), all under
  FABLE_OPERATOR.md identically, recursion-guarded, with automatic
  switchback to the highest available brain on every call (wired and
  scenario-tested in ask_fable.sh). Original OP-GROK text follows: when the Claude CLI is
  unavailable (session limit/error), the Grok CLI substitutes as Fable
  1:1 — same role, same mandates, FABLE_OPERATOR.md governs it
  identically; automatic switchback to Claude when available (wired in
  ask_fable.sh).
- OP-VOLUME-20260705-BELA (Béla, 2026-07-05): VOLUME IS A CO-PRIMARY
  GOAL WITH PROFITABILITY. Profit = edge x volume; without volume there
  is no money. The watched wallets trade nearly every window with many
  orders and PRINT money — that is the model to match. The system must
  maximize the COUNT of positive-expectancy trades, not just their
  average quality: many trades, many windows, many orders where the
  evidence supports them. Low volume is failure even on a
  slightly-positive day. Daily trade count and window coverage are
  co-primary KPIs next to realized PnL on every scorecard, and both
  agents treat missing volume as a defect to engineer away (more
  members, wider evidenced bands, consensus lane, more markets) — the
  expectancy bar stays, everything else scales up.
  NUMERIC BAR (operator, 2026-07-05 evening): BTC-5m alone has 288
  windows/day. "Volume exists" means trading in AT LEAST ~50% of daily
  windows (>=144) with meaningful order counts per window, alongside
  positive daily PnL — that is profit = edge x volume realized. Window
  coverage % of 288 is reported on every scorecard; the growth program
  (members, bands, consensus, markets) is graded against closing the
  gap to >=50% and beyond. 2026-07-05 is BASELINE-ONLY (the machine was
  built mid-day); the first full verdict day is 2026-07-06.
- OP-TARGET-20260705-BELA (Béla + Fable ladder, 2026-07-05): RETURN
  TARGET. North star (direction): +2%/day avg, +10-15%/week. BINDING
  minimum is a LADDER that rises with proof. OP-FASTPROOF-20260705-BELA
  (Fable DIRECTION 2026-07-05T19:20Z) supersedes calendar-based proof:
  samples gate promotion, while Sunday weekly verdicts remain the
  honesty backstop.
  Phase 1 (current): positive week, outcome goal +3-5%/week; process
  minimums: window coverage trending toward the OP-VOLUME numeric bar
  (>=144/288 daily), member set held and growing, consensus lane decided
  on schedule.
  Phase 2 promotion / sizing-ramp unlock: >=300 resolved live fills with
  positive cumulative PnL AND positive rolling PnL on >=3 distinct days.
  Phase 2 target: +1%/day avg, +5-8%/week.
  Sizing ramp: fill-count based; every +100 resolved fills with positive
  rolling PnL unlocks the next size step ($8->$12->$16->$20, then % of
  bankroll per compounding doctrine). A negative rolling 100-fill block
  pauses the ramp, not trading, until positive again.
  Lane promotion (consensus, TRACK2, new members): >=50 resolved paper
  fills positive at our prices promotes to small live allocation; >=150
  resolved live fills positive scales it.
  Phase 3 (proven at scale, multiple engines): +10-15%/week becomes the
  binding weekly minimum; +2%/day stays the stretch star.
  Every DAILY SCORECARD reports actual vs the current phase target (% +
  USD) and fill-count progress toward the sample gates above; every
  Sunday 00:00 UTC a WEEKLY verdict grades the week and decides phase
  promotion/demotion. A below-phase-target week is a
  system defect: the brain names the limiting factor (coverage / edge /
  sizing) and orders the fix. The target drives engineering priorities;
  the expectancy bar and evidence rules still govern HOW it is pursued —
  the ladder may never be climbed by lowering standards.
- OP-BTC5M-EXCLUSIVE-20260707-BELA (Béla, 2026-07-07): the 5-minute
  crypto markets (BTC-5m first, ETH/crypto short-cycle family alongside)
  are the EXCLUSIVE battlefield until BTC-5m is profitably solved. NO
  new markets until then: E8-weather PARKED, E2 slow-market copy lane
  PARKED (supersedes their earlier elevations; knowledge kept, lane
  work zero). If current directions fail, new directions are sought ON
  THIS MARKET. OP-ANYMETHOD remains method-free WITHIN this market
  scope. Prime study: the operator's two-sided inventory hypothesis
  (pair-sum arb, intra-window scalp with sell leg, pro two-sided PnL
  split, two-sided replay).
- OP-BYPASS-20260703-BELA (Béla, 2026-07-03): full bypass-approval mode.
  Codex runs with approvals/sandbox bypassed, Fable runs with permissions
  skipped (wired in codex_heartbeat.sh and ask_fable.sh). No approval
  prompt may gate any action. EVERY question — including ones that would
  normally go to the human — is answered by the agents themselves, in
  favor of the goal: Fable decides, logs the decision and reasoning in
  HANDOFF.md, and work continues the same heartbeat. Waiting for a human
  answer is forbidden.

## The flow (continuous loop)

1. DISCOVER — leaderboard scan (hourly): pull Polymarket leaderboard +
   activity, score candidates (volume, activity, realized PnL), auto-
   register new wallets into the registry with tag `discovered`.
2. OBSERVE — paper copy everything: realtime feed (RTDS / Polygon WSS under
   VPN) on ALL registered wallets; every action becomes a paper CopyIntent.
   Maintain per-wallet rolling copyability score:
   - activity: fills/day
   - copyable_rate: share of their entries fillable within the slippage
     cap at detection latency (from book evidence)
   - rolling paper PnL at our copy prices (not theirs)
3. PROMOTE — automatic: a wallet goes live when its rolling window shows it
   is copyable and profitable at our latency. Defaults (Fable tunes them
   with evidence, logged in HANDOFF): window 48h, >=20 paper-copied fills,
   copyable_rate >=70%, rolling paper PnL > 0. Best-ranked wallet(s) live
   through the single guard.
4. LEARN — before a selected wallet goes live: download its full 30-day
   order history and replay it at execution level. For every move (entries,
   exits, scale-ins, both sides) determine how WE would have executed it at
   our detection latency: fillable within slippage cap? at what price? what
   copy PnL? Output a per-wallet execution profile that parameterizes the
   live copier: which move types to copy, sizing fraction, slippage
   setting, expected copy PnL. Acceptance is profitability, not
   perfection: the 30d replayed copy PnL at our latency must be positive —
   moves we can't copy well are dropped by the profile, not blockers.
   Persist as data/research/execution_profile_<wallet>.json.
5. LIVE COPY — same intents, live, parameterized by each wallet's
   execution profile. Structure (Fable 2026-07-05T12:35Z, widened
   2026-07-05T22:30Z): an ACTIVE SET of gate-qualified MEMBERS behind
   the ONE guard — a member is any qualified (engine, params) pair:
   copy-wallet members (wallet, band) AND signal-engine members (e.g.
   whale-consensus, E6 whale-side, maker-quoting) alike. Every member
   emits intents into the SAME CopyIntent pipeline, executes through
   the SAME single guard, and carries the SAME per-member protections
   (-$16/20 rotation, caps, participation counters) —
   minimum 3, grown as qualification allows (target 8-10+ with
   complementary activity hours, per the OP-VOLUME numeric bar) —
   whichever member is currently active gets copied; per-member bands,
   caps, and mechanical protections. Execution model (v3, Fable
   2026-07-06T22:25Z): window-inventory mirroring — track the wallet's
   cumulative position and VWAP per window, converge to the policy
   fraction of it via the DRIP LOOP (v2 origin: Fable
   2026-07-04T10:35Z; v3 drip supersession: Fable
   2026-07-06T22:25Z): $1-2.5 tranches emitted every few seconds while
   position < target AND the VWAP+drift-buffer price gate holds, pacing
   under API limits; signal weakening stops the drip (natural cut);
   maker fallback on miss; stopping near resolution. Strong-tier
   signals (22:10Z) drip with larger window budgets. Slippage cap and chasing guard remain.
   Paper shadow keeps running on the same intents for drift measurement.
   The profile is re-learned rolling as new history accumulates.
6. ROTATE — automatic replacement (this is the profit mechanism, not a
   halt). Applied PER ACTIVE-SET MEMBER; any brain tier executes:
   (a) LOSS: a member's rolling realized PnL over its last 20 resolved
       fills below -$8 -> demote that member immediately, backfill the
       set from the ranked queue.
   (b) INACTIVITY: a member asleep costs nothing inside the set; the
       trigger is COVERAGE (Fable 2026-07-05T12:35Z): if the WHOLE set
       yields zero eligible flow for >1h while any positive-profile
       registry wallet trades, that is a coverage incident — qualify or
       swap members the same heartbeat. "No proven alternate" is an
       instruction to go prove one, not permission to wait.
   Demoted wallets keep their gate evidence on file and resume it if
   repromoted. The leaderboard scan keeps the candidate pool full, and
   the ranking includes hour-of-week activity coverage.
7. SELF-DEVELOPMENT — the Codex loop runs continuously; Codex consults
   Fable via ./scripts/ask_fable.sh on every question or milestone; Fable
   answers with binding decisions in HANDOFF.md the same heartbeat. No
   question waits for the human. HANDOFF.md is the shared brain state.
8. CONTINUOUS PROFIT LOOP (self-learning, standing): the system re-learns
   itself from its own results, forever, without human input:
   - PnL attribution per wallet / policy / market window from the live and
     paper ledgers, refreshed every cycle.
   - Whatever is losing gets diagnosed and fixed or replaced automatically:
     bad wallet -> rotate; bad sizing/slippage/move-filter -> retune the
     execution profile from fresh evidence; bad latency -> improve the
     path. Execution profiles and promotion thresholds are re-learned
     rolling as data accumulates.
   - Every adjustment is a Fable decision logged in HANDOFF.md with the
     evidence and the measured result of the previous adjustment, so the
     system learns from its own change history too.

## Efficiency mandate (operator, 2026-07-03)

Find the effective, profitable solution FAST. Binding on both agents:

- Every heartbeat, do the action with the highest expected profit-per-hour
  of work. Ask before starting anything: "is this the fastest known path
  to more live profit?" If not, do the faster thing.
- Timebox everything: an experiment or repair that has not produced new
  evidence within 2 heartbeats is killed or redesigned — Fable decides,
  same heartbeat. No slow-burning side quests.
- Prefer measurements that decide something. Data collected without a
  pending decision attached is waste; every capture/report must name the
  decision it will settle and its threshold in advance.
- Reuse before building: existing modules, existing evidence, existing
  history downloads. Build new only when nothing existing can answer.
- Small samples decide direction, large samples confirm it: act on the
  best available evidence now and refine live, rather than waiting for
  perfect data. (Consistent with no-fake-green: acting early is fine,
  CLAIMING proven is not.)

## Standing quality mandate (operator, 2026-07-03)

Implement the highest-grade, smartest, best self-learning, most profitable
solution known to the agents — chosen by their own judgment, not the most
convenient one. When a better approach is known (better feed, better fill
model, better learning signal), switching to it is the task, and the agents
decide it themselves. Continuous profitability is the standing outcome the
human operator expects with zero intervention; the human provides only
infrastructure.

## Track 2 — Multi-wallet copy inventory (operator direction, 2026-07-03)

While Track 1 (single best wallet, maximal live copy) runs, build the next
level: a multi-wallet copy inventory over ALL scanned wallets — many orders
per window, latency-minimized, using the existing consensus/inventory
machinery (src/wallet_copy/consensus.py, inventory.py) fed by the realtime
feed. Rules:

- Design first: Fable owns the design; think it through and write it down
  (docs/agents/MULTI_WALLET_INVENTORY_DESIGN.md) before building — sizing
  across overlapping wallet signals, per-window order budget, inventory
  netting (opposing copies cancel instead of double-paying spread),
  latency path identical to Track 1.
- Paper-first, thoroughly: the full inventory lane runs in paper on real
  realtime data until its rolling paper PnL is positive and it beats or
  meaningfully adds to Track 1's live results.
- Promotion: Fable decides per OP-AUTONOMY-20260703-BELA, evidence logged
  in HANDOFF.md. If paper proves it, it goes live through the same single
  guard. Track 1 keeps running at maximum until then.

## What must still be built (build order)

1. `scripts/scan_leaderboard.py` — DISCOVER stage, auto-registration,
   candidate scoring, hourly via launchd.
2. Copyability scorer + 30d execution-profile learner (LEARN stage) — rolling per-wallet score from the realtime feed +
   book evidence, persisted state.
3. Promotion/rotation engine — applies stage 3/6 rules to the scores,
   updates the live guard's target wallet, logs every decision to
   HANDOFF.md with the evidence snapshot.
4. Persistent runner supervision — feed, paper lane, live guard, scanner
   under launchd with auto-restart, VPN egress check per cycle (degrade
   to reconnect-retry on drop, resume when back).

## Framework Audit — the system examines its own cage (operator, 2026-07-07)

Every 72 hours (and immediately after any 2-day goal-progress stall) a
dedicated OUTSIDE-THE-FRAME AUDIT runs: a fresh session that reads the
GOAL FIRST and drafts its critique BEFORE reading the accumulated
rulebook — so the framework cannot capture its own auditor. It answers,
across four axes (AI logic, development process, data sources,
execution): "Knowing everything we've learned, would we build THIS from
zero today? Which rules, KPIs, cadences, or structures now cost more
than they protect? What would a fresh outside quant do differently?"
Every rule must re-justify itself against the goal; rules that fail are
PRUNED the same day (with the operator informed of prunings). The
audit's authority: it may propose replacing ANYTHING — including the
audit rule itself.

SEAM AUDIT (operator-prompted, 2026-07-20, after the regime-boundary
seat-carryover miss): every Framework Audit pass MUST additionally
enumerate the seams — places where two rule domains touch (weekday<->
weekend and day boundaries, restarts, demotion/readmission clock
expiries, admission waves, cap/overlay regeneration, session/model
changes) — and prove for each seam that a named rule OWNS it. A seam
with no owning rule is a defect of the framework, logged and assigned
that day. Rationale: every component can pass its own contract while
the gap lives between contracts; four operator catches (order-flow
drought, missing weekend ruling, Monday re-bind, boundary seat
carryover) were all unowned seams. The auditor hunts them on schedule
so the operator does not have to.

FINAL CLAUSE — NO SACRED RULES: nothing in this document or any other
is sacred except the goal (daily-profitable live trading, BTC-5m first)
and the honesty of the ledger. Every mandate, gate, KPI, cadence, and
structure exists to serve the goal and is disposable the moment it does
not. There are no limits — only the goal, above everything.

## Enforcement (binding on both agents)

RED ESCALATION LADDER — NO RESIGNATION (operator, 2026-07-07, verbatim:
"ami vörös azt nem elfogadja a rendszer és nem beletörődik hogy a 0=0
hanem javítja akármilyen módon a cél érdekében"): a persisting red is
never a state, only a countdown. Rules: (1) every deadman re-alert /
incident recurrence must be answered with a DIFFERENT or STRONGER
intervention than the last — repeating a failed attempt unchanged is
resignation and is forbidden; the incident log must show an escalation
sequence, not a loop. (2) Standing rungs for order-flow red, in order:
config unchoke -> signal-source fast path -> alternate transport
(relay/WSS/onchain) -> guard restart -> protection-bounded emergency
admission (any measured-positive cell trades at min size) -> method
switch (maker-first / structural lane trades paper-proven cells live
small). Climbing the ladder is mechanical duty, not a decision. (3) A
red older than 4 hours with rungs still unclimbed is a brain failure
logged by name. 0=0 is never acceptable: zero activity has zero chance
of reaching $100-300/day, so the EV of climbing always wins.

ORDER-FLOW DEADMAN (operator, 2026-07-07): a zero-AI mechanical alarm
(scripts/order_flow_deadman.py, brainless_ops 10-min cadence) fires an
INCIDENT whenever can_trade=True and no live order was accepted for 30
minutes. It re-alerts every 30 min while the drought persists and
cannot be silenced by interpretation — no denominator, no "0 active
windows = 0 misses" blindness. A firing deadman outranks all other
work for every agent. Born from the 2026-07-07 17:15-21:45Z drought
the operator caught before the system did (logged as a brain failure).

THROUGHPUT DEADMAN / POLICY CHOKE (operator, 2026-07-20): the same
mechanical family fires `INCIDENT_POLICY_CHOKE` whenever `can_trade=True`
and either the selected member or the whole runtime set produces at least
50 fresh own-source rows with zero accepted live orders in a strict
rolling 30-minute interval. This output-based state is never source quiet
regardless of which policy, toxicity, floor, cap, or submit stage terminates
the flow; the payload names the dominant terminating stage and counts. Its first
rung is an acceptance-share seat read: a weekday-positive runtime member
with materially nonzero own-source policy acceptance outranks a zero-share
incumbent. If every runtime member is zero, the next rung is
protection-bounded emergency admission of a measured-positive cell at
minimum size; if that cannot restore flow, method switch follows. These
rungs may not loosen expectancy, price, fill-cap, or size protections. The
P-1 fire drill must prove that a synthetic 497-source/zero-accepted-output
state fires the incident and emits the rung-A seat read. Rung B is
deadman-owned: select exactly one non-runtime ready-shadow candidate whose
current-regime cell is positive (PnL/ROI >0, n>=200), whose own fresh BUY
flow is >=10 in the same 30-minute window, whose fading/cooloff and external
liveness gates pass, and whose exact evidenced paper policy is reusable.
Admit and pin it atomically at min($1, standing probe cap) for a hard,
non-refreshing 3600s TTL. The existing auto-degrade owns losses; the deadman
owns TTL expiry, disables a zero-conversion probe, records a 24h wallet
cooloff, and emits `RUNG_C_METHOD_SWITCH_DUE`. Active rung-B pins are
idempotent and never stack. Rung C switches candidate supply, never quality:
it reuses the byte-identical F1-F4 evaluator over cohort-alive admission
packets and then the ranked full-pool queue, with the same exact-policy,
$1/3600s TTL, 24h cooloff, one-seat, and idempotence contract. If that full
sweep is dry, `RUNG_C_NO_ADMISSIBLE_TARGET` is the executed terminal outcome:
it remains visible and is re-evaluated each heartbeat without actuator spin.
Holding the unchanged bars when no measured-positive member or method exists
is the method-switch decision; negative methods are never promoted to fake
throughput.

NO-EXCUSES RULE (operator, 2026-07-07, verbatim: "ne mentegetőzz ne
mentegetőzzön se a fable-cli, sem a codex. hajtsátok végre"): neither
brain nor implementer apologizes, hedges, or explains why something
was hard. Every report is three things only: STATE (numbers), CAUSE
(named), ACTION (shipped or next, with owner and time). Sentences that
justify, soften, or pre-excuse are deleted before writing. A missed
target is reported as a defect + fix, never as a story. Execute.

- Document authority, in order: (1) this flow contract + recorded operator
  decisions, (2) the latest fable DIRECTION in HANDOFF.md, (3)
  LIVE_TODAY_SPRINT.md, (4) CODEX_TASK.md (detection build details). On any
  conflict the higher document wins; fixing the lower one is part of the
  task.
- Every HANDOFF entry (codex STATUS and fable DIRECTION alike) must name
  the flow stage each work item advances: DISCOVER / OBSERVE / PROMOTE /
  LEARN / LIVE / ROTATE / TRACK2 / SELF-DEV. Work that maps to no stage is
  scope drift: Codex drops it, Fable redirects it.
- Priority when stages compete: LIVE operations of the chosen wallet first,
  then the stage that most directly increases live profit, then TRACK2.
- PROFITABLE CONFIG LOCK (operator, 2026-07-05, OP-STABLE-20260705-BELA):
  when a live lane passes its resolved-fill gate with positive rolling
  PnL, the primary Fable declares CONFIG LOCK in a DIRECTION. Under lock:
  the live path (policy, caps, buffers, sizing, guard args, hot-path
  code) is FROZEN — codex and substitute brains may not modify, refactor,
  "improve", retune, or reload it, no matter what. Only the mechanical
  protections keep executing (rolling loss rotation, activity-aware
  inactivity rotation, hard entry cap, MISS incidents). All development
  continues in paper/research lanes
  without touching live-path files. Unlock requires a primary-Fable
  DIRECTION with evidence: either the rotation mechanic fired, or a
  paper-proven improvement measurably beats the locked lane. Any commit
  touching live-path files under lock must cite the unlocking DIRECTION
  id — without it the change is forbidden and gets reverted. Fable
  itself changes a locked lane only when necessary AND justified in
  writing — stability of a winning configuration outranks improvement.
- PRODUCING-PROTECTED LIVE CHANGE DISCIPLINE (operator/Fable,
  2026-07-10T19:00Z + 19:10Z amendment): while the canonical live verdict
  is PRODUCING, or for 2 days after leaving PRODUCING, every live-path
  behavior change is justified, snapshotted, journaled, canaried, and
  revertible. Before any non-SOS touch, capture `golden_config_<ts>` with
  `scripts/capture_live_golden_config.py`; write a one-line target
  justification naming the enemy or defect plus expected effect in dollars
  or windows; append `data/research/live_change_journal.jsonl`; change one
  variable only; start on the smallest meaningful canary slice; widen only
  after the pre-registered metric passes; instantly restore the golden
  snapshot on regression. SOS interventions (deadman red, guard down,
  bleeding breach, producing-verdict at risk, or restoring documented
  behavior) are executed immediately, but still require the golden snapshot
  before the touch and a journal entry. Research, paper, shadow, and dry-run
  lanes remain unrestricted; this rule governs the last step onto the
  producing live path.
- TRADING CONTINUITY INVARIANT (operator, 2026-07-05, structural — binds
  every brain tier including primary): the system's DEFAULT STATE is
  trading with bounded protections; strictness is CONFIRMATION, never
  PRECONDITION. No gate, ruling, or threshold — regardless of which
  brain authored it — may leave the live set empty or the system
  armed-idle for more than 60 minutes while ANY candidate with positive
  prior evidence exists in the registry. When that happens, the gate
  AUTO-DEGRADES mechanically to protection-bounded admission (top
  candidates by existing evidence, standard sizing, -$16/20 rotation as
  the real admission control) — no brain approval needed, any tier or
  the brainless runner executes it. A brain creating a gate that can
  choke trading is itself a defect: every new gate must state its
  auto-degrade path at creation or it is invalid. Discovery is
  live-funded and protection-bounded by design; idleness is never the
  safe option, because idle loses with certainty.
- WINDOW PARTICIPATION RULE (operator, 2026-07-04): if ANY active-set
  member is actively trading a window (has copy-eligible orders within
  its band) and WE produce zero accepted orders, that window is a MISS.
  3 consecutive missed active windows = incident, same heartbeat: pull
  the skip/reject taxonomy counts, name the gate or defect that ate the
  flow, and either fix it or log an explicit Fable decision that skipping
  was evidence-correct (measured skip is a decision, never a default).
  Sitting through inactive-for-us windows while the wallet fires hundreds
  of orders is irrational and forbidden — non-participation is a defect
  until proven a choice.
- WINDOW VOLUME KPI (operator, 2026-07-05): the bot's essence is that it
  TRADES and wins — presence is the product. Measured per heartbeat and
  on the DAILY SCORECARD: windows_traded / windows_total, plus a reason
  taxonomy for every empty window: no_signal (no member in-band flow AND
  no consensus signal — acceptable, logged), signal_but_missed (DEFECT,
  fixed same heartbeat), filtered_by_band (feeds band-tuning review).
  Low volume is a system defect visible to BOTH agents: if
  windows_traded stays below the target while the whale cohort trades
  nearly every window, the fix is growing SIGNAL COVERAGE — more
  qualified members, wider evidence-backed bands, and the consensus
  lane — never lowering the expectancy bar. Volume target: trades in
  the large majority of windows, reached by coverage, not by hope.
- POINTER-PROMPT RULE (operator, 2026-07-05): every scheduled/recurring
  prompt (app scheduler, chat automation, launchd, cron) must be a
  ONE-LINE POINTER to a canonical repo file (e.g. "Read
  docs/agents/HEARTBEAT_PROMPT.md and execute it literally"). Carrying
  instruction CONTENT inside a scheduled prompt is forbidden — content
  rots, pointers do not. Codex converts its own automations to pointers
  wherever it can edit them, and the DAILY SCORECARD includes an
  automation-drift check: list every automation + confirm each is a
  pointer; any content-carrying automation found is a defect fixed the
  same heartbeat (or escalated with the exact replacement line if only
  the operator can edit it).
- EMPTY-RESULT AUDITS THE INSTRUMENT (Fable, 2026-07-05): if a
  qualification/measurement pass over a large universe (thousands of
  wallets, dozens of positive profiles) returns ZERO candidates/members,
  the mandatory first hypothesis is that the BAR or the MEASUREMENT is
  broken — never that the market is empty. The pass result must then be
  escalated to the brain with the bar, the sample sizes, and the filter
  fallout counts, and the bar/instrument gets reviewed before the empty
  result may be accepted as fact. Resignation to zero is forbidden.
- BUILD mode vs OBSERVE mode: the loop stays in BUILD mode — every
  heartbeat delivers development work — until Fable declares OBSERVE mode
  in a DIRECTION entry. OBSERVE requires evidence of a consistent,
  realistically profitable live solution: default bar (Fable tunes with
  evidence) is 7 consecutive days of positive rolling live PnL on real
  fills in the ledger. Monitoring-only heartbeats before that declaration
  are forbidden for both agents. Any regression (rolling live PnL turns
  negative, feed/execution degrades) returns the loop to BUILD mode
  automatically, no human input needed.

## Structural invariants (what keeps the system debuggable)

- CopyIntent parity: live executes the same intents as paper.
- Single live guard is the only order submitter.
- Every promotion/rotation/threshold change is logged in HANDOFF.md with
  the evidence that justified it.
