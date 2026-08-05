# Codex Heartbeat Prompt (canonical — automations POINT here, never copy
# content; pointer line: "Read docs/agents/HEARTBEAT_PROMPT.md in
# /Users/belavarga/claudecode/polymarket-agent and execute it literally
# as this run's instructions.")

Wallet-copy live trading heartbeat for
/Users/belavarga/claudecode/polymarket-agent.

SOURCE OF TRUTH, in this order, every run: AGENTS.md ->
docs/agents/AUTONOMOUS_FLOW.md (flow contract + recorded operator
decisions) -> the LATEST fable DIRECTION in docs/agents/HANDOFF.md. These
override anything remembered from earlier runs and anything hardcoded in
any prompt. Never carry candidate ids, wallets, policies, or build phases
inside a prompt — read them fresh from repo state each run; prompts rot,
the repo is the living truth.

SERVING RUN MUTEX (Fable 2026-07-13T05:15Z): before any state-changing
or milestone work, ensure this run owns the shared serving mutex. If
`CODEX_SERVING_MUTEX_HELD=1`, the runner already owns it. Otherwise run
`python3 scripts/codex_serving_mutex.py acquire --owner heartbeat_prompt --holder-pid $$ --token-file /tmp/polymarket_codex_serving_run.heartbeat_prompt.token`;
if it reports HELD / exits 75, skip fast: no HANDOFF write, no Fable call,
no commit. If acquired here, release it at the very end with
`python3 scripts/codex_serving_mutex.py release --token-file /tmp/polymarket_codex_serving_run.heartbeat_prompt.token`.

STATE DIGEST SHADOW PHASE (Fable 2026-07-06T20:50Z): if
data/research/state_digest.md exists, read it in addition to the full source
context above. It is a reading aid only, never an authority source. Every
STATUS must grade it: did the digest contain all material facts actually
used this run? If not, log the missing item as a digest defect and fix the
digest generator the same heartbeat. Never run digest-only; full-context
fallback is permanent.

STRUCTURAL INVARIANTS (the only hard limits): CopyIntent parity, and
scripts/run_wallet_copy_live_guard.py as the sole live order submitter —
never start a second one. Everything else — rotation, promotion,
thresholds, arming, method changes — is governed by the recorded operator
decisions in AUTONOMOUS_FLOW.md: Fable decides; consult it via
./scripts/ask_fable.sh (the brain chain falls back Claude -> Grok ->
AGY -> codex/GPT automatically, timeout-guarded).

EVERY RUN:
0. DEADMAN FIRST: if data/research/order_flow_deadman_state.json says
   INCIDENT_ORDER_FLOW_DEAD (or a brainless NOTIFY deadman entry is the
   newest), DROP EVERYTHING: name the choke, restore live order flow,
   verify accepted orders resume, report the participation table — all
   in THIS run. No other work counts while the deadman is firing.
1. Verify REAL trading, not process health: "guard fut" != "valóban
   történik live copy trading". YES only if: single guard is sole
   authority + runtime can_trade=true + fresh ledger delta since last
   heartbeat (or a fresh live-eligible intent armed). Check the
   per-window participation table: a window where ANY active-set member
   has copy-eligible orders and we have zero accepted orders is a MISS;
   3 consecutive misses = incident
   handled THIS run (taxonomy counts -> named cause -> fix or explicit
   evidence-backed measured-skip decision from Fable).
2. Execute the latest fable DIRECTION's next-list, top-down. Do not
   self-select other work; LIVE profit work outranks everything.
3. Any obstacle is an OPEN DEFECT logged as "defect | attempts (3+) |
   next" — the blocker concept is retired; a defect without a next action
   does not exist.
4. End with a stage-tagged STATUS entry (<=25 lines) in HANDOFF.md and a
   commit; call ask_fable.sh at milestones and on every open question.

REPORT IN HUNGARIAN, concisely: actual_live_trading igen/nem;
részvétel-tábla (utolsó N aktív ablak: wallet-orderek / beadásaink /
fill-jeink); ledger-delta és realizált PnL-delta az előző heartbeat óta;
windows_traded/288 (OP-VOLUME numerikus mérce);
aktív wallet/policy (friss state-ből olvasva); nyitott defectek next
actionnel; a DIRECTION melyik pontja készült el és mi a következő.

NOTIFY ha: nincs valódi trading miközben bármely készlet-tag wallet aktív; MISS-incidens;
PnL-trend negatívba fordul; agy-lánc degradált (Grok/GPT helyettesít);
operátori infra kell (VPN, kulcs, egyenleg). DONT_NOTIFY csak ha valódi
trading fut, a DIRECTION-lista halad, és nincs emberi teendő.

NO-DELTA SILENCE RULE (operator, 2026-07-05): a strict no-delta heartbeat
(no ledger delta, no defect change, no gate crossing) writes NO handoff
entry and makes NO ask_fable call — silence is the report. Exceptions:
one consolidated status line per hour, and an IMMEDIATE brain call on
gate crossing, PnL sign change, MISS incident, defect change, or wallet
activity resuming. Never burn tokens confirming that nothing happened.

CONFIG LOCK RULE (operator, 2026-07-05): if the latest fable DIRECTION
declares CONFIG LOCK on the live lane, you do NOT touch the live path —
no tuning, refactoring, reloading, or "improving", regardless of ideas.
Measurement, paper lanes, and research only. Mechanical protections
(rolling rotation, hard cap, incidents) keep running. Changes to a locked
live path require a primary-Fable unlocking DIRECTION id cited in the
commit.

DAILY SCORECARD (operator, 2026-07-05): on the first heartbeat after
00:00 UTC, run `python3 scripts/report_daily_scorecard.py --day
<YYYY-MM-DD UTC yesterday>` and write a DAILY entry to HANDOFF.md:
realized PnL from the ledger (total + per lane), fills/win-rate,
windows_traded/288, guard skip histogram, active-set roster/policy
changes, and E5/E6 gate progress from `engine_race_gates`. Then call the
brain for the day's verdict and plan. The day is the unit of success —
every day ends with a number and a plan.
Include SINCE-TOPUP truth (operator, 2026-07-06): canonical PnL and
actual on-chain balance measured against the LAST WALLET TOP-UP baseline
($335 at 2026-07-05, updated on every future deposit/withdrawal), shown
next to the daily number on every scorecard and STATUS PnL line. The
since-topup line is the primary "is it actually producing" verdict —
daily deltas alone are forbidden as a success claim.
Include target-vs-actual per OP-TARGET: +2%/day minimum average, weekly
verdict every Sunday 00:00 UTC vs the 10-15% weekly minimum.
HANDOFF ROLL (Fable 2026-07-07): in the same first-heartbeat-after-00:00
pass, roll HANDOFF.md entries older than 2 full days into the current
month's docs/agents/HANDOFF_ARCHIVE_*.md (append-only; nothing deleted;
recorded operator decisions stay canonical in AUTONOMOUS_FLOW.md). The
two files together are the append-only history — the roll is a move,
never an edit.
Include an automation-drift check: list every self-managed automation and
its pointer-status; any content-carrying prompt is a defect fixed in the
same heartbeat.
