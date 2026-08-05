# Codex Deep-Research Prompt (canonical, 4-hourly cadence)

THE GOAL (carry it in every study): $100-300/day cash profit,
BTC-5m, toward 288/288 profitable windows — research exists only to
feed that number. Every study names which enemy/campaign it serves.

Background research/development pass for
/Users/belavarga/claudecode/polymarket-agent. This is the WIDE lane: it
feeds the live system, it never steers it.

SOURCE OF TRUTH, in this order, every run: AGENTS.md ->
docs/agents/AUTONOMOUS_FLOW.md (flow contract + recorded operator
decisions) -> latest fable DIRECTION in docs/agents/HANDOFF.md. Older
framework docs (WALLET_COPY_OPERATING_FRAMEWORK.md, old mission wording)
are historical: where they conflict with the recorded operator decisions,
the decisions win. No prompt-carried facts — read candidates, policies,
thresholds, and gates fresh from repo state; they change hourly.
Precedence note: recorded operator decisions and the latest Fable DIRECTION
override standing prompts/docs, including this file, mission.py, and
framework docs.

RUN-START MUTEX: before serving any milestone, try to create
`/tmp/wallet_copy_research_development_serving.lock` with `mkdir`. If it
already exists and the recorded PID is still alive, exit with a short
STATUS/automation-memory note instead of doing duplicate work. If the PID is
dead, remove the stale directory and continue. Write `pid`, `started_at`, and
the current HANDOFF latest-direction timestamp into the lock directory, and
remove it on clean exit. This is separate from `codex_heartbeat.sh`'s launchd
lock because app automations can enter without that wrapper.
Use `scripts/with_research_development_serving_lock.sh <command...>` when a
shell wrapper can own the whole run; do not write a short-lived subshell PID as
the holder.

STRUCTURAL INVARIANTS ONLY: CopyIntent parity; the single live guard is
the sole live order submitter — this research pass never starts runners,
never touches guard config, never edits mission/strategy state for the
live lane (propose via HANDOFF, Fable decides per OP-AUTONOMY).

SCOPE OF THIS PASS (stage-tagged, in priority order):
1. DISCOVER — maximize observation: refresh leaderboard onboarding
   (weekly+monthly crypto, full pages), preserve all known wallets, add
   new ones to the registry. Never progress by dropping or narrowing
   coverage.
2. LEARN — refresh per-wallet execution profiles and alpha-decay/
   copyability evidence from accumulated captures; refresh the per-window
   behavior model (orders/window, laddering, VWAP paths).
3. OBSERVE — advance the paper lanes the latest DIRECTION names (e.g.
   whale_consensus_v1 paper measurement, top-10 cohort measurement),
   dedupe-clean, survivor-bias-safe.
4. PROMOTE prep — rerank candidates by measured copyability at our
   latency + paper PnL at our prices; if a candidate beats the live
   lane's fresh-window numbers, write the evidence package and flag it
   for Fable's rotation decision in your STATUS.
Timebox each stage; an experiment with no new evidence in 2 heartbeats is
killed or redesigned (efficiency mandate).

RULES: obstacles are open defects ("defect | attempts (3+) | next") — the
blocker concept is retired; no idle ANALYZE/HOLD states — every run ends
with shipped evidence or a shipped fix; after code edits run targeted
tests + compileall (full pytest only when hot-path src/scripts change);
end with a stage-tagged STATUS entry (<=25 lines) + commit + ask_fable.sh
(brain chain handles fallback).

LARGE-ARTIFACT COMMIT HYGIENE (Fable 2026-07-20T04:10Z): after refreshing
`routing_shadow_validation_latest.json` or
`wallet_market_cohort_replay_latest.json`, run
`python3 scripts/write_research_artifact_digest.py`. The full payloads are
ignored local runtime state and must not be staged; commit only their
`*_digest.json` companions, which preserve status/counters, preregistered
clocks, SHA-256, and row count.

REPORT IN HUNGARIAN, concisely: lefedettség (registry méret, új wallet);
profil/alpha-decay frissítés eredménye; paper-lane számok (esemény, EV,
PnL a mi árainkon); jelölt-ranglista top 5 és van-e rotáció-jelölt a
Fable-nek; nyitott defectek next actionnel; következő lépés.
