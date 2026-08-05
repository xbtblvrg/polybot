# Operator Ideas — PARKED (2026-07-19)

Source: operator-collected external material on loop engineering
(29-page PDF, reviewed by Fable 2026-07-19). Operator decision
(Béla, 2026-07-19): SAVE for later gradual implementation; nothing
urgent — the running system is NOT to be modified because of this
document. No agent may schedule work on these items without a
later explicit Fable DIRECTION or operator order.

Status: PARKED. This file is a memory, not a task list.

## P-1. Gate fire drills (verifier-rot defense) — the strongest idea

Claim: every verifier decays — a gate that once caught a failure
drifts blind as the system changes around it, and a green check
becomes ritual, not proof.

Idea: on a schedule, deliberately inject a known-bad input and
prove the gate still screams. Forms: re-introduce a previously
fixed bug in a sandbox and confirm the regression test fails;
feed the audit layer a doctored packet (wrong digit, fake-green
artifact) and confirm the digit-check catches it; feed a deadman
a synthetic dead state and confirm it fires.

Fit: extends the existing "fire drill #1" debt-queue item and the
functional-truth audit doctrine. Natural adoption point: when the
debt queue reaches fire drill #1, widen it into a recurring
all-gates drill instead of a one-off.

## P-2. Escalation-pattern meta-read

Claim: log every stop-condition/trigger firing across every loop;
a gate that constantly hits its ceiling signals a miscalibrated
threshold, not a noisy world.

Idea: a periodic (e.g. monthly) report-only artifact: which
tripwires/clocks/deadmen fired how often, which never fire at
all, and what that says about calibration. Incidents are already
logged; this is only an aggregation view.

Fit: report-only, cheap. Natural adoption point: bundle with a
Framework Audit pass.

## P-3. Cost-per-accepted-change on the heartbeat economy

Claim: the metric that matters for an agent loop is not tokens
but cost per accepted (gated) result.

Idea: our scarce resource is the codex heartbeat slot (one per
~15 min). Track per measurement lane: heartbeats consumed per
gated finding. Lanes far above median are candidates for
timeboxing or retirement. The existing 2-heartbeat timebox rule
already bounds the worst case; this adds finer resolution.

Fit: extends the weekly R&D yield discipline ("only gated
findings count"). Natural adoption point: weekly verdict
tooling, report-only first.

## Explicitly NOT adopted (reviewed and rejected for now)

The material's remaining content (loop anatomy, maker/checker
split, ground-truth judging, stop conditions, state files, blast
radius, context hygiene) is already implemented in this system,
in most cases more rigorously than described. No structural
change is warranted.
