"""Action-oriented workflow status helpers for wallet-copy gates."""

from __future__ import annotations

from typing import Iterable


ANALYZE = "ANALYZE"
CORRECTION = "CORRECTION"
PASS = "PASS"
WATCH = "WATCH"


_CORRECTION_HINTS = (
    "bug",
    "contradict",
    "coverage",
    "error",
    "fail",
    "fallback",
    "missed",
    "mismatch",
    "not_live_admissible",
    "not_live_ready",
    "rejected",
    "reset",
    "stale",
    "violation",
)

_ANALYZE_HINTS = (
    "bounded",
    "candidate_search",
    "insufficient",
    "limited",
    "missing",
    "no_",
    "research_only",
    "unresolved",
)


def active_status_from_blockers(blockers: Iterable[object] | None, *, default: str = WATCH) -> str:
    """Map blockers to an active next-step status.

    The old passive "wait until ready" state hid whether the next useful action was
    measurement or repair. This helper keeps the money gate strict while making the
    development loop explicit: ANALYZE means gather better evidence, CORRECTION
    means a concrete copy/fill/lifecycle issue must be fixed.
    """

    blocker_text = " ".join(str(blocker).lower() for blocker in (blockers or []) if blocker)
    if not blocker_text:
        return default
    if any(hint in blocker_text for hint in _CORRECTION_HINTS):
        return CORRECTION
    if any(hint in blocker_text for hint in _ANALYZE_HINTS):
        return ANALYZE
    return default


def active_plan_mode(live_ready: bool, blockers: Iterable[object] | None, *, target: str) -> str:
    if live_ready:
        return "READY_BEHIND_EXPLICIT_OPERATOR_GATE"
    return f"{active_status_from_blockers(blockers, default=ANALYZE)}_UNTIL_{target}"
