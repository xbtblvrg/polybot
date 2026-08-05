"""Shared truth predicates for the immutable WIDE standby binding."""

from __future__ import annotations

from typing import Any


TERMINAL_EXECUTION_STATUSES = frozenset({"EXECUTED", "PARK_COMMITTED"})


def binding_terminally_executed(artifact: dict[str, Any] | None) -> bool:
    """Return whether an allowed execution spelling carries a terminal outcome."""

    if not isinstance(artifact, dict):
        return False
    binding = artifact.get("binding")
    if not isinstance(binding, dict):
        return False
    outcome = binding.get("terminal_outcome_on_deadline")
    return bool(
        artifact.get("execution_status") in TERMINAL_EXECUTION_STATUSES
        and isinstance(outcome, dict)
        and outcome.get("terminal") is True
    )
