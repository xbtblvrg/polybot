"""Fail-loud loading for the canonical wallet-copy money scorecard."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


MAX_SCORECARD_AGE_HOURS = 6.0


def _parse_generated_at(value: Any) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RuntimeError("scorecard generated_at is missing or invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def assert_scorecard_fresh(
    payload: Any,
    *,
    path: str | Path,
    now: datetime | None = None,
    max_age_hours: float = MAX_SCORECARD_AGE_HOURS,
) -> dict[str, Any]:
    """Return a scorecard only when its generation timestamp is fresh."""

    if not isinstance(payload, dict) or not payload:
        raise RuntimeError(f"scorecard missing or invalid: {path}")
    generated_at = _parse_generated_at(payload.get("generated_at"))
    current = (now or datetime.now(UTC)).astimezone(UTC)
    age_hours = (current - generated_at).total_seconds() / 3600.0
    if age_hours > float(max_age_hours):
        raise RuntimeError(
            f"scorecard stale: {path} generated_at={generated_at.isoformat()} "
            f"age_hours={age_hours:.3f} limit_hours={float(max_age_hours):.3f}"
        )
    return payload


def load_fresh_scorecard(
    path: str | Path,
    *,
    now: datetime | None = None,
    max_age_hours: float = MAX_SCORECARD_AGE_HOURS,
) -> dict[str, Any]:
    target = Path(path)
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"scorecard unreadable: {target}") from exc
    return assert_scorecard_fresh(payload, path=target, now=now, max_age_hours=max_age_hours)
