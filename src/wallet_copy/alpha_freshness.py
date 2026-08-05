"""Fail-closed age checks for alpha reports consumed by paper builders."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _timestamp(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def require_fresh_alpha_report(
    report: dict[str, Any],
    *,
    path: str,
    max_age_h: float = 24.0,
    now: datetime | None = None,
) -> float:
    now_ts = (now or datetime.now(tz=UTC)).timestamp()
    source_ts = _timestamp(report.get("updated_at"))
    if source_ts is None:
        raise ValueError(f"ALPHA_REPORT_AGE_MISSING_REFUSED path={path}")
    age_h = max(0.0, now_ts - source_ts) / 3600.0
    if age_h > float(max_age_h):
        raise ValueError(
            f"STALE_ALPHA_REPORT_REFUSED age_h={age_h:.6f} "
            f"max_age_h={float(max_age_h):.6f} path={path}"
        )
    return age_h
