from datetime import UTC, datetime

import pytest

from src.wallet_copy.alpha_freshness import require_fresh_alpha_report


def test_stale_alpha_report_refuses_with_named_age() -> None:
    with pytest.raises(ValueError, match="STALE_ALPHA_REPORT_REFUSED age_h=25.000000"):
        require_fresh_alpha_report(
            {"updated_at": "2026-07-30T10:00:00Z"},
            path="alpha.json",
            now=datetime(2026, 7, 31, 11, 0, tzinfo=UTC),
        )


def test_fresh_alpha_report_returns_age() -> None:
    assert require_fresh_alpha_report(
        {"updated_at": "2026-07-31T10:30:00Z"},
        path="alpha.json",
        now=datetime(2026, 7, 31, 11, 0, tzinfo=UTC),
    ) == 0.5
