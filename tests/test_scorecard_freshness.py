from datetime import UTC, datetime

import pytest

from src.wallet_copy.scorecard import assert_scorecard_fresh


def test_current_scorecard_fails_loud_when_older_than_six_hours() -> None:
    with pytest.raises(RuntimeError, match="scorecard stale"):
        assert_scorecard_fresh(
            {"generated_at": "2026-08-02T10:00:00Z"},
            path="scorecard.json",
            now=datetime(2026, 8, 2, 16, 0, 1, tzinfo=UTC),
        )


def test_current_scorecard_accepts_six_hour_boundary() -> None:
    payload = {"generated_at": "2026-08-02T10:00:00Z"}
    assert (
        assert_scorecard_fresh(
            payload,
            path="scorecard.json",
            now=datetime(2026, 8, 2, 16, 0, 0, tzinfo=UTC),
        )
        is payload
    )
