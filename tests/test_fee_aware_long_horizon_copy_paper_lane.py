from __future__ import annotations

import datetime

from scripts.launch_fee_aware_long_horizon_copy_paper_lane import build_launchd_payload
from scripts.run_fee_aware_long_horizon_copy_paper_lane import (
    A689,
    DEADLINE_TS,
    durable_intent_ids,
    _report,
    observation_from_book,
    pending_disposition,
    pending_from_intents,
    polygon_rows_to_intents,
)


def _row() -> dict:
    return {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "transaction_hash": "0xtx",
        "log_index": 3,
        "event_ts": 100.0,
        "captured_at_s": 100.5,
        "selected_wallet": A689,
        "registry_wallets": [A689],
        "decoded": {"asset": "token-up", "side": "BUY", "price": 0.4, "size": 10.0},
    }


def test_polygon_rows_create_one_parity_intent_and_both_a689_horizons() -> None:
    intents, unmapped, excluded = polygon_rows_to_intents(
        [_row(), _row()],
        token_meta={"token-up": {"market_slug": "btc-updown-5m-100", "condition_id": "cond", "outcome": "Up"}},
        collector_start_ts=100.0,
    )

    assert len(intents) == 1
    assert not unmapped
    assert not excluded
    assert intents[0].copy_size_usd == 1.0
    assert intents[0].live_orders_allowed is False
    assert [row["horizon_s"] for row in pending_from_intents(intents)] == [5.0, 30.0]


def test_observation_uses_first_timely_ask_and_canonical_fee() -> None:
    intents, _, _ = polygon_rows_to_intents(
        [_row()],
        token_meta={"token-up": {"market_slug": "btc-updown-5m-100", "condition_id": "cond", "outcome": "Up"}},
        collector_start_ts=100.0,
    )
    pending = pending_from_intents(intents)[0]

    row = observation_from_book(
        pending,
        {"bids": [{"price": "0.49"}], "asks": [{"price": "0.50"}]},
        captured_at_s=106.0,
    )

    assert row is not None
    assert row["observation_lag_s"] == 1.0
    assert row["principal_usd"] == 1.0
    assert row["filled_shares"] == 2.0
    assert row["expected_fee_usd"] == 0.0
    assert row["live_order_attempted"] is False
    assert observation_from_book(pending, {"asks": [{"price": "0.50"}]}, captured_at_s=111.0) is None


def test_mapped_non_btc5m_signal_is_excluded_not_left_unmapped() -> None:
    intents, unmapped, excluded = polygon_rows_to_intents(
        [_row()],
        token_meta={"token-up": {"market_slug": "eth-updown-5m-100", "condition_id": "cond", "outcome": "Up"}},
        collector_start_ts=100.0,
    )

    assert not intents
    assert not unmapped
    assert excluded[0]["reason"] == "mapped_non_btc5m_market"


def test_missing_token_metadata_remains_retryable() -> None:
    intents, unmapped, excluded = polygon_rows_to_intents(
        [_row()],
        token_meta={},
        collector_start_ts=100.0,
    )

    assert not intents
    assert len(unmapped) == 1
    assert not excluded


def test_durable_intent_ids_survive_when_all_horizons_expired() -> None:
    state = {
        "pending": [],
        "observations": [],
        "expired": [{"intent": {"intent_id": "ci_expired"}}, {"intent": {"intent_id": "ci_expired"}}],
    }

    assert durable_intent_ids(state) == {"ci_expired"}


def test_launchd_collector_is_persistent_paper_process() -> None:
    payload = build_launchd_payload(python="/usr/bin/python3", stdout="/tmp/out", stderr="/tmp/err")

    assert payload["KeepAlive"] is True
    assert payload["RunAtLoad"] is True
    assert payload["EnvironmentVariables"]["SSL_CERT_FILE"].endswith("cacert.pem")
    assert "run_fee_aware_long_horizon_copy_paper_lane.py" in payload["ProgramArguments"][1]


def test_deadline_constant_matches_preregistered_deadline() -> None:
    registered = datetime.datetime(2026, 7, 22, 19, 0, tzinfo=datetime.timezone.utc)

    assert DEADLINE_TS == registered.timestamp()


def test_pending_disposition_refuses_observation_at_or_after_deadline() -> None:
    item = {"target_ts": 100.0}

    assert pending_disposition(item, now=99.0, deadline_ts=200.0) == "WAIT"
    assert pending_disposition(item, now=103.0, deadline_ts=200.0) == "OBSERVE"
    assert pending_disposition(item, now=106.0, deadline_ts=200.0) == "MISSED_OBSERVATION_LAG"
    assert pending_disposition(item, now=200.0, deadline_ts=200.0) == "AFTER_DEADLINE"
    assert pending_disposition(item, now=103.0, deadline_ts=103.0) == "AFTER_DEADLINE"


def test_report_marks_unmapped_signal_as_pending_not_parity_failure(tmp_path) -> None:
    report = _report(
        {
            "experiment_id": "fee-aware-long-horizon-copy-paper-20260719",
            "source_signals": 1,
            "copy_intents": 0,
            "pending": [],
            "observations": [],
            "unmapped": [{"signal_id": "lhs_unmapped"}],
        },
        resolutions_path=str(tmp_path / "missing.jsonl"),
        deadline_ts=9_999_999_999.0,
    )

    assert report["copy_intent_parity"]["status"] == "PENDING_TOKEN_MAPPING"
    assert report["copy_intent_parity"]["violations"] == 0
    assert report["copy_intent_parity"]["pending_token_mapping"] == 1
