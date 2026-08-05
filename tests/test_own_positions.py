import json
import hashlib
from pathlib import Path

from eth_utils import function_signature_to_4byte_selector

from src.wallet_copy.own_positions import (
    ledger_estimate_redeem_candidates_from_report,
    ledger_redeemable_estimate,
    normalize_position_row,
    redeem_calldata,
    split_position_calldata,
    redeem_candidates_from_report,
    update_redeem_deadman,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_normalizes_redeemable_position_and_builds_redeem_calldata() -> None:
    condition_id = "0x" + ("12" * 32)
    row = normalize_position_row(
        {
            "conditionId": condition_id,
            "asset": "token-a",
            "slug": "btc-updown-5m-1000",
            "outcome": "YES",
            "size": "4.2",
            "currentValue": "3.8",
            "avgPrice": "0.55",
            "redeemable": "true",
            "negRisk": "false",
        }
    )

    assert row["condition_id"] == condition_id
    assert row["redeemable"] is True
    assert row["neg_risk"] is False
    assert row["redeemable_value_usd"] == 3.8

    calldata = redeem_calldata(condition_id)
    selector = function_signature_to_4byte_selector(
        "redeemPositions(address,bytes32,bytes32,uint256[])"
    ).hex()
    assert calldata.startswith("0x" + selector)
    assert condition_id[2:].lower() in calldata.lower()

    split = split_position_calldata(condition_id, amount_usd=1.0)
    split_selector = function_signature_to_4byte_selector(
        "splitPosition(address,bytes32,bytes32,uint256[],uint256)"
    ).hex()
    assert split.startswith("0x" + split_selector)
    assert condition_id[2:].lower() in split.lower()


def test_redeem_candidates_skip_neg_risk_and_invalid_conditions() -> None:
    valid = "0x" + ("34" * 32)
    report = {
        "redeemable_positions": [
            {"condition_id": valid, "market_slug": "m1", "redeemable_value_usd": 9.0},
            {"condition_id": "0xshort", "market_slug": "bad", "redeemable_value_usd": 100.0},
            {
                "condition_id": "0x" + ("56" * 32),
                "market_slug": "neg",
                "redeemable_value_usd": 5.0,
                "neg_risk": True,
            },
        ]
    }

    assert redeem_candidates_from_report(report) == [
        {
            "condition_id": valid,
            "market_slug": "m1",
            "estimated_value_usd": 9.0,
            "source": "data_api_redeemable_position",
            "neg_risk": False,
        }
    ]


def test_ledger_estimate_candidates_are_labeled_for_fallback() -> None:
    valid = "0x" + ("9a" * 32)
    report = {
        "ledger_estimate": {
            "conditions": [
                {
                    "condition_id": valid,
                    "market_slug": "m1",
                    "estimated_value_usd": 8.25,
                    "winning_side": "YES",
                    "orders": 2,
                },
                {"condition_id": "0xshort", "estimated_value_usd": 100.0},
                {"condition_id": "0x" + ("ab" * 32), "estimated_value_usd": 0.0},
            ]
        }
    }

    assert ledger_estimate_redeem_candidates_from_report(report) == [
        {
            "condition_id": valid,
            "market_slug": "m1",
            "estimated_value_usd": 8.25,
            "source": "ledger_estimate_data_api_unavailable",
            "winning_side": "YES",
            "orders": 2,
            "neg_risk": False,
        }
    ]


def test_ledger_redeemable_estimate_subtracts_local_redeem_events(tmp_path: Path) -> None:
    condition_id = "0x" + ("78" * 32)
    ledger = tmp_path / "ledger.json"
    resolutions = tmp_path / "resolutions.jsonl"
    redeem_events = tmp_path / "redeem_events.jsonl"
    _write_json(
        ledger,
        {
            "orders": [
                {
                    "status": "FILLED",
                    "submitted_at": "2026-07-08T00:01:00Z",
                    "condition_id": condition_id,
                    "market_slug": "btc-updown-5m-1700000000",
                    "side": "YES",
                    "limit_price": 0.5,
                    "filled_size_usd": 2.5,
                    "filled_shares": 5.0,
                },
                {
                    "status": "FILLED",
                    "submitted_at": "2026-07-08T00:02:00Z",
                    "condition_id": condition_id,
                    "market_slug": "btc-updown-5m-1700000000",
                    "side": "NO",
                    "limit_price": 0.4,
                    "filled_size_usd": 2.0,
                    "filled_shares": 5.0,
                },
            ]
        },
    )
    _write_jsonl(resolutions, [{"condition_id": condition_id, "direction": "UP", "source": "test"}])
    _write_jsonl(
        redeem_events,
        [
            {
                "status": "STATE_CONFIRMED",
                "conditions": [{"condition_id": condition_id, "estimated_value_usd": 1.5}],
            },
            {
                "status": "ZERO_POSITION_BALANCE",
                "conditions": [{"condition_id": condition_id, "estimated_value_usd": 0.5}],
            }
        ],
    )

    estimate = ledger_redeemable_estimate(
        ledger_path=ledger,
        resolutions_path=resolutions,
        redeem_events_path=redeem_events,
    )

    assert estimate["status"] == "ESTIMATE"
    assert estimate["resolved_winning_fills"] == 1
    assert estimate["total_redeemable_locked_usd"] == 3.0
    assert estimate["conditions"][0]["local_redeemed_usd"] == 2.0
    assert estimate["conditions"][0]["estimated_value_usd"] == 3.0


def test_redeem_deadman_incidents_after_threshold_age(tmp_path: Path) -> None:
    state_path = tmp_path / "deadman.json"
    _write_json(
        state_path,
        {
            "status": "WATCH_REDEEMABLE_LOCKED",
            "first_seen_above_threshold_at": "2020-01-01T00:00:00Z",
            "condition_set_hash": hashlib.sha256(b"").hexdigest()[:24],
        },
    )
    state = update_redeem_deadman(
        report={
            "summary": {
                "redeemable_locked_usd": 21.25,
                "locked_value_source": "ledger_estimate_data_api_unavailable",
            }
        },
        state_path=state_path,
        threshold_usd=20.0,
        threshold_age_s=60.0,
    )

    assert state["status"] == "INCIDENT_REDEEMABLE_LOCKED"
    assert state["incident"] is True
    assert state["next_action"] == "run own-position redeemer now"


def test_redeem_deadman_acknowledges_relayer_quota_wait(tmp_path: Path) -> None:
    state_path = tmp_path / "deadman.json"
    redeemer_state_path = tmp_path / "redeemer.json"
    _write_json(
        state_path,
        {
            "status": "WATCH_REDEEMABLE_LOCKED",
            "first_seen_above_threshold_at": "2020-01-01T00:00:00Z",
            "condition_set_hash": hashlib.sha256(b"").hexdigest()[:24],
        },
    )
    _write_json(
        redeemer_state_path,
        {
            "status": "RELAYER_QUOTA_EXHAUSTED",
            "next_retry_at": "2100-01-01T00:00:00Z",
        },
    )

    state = update_redeem_deadman(
        report={
            "summary": {
                "redeemable_locked_usd": 49.05,
                "locked_value_source": "data_api_positions",
            }
        },
        state_path=state_path,
        redeemer_state_path=redeemer_state_path,
        threshold_usd=20.0,
        threshold_age_s=60.0,
    )

    assert state["status"] == "WATCH_REDEEMABLE_LOCKED"
    assert state["incident"] is False
    assert state["acknowledged_reason"] == "QUOTA_WAIT_UNTIL_NEXT_RETRY"
    assert state["acknowledged_until"] == "2100-01-01T00:30:00Z"
    assert state["next_action"] == "continue 10-minute refresh/redeem cycle"


def test_redeem_deadman_incidents_after_quota_wait_grace(tmp_path: Path) -> None:
    state_path = tmp_path / "deadman.json"
    redeemer_state_path = tmp_path / "redeemer.json"
    _write_json(
        state_path,
        {
            "status": "WATCH_REDEEMABLE_LOCKED",
            "first_seen_above_threshold_at": "2020-01-01T00:00:00Z",
            "condition_set_hash": hashlib.sha256(b"").hexdigest()[:24],
        },
    )
    _write_json(
        redeemer_state_path,
        {
            "status": "RELAYER_QUOTA_EXHAUSTED",
            "next_retry_at": "2020-01-01T00:00:00Z",
        },
    )

    state = update_redeem_deadman(
        report={
            "summary": {
                "redeemable_locked_usd": 49.05,
                "locked_value_source": "data_api_positions",
            }
        },
        state_path=state_path,
        redeemer_state_path=redeemer_state_path,
        threshold_usd=20.0,
        threshold_age_s=60.0,
    )

    assert state["status"] == "INCIDENT_REDEEMABLE_LOCKED"
    assert state["incident"] is True
    assert "acknowledged_reason" not in state


def test_redeem_deadman_resets_timer_when_condition_set_changes(tmp_path: Path) -> None:
    state_path = tmp_path / "deadman.json"
    _write_json(
        state_path,
        {
            "status": "WATCH_REDEEMABLE_LOCKED",
            "first_seen_above_threshold_at": "2020-01-01T00:00:00Z",
            "condition_set_hash": "old-condition-set",
        },
    )
    state = update_redeem_deadman(
        report={
            "summary": {
                "redeemable_locked_usd": 21.25,
                "locked_value_source": "ledger_estimate_data_api_unavailable",
            },
            "ledger_estimate": {
                "conditions": [{"condition_id": "0x" + ("12" * 32), "estimated_value_usd": 21.25}]
            },
        },
        state_path=state_path,
        threshold_usd=20.0,
        threshold_age_s=60.0,
    )

    assert state["status"] == "WATCH_REDEEMABLE_LOCKED"
    assert state["incident"] is False
    assert state["age_s"] < 5.0
    assert state["condition_count"] == 1
