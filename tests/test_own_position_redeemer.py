import json
import sys
from datetime import datetime, timezone

import scripts.run_own_position_redeemer as redeemer
from scripts.run_own_position_redeemer import (
    _active_quota_wait,
    _is_proxy_sdk_unsupported_error,
    _is_zero_position_balance_precheck,
    _load_relayer_symbols,
    _proxy_sdk_unsupported_update,
    _quota_state_from_error,
    _zero_balance_skip_update,
)


def test_quota_error_records_retry_timestamp() -> None:
    base = datetime(2026, 7, 8, 9, 42, 29, tzinfo=timezone.utc)
    state = _quota_state_from_error(
        "RelayerApiException: quota exceeded: 0 units remaining, resets in 60 seconds",
        base_ts=base,
    )

    assert state["status"] == "RELAYER_QUOTA_EXHAUSTED"
    assert state["relayer_quota_reset_s"] == 60.0
    assert state["next_retry_at"] == "2026-07-08T09:43:29Z"


def test_active_quota_wait_accepts_prior_generic_error() -> None:
    wait = _active_quota_wait(
        {
            "status": "ERROR",
            "generated_at": "2999-01-01T00:00:00Z",
            "error": "RelayerApiException: quota exceeded: 0 units remaining, resets in 60 seconds",
        }
    )

    assert wait["status"] == "RELAYER_QUOTA_EXHAUSTED"
    assert wait["next_retry_at"] == "2999-01-01T00:01:00Z"
    assert wait["previous_status"] == "ERROR"


def test_zero_balance_precheck_is_detected_from_relayer_result() -> None:
    result = {
        "status": "PRECHECK_SKIPPED",
        "wait_result": {"error": "redeem skipped: zero position balance"},
    }

    assert _is_zero_position_balance_precheck(result) is True
    assert _is_zero_position_balance_precheck({"status": "PRECHECK_SKIPPED", "error": "real relayer error"}) is False


def test_zero_balance_skip_update_marks_benign_rc0_state() -> None:
    update = _zero_balance_skip_update(result={"status": "PRECHECK_SKIPPED"})

    assert update["status"] == "SKIPPED_ZERO_BALANCE"
    assert update["executed"] is False
    assert update["skipped"] is True
    assert update["skip_reason"] == "PRECHECK_SKIPPED_ZERO_POSITION_BALANCE"


def test_proxy_sdk_unsupported_error_marks_rc0_state() -> None:
    error = "RuntimeError: RelayerClientException: expected safe 0xabc is not deployed"
    update = _proxy_sdk_unsupported_update(error)

    assert _is_proxy_sdk_unsupported_error(error) is True
    assert update["status"] == "RELAYER_PROXY_SDK_UNSUPPORTED"
    assert update["executed"] is False
    assert update["skipped"] is True
    assert "proxy-capable" in update["next_action"]


def test_load_relayer_symbols_supports_installed_sdk_transaction_shape() -> None:
    _, _, transaction = _load_relayer_symbols()

    row = transaction(to="0x" + "22" * 20, data="0x", value="0")

    assert row.to == "0x" + "22" * 20
    assert row.data == "0x"
    assert row.value == "0"


def test_data_api_zero_balance_exception_marks_benign_rc0_state(monkeypatch, tmp_path) -> None:
    condition_id = "0x" + "11" * 32
    positions = tmp_path / "positions.json"
    output = tmp_path / "own_redeemer_state.json"
    events = tmp_path / "own_redeem_events.jsonl"
    positions.write_text(
        json.dumps(
            {
                "redeemable_positions": [
                    {
                        "condition_id": condition_id,
                        "market_slug": "btc-updown-5m-test",
                        "redeemable_value_usd": 5.0,
                        "neg_risk": False,
                    }
                ]
            }
        )
    )

    def fail_zero_balance(candidates, *, wait):
        raise RuntimeError(
            "RelayerApiException[status_code=400, "
            "error_message={'error': 'PRECHECK_SKIPPED: redeem skipped: zero position balance'}]"
        )

    monkeypatch.setattr(redeemer, "_execute_redeem_batch", fail_zero_balance)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_own_position_redeemer.py",
            "--positions",
            str(positions),
            "--output",
            str(output),
            "--events",
            str(events),
            "--execute",
            "--wait",
        ],
    )

    assert redeemer.main() == 0
    state = json.loads(output.read_text())
    assert state["candidate_source"] == "data_api_redeemable_position"
    assert state["status"] == "SKIPPED_ZERO_BALANCE"
    assert state["executed"] is False
    assert state["skipped"] is True
    assert state["result"]["status"] == "ZERO_POSITION_BALANCE_CLEARED"


def test_proxy_sdk_unsupported_exception_marks_rc0_state(monkeypatch, tmp_path) -> None:
    condition_id = "0x" + "11" * 32
    positions = tmp_path / "positions.json"
    output = tmp_path / "own_redeemer_state.json"
    events = tmp_path / "own_redeem_events.jsonl"
    positions.write_text(
        json.dumps(
            {
                "redeemable_positions": [
                    {
                        "condition_id": condition_id,
                        "market_slug": "btc-updown-5m-test",
                        "redeemable_value_usd": 5.0,
                        "neg_risk": False,
                    }
                ]
            }
        )
    )

    def fail_safe_not_deployed(candidates, *, wait):
        raise RuntimeError("RelayerClientException: expected safe 0xabc is not deployed")

    monkeypatch.setattr(redeemer, "_execute_redeem_batch", fail_safe_not_deployed)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_own_position_redeemer.py",
            "--positions",
            str(positions),
            "--output",
            str(output),
            "--events",
            str(events),
            "--execute",
            "--wait",
        ],
    )

    assert redeemer.main() == 0
    state = json.loads(output.read_text())
    assert state["status"] == "RELAYER_PROXY_SDK_UNSUPPORTED"
    assert state["executed"] is False
    assert state["skipped"] is True
    assert "expected safe 0xabc is not deployed" in state["error"]
