import argparse
import json
from pathlib import Path

import scripts.run_wallet_copy_live_guard as guard
from scripts.run_wide_positive_slice_family import WALLET, run_once


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        measurement=str(tmp_path / "measurement.json"),
        standings=str(tmp_path / "standings.json"),
        preregistration=str(tmp_path / "prereg.json"),
        state=str(tmp_path / "state.json"),
        events=str(tmp_path / "events.jsonl"),
        intents=str(tmp_path / "intents.jsonl"),
        terminals=str(tmp_path / "terminals.jsonl"),
    )


def _standings() -> dict:
    return {
        "manifest_reconciliation": {
            "manifest_identity_exact": True,
            "manifest_id": "manifest",
        },
        "slice_failure_matrix": [
            {
                "wallet": WALLET,
                "move_slice_key": "120-180|0.25-0.50",
                "positive_seed": True,
                "checks": {"current_alpha_slice_eligible": True},
            }
        ],
        "standings": [
            {
                "wallet": WALLET,
                "alpha_move_slices": [
                    {
                        "move_slice_key": "120-180|0.25-0.50",
                        "eligible": True,
                    }
                ],
            }
        ],
    }


def _order(order_id: str, recorded_at: str, *, resolved: bool = True) -> dict:
    return {
        "order_id": order_id,
        "recorded_at": recorded_at,
        "wallet": WALLET,
        "run_id": "run",
        "cohort_id": "cohort",
        "policy_id": "policy",
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-4070908800",
        "outcome": "Down",
        "token_id": "token",
        "fill_price": 0.4,
        "source_price": 0.39,
        "source_shares": 10.0,
        "source_event_ts": 4070908801.0,
        "transaction_hash": f"tx-{order_id}",
        "log_index": order_id,
        "alpha_move_slice": {"move_slice_key": "120-180|0.25-0.50"},
        "resolved": resolved,
        "post_fee_pnl_usd": 1.0 if resolved else None,
        "expected_fee_usd": 0.01 if resolved else None,
        "receipt_to_book_fetch_lag_s": 1.0,
        "filled_cost_usd": 1.0,
        "f1_f4_terminal": {
            "F4_executable_book": "PASS",
            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
        },
    }


def test_family_preregisters_then_excludes_every_baseline_order(tmp_path: Path) -> None:
    args = _args(tmp_path)
    measurement = {
        "policy_id": "policy",
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "orders": [_order("baseline", "2026-07-25T00:00:00+00:00")],
    }
    _write(Path(args.measurement), measurement)
    _write(Path(args.standings), _standings())

    first = run_once(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    assert first["summary"]["prospective_orders"] == 0
    assert prereg["baseline_order_ids"] == ["baseline"]

    measurement["orders"].append(_order("future", "2099-07-25T00:00:00+00:00"))
    _write(Path(args.measurement), measurement)
    second = run_once(args)
    assert second["summary"]["prospective_orders"] == 1
    assert second["summary"]["prospective_resolved_orders"] == 1
    assert second["gates"]["prospective_resolved_gte_50"] is False
    assert second["admission_ready"] is False


def test_family_refuses_tampered_preregistration(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(
        Path(args.measurement),
        {"policy_id": "policy", "cohort": {"run_id": "run", "cohort_id": "cohort"}, "orders": []},
    )
    _write(Path(args.standings), _standings())
    run_once(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    prereg["wallet"] = "0x0000000000000000000000000000000000000001"
    _write(Path(args.preregistration), prereg)

    try:
        run_once(args)
    except RuntimeError as exc:
        assert "checksum mismatch" in str(exc)
    else:
        raise AssertionError("tampered preregistration was accepted")


def test_family_keeps_accrued_orders_when_wide_cut_rolls(tmp_path: Path) -> None:
    args = _args(tmp_path)
    measurement = {
        "policy_id": "policy",
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "orders": [],
    }
    _write(Path(args.measurement), measurement)
    _write(Path(args.standings), _standings())
    run_once(args)

    measurement["orders"] = [_order("future", "2099-07-25T00:00:00+00:00")]
    _write(Path(args.measurement), measurement)
    assert run_once(args)["summary"]["prospective_orders"] == 1

    measurement["cohort"] = {"run_id": "next", "cohort_id": "next-cohort"}
    measurement["orders"] = []
    _write(Path(args.measurement), measurement)
    rolled = run_once(args)
    assert rolled["summary"]["prospective_orders"] == 1
    assert rolled["summary"]["prospective_resolved_orders"] == 1


def test_gate_complete_family_emits_checksum_bound_activation_and_guard_reconstructs(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)
    measurement = {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "wallets": {WALLET: {}},
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "orders": [],
    }
    _write(Path(args.measurement), measurement)
    _write(Path(args.standings), _standings())
    run_once(args)
    measurement["orders"] = [
        _order(f"future-{idx}", f"2099-01-01T00:00:{idx:02d}+00:00")
        for idx in range(50)
    ]
    _write(Path(args.measurement), measurement)

    ready = run_once(args)

    assert ready["admission_ready"] is True
    assert ready["activation"]["status"] == "ACTIVATION_READY"
    assert ready["attrition_funnel"]["policy_copyable"] == 50
    intent, checks, reason = guard._wide_family_activation_validation(
        ready,
        generated_at="2099-01-01T00:00:49+00:00",
    )
    assert reason == "PASS"
    assert all(checks.values())
    assert intent is not None
    assert intent.copy_size_usd == 1.0
    assert intent.market_slug == "btc-updown-5m-4070908800"
    assert intent.metadata["wide_positive_slice_family"]["source_lineage"]["run_id"] == "run"


def test_guard_refuses_activation_checksum_drift_and_stale_window(tmp_path: Path) -> None:
    args = _args(tmp_path)
    measurement = {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "wallets": {WALLET: {}},
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "orders": [],
    }
    _write(Path(args.measurement), measurement)
    _write(Path(args.standings), _standings())
    run_once(args)
    measurement["orders"] = [
        _order(f"future-{idx}", f"2099-01-01T00:00:{idx:02d}+00:00")
        for idx in range(50)
    ]
    _write(Path(args.measurement), measurement)
    ready = run_once(args)
    ready["activation"]["paper_intent"]["limit_price"] = 0.41

    intent, checks, reason = guard._wide_family_activation_validation(
        ready,
        generated_at="2099-01-01T00:05:49+00:00",
    )

    assert intent is None
    assert reason == "ACTIVATION_PROTECTION_GATE_FAILED"
    assert checks["activation_checksum_exact"] is False
    assert checks["paper_intent_checksum_exact"] is False
    assert checks["current_btc5m_window"] is False


def test_gate_complete_family_routes_exact_one_dollar_intent_through_sole_guard(
    monkeypatch,
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)
    measurement = {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "wallets": {WALLET: {}},
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "orders": [],
    }
    _write(Path(args.measurement), measurement)
    _write(Path(args.standings), _standings())
    run_once(args)
    measurement["orders"] = [
        _order(f"future-{idx}", f"2099-01-01T00:00:{idx:02d}+00:00")
        for idx in range(50)
    ]
    _write(Path(args.measurement), measurement)
    ready = run_once(args)
    deadman = tmp_path / "deadman.json"
    ledger = tmp_path / "ledger.json"
    actuator = tmp_path / "actuator.json"
    _write(
        deadman,
        {
            "status": "INCIDENT_ORDER_FLOW_DEAD",
            "can_trade": True,
            "raw_accepted_order_deadman": {"accepted_order_idle_s": 3600},
        },
    )
    _write(ledger, {"orders": []})
    live_args = argparse.Namespace(
        wide_family_live_actuator=True,
        wide_family_state=args.state,
        wide_family_live_actuator_state=str(actuator),
        cross_exchange_deadman_state=str(deadman),
        execute_live=True,
        explicit_live_operator_go=True,
        live_orders_allowed=True,
        operator_approval_id="OP-LIVE-TEST",
        live_ledger_state=str(ledger),
        live_ledger_event_log=str(tmp_path / "ledger-events.jsonl"),
        per_window_fill_cap=1,
    )
    monkeypatch.setattr(
        guard,
        "_parity_capsules",
        lambda intents, **kwargs: (
            [
                {
                    "status": "PASS",
                    "parity_digest": "family-parity",
                    "mismatched_fields": [],
                    "decision_wallet_copy_matches_live_intent": True,
                }
            ],
            {intents[0].condition_id: ["yes-token", intents[0].token_id]},
            [],
        ),
    )

    async def fake_execute(_args, intents, *, source_tag, lane, **kwargs):
        assert source_tag == guard._WIDE_FAMILY_SOURCE
        assert lane == guard._WIDE_FAMILY_LANE
        assert intents[0].copy_size_usd == 1.0
        assert intents[0].mode == "paper"
        return {
            "results": [
                {
                    "status": "submitted",
                    "post_status": "live",
                    "order_id": "wide-family-order",
                    "accepted_at": "2099-01-01T00:00:49+00:00",
                }
            ]
        }

    monkeypatch.setattr(guard, "_execute_cross_exchange_live_route_async", fake_execute)

    result = guard._run_wide_family_live_actuator(
        live_args,
        generated_at="2099-01-01T00:00:49+00:00",
    )

    assert ready["activation"]["status"] == "ACTIVATION_READY"
    assert result["status"] == "LIVE_SUBMITTED"
    assert result["orders_submitted"] == 1
    assert result["orders_accepted"] == 1
    assert result["last_order_id"] == "wide-family-order"
    assert result["single_submitter"] == "scripts/run_wallet_copy_live_guard.py"
