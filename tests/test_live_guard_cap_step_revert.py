import argparse
import json


E6DB = "0xe6db20932faf0f9780acf75d95c74c9984407dac"


def _overlay() -> dict:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_active_set_auto_degrade_state",
        "updated_at": "2026-07-10T02:18:33Z",
        "latest_e6db_cap_step": {
            "applied_at": "2026-07-10T02:18:33Z",
            "candidate_id": "runtime_auto_degrade_e6db20932f",
            "direction_id": "2026-07-10T02:13Z-fable-e6db-cap6-step",
            "flow_stage": "LIVE/ROTATE",
            "from_max_order_usd": 2.0,
            "from_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
            "mechanical_revert": {
                "consecutive_losing_resolutions": 4,
                "cum_resolved_pnl_from_step_lte_usd": -5.0,
                "requires_fable_ping": False,
                "revert_max_order_usd": 2.0,
                "revert_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
            },
            "source_wallet": E6DB,
            "to_max_order_usd": 3.0,
            "to_policy_id": "fast_wf_0.10_cap_6_all_prices_minusd_0_all_window",
        },
        "members": [
            {
                "candidate_id": "runtime_auto_degrade_e6db20932f",
                "source_wallet": E6DB,
                "policy_id": "fast_wf_0.10_cap_6_all_prices_minusd_0_all_window",
                "max_order_usd": 3.0,
                "status": "FABLE_0213_E6DB_CAP6_STEP",
                "policy": {
                    "policy_id": "fast_wf_0.10_cap_6_all_prices_minusd_0_all_window",
                    "max_order_usd": 3.0,
                    "max_price": 0.5,
                    "wallet_fraction": 0.1,
                },
            },
            {
                "candidate_id": "other",
                "source_wallet": "0x1111111111111111111111111111111111111111",
                "policy_id": "other_policy",
                "max_order_usd": 9.0,
            },
        ],
    }


def _order(ts: str, pnl: float) -> dict:
    return {
        "submitted_at": ts,
        "source_wallet": E6DB,
        "status": "FILLED",
        "market_slug": "btc-updown-5m-1783650000",
        "order_id": f"order-{ts}",
        "test_pnl_usd": pnl,
    }


def _install_fake_scoring(monkeypatch):
    import scripts.run_wallet_copy_live_guard as guard

    monkeypatch.setattr(guard, "load_resolutions", lambda _path: {})

    def fake_score_order(order, _resolutions):
        return {
            "status": "FILLED",
            "resolved": True,
            "cost_usd": abs(float(order["test_pnl_usd"])),
            "payout_usd": 0.0,
            "pnl_usd": float(order["test_pnl_usd"]),
            "market_slug": order.get("market_slug"),
        }

    monkeypatch.setattr(guard, "score_order", fake_score_order)
    return guard


def test_cap6_revert_does_not_front_run_threshold(tmp_path, monkeypatch) -> None:
    guard = _install_fake_scoring(monkeypatch)
    overlay_path = tmp_path / "auto_degrade.json"
    ledger_path = tmp_path / "ledger.json"
    overlay_path.write_text(json.dumps(_overlay()), encoding="utf-8")
    ledger_path.write_text(
        json.dumps(
            {
                "orders": [
                    _order("2026-07-10T02:19:47Z", -2.399999),
                    _order("2026-07-10T02:23:46Z", -2.4),
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "AUTO_DEGRADE_ACTIVE_SET_STATE", overlay_path)

    result = guard._maybe_execute_cap_step_revert(
        argparse.Namespace(live_ledger_state=str(ledger_path), resolutions=str(tmp_path / "resolutions.jsonl")),
        generated_at="2026-07-10T02:55:00Z",
    )

    stored = json.loads(overlay_path.read_text(encoding="utf-8"))
    assert result["status"] == "WATCH"
    assert result["attribution"]["cum_resolved_pnl_usd"] == -4.799999
    assert stored["members"][0]["policy_id"] == "fast_wf_0.10_cap_6_all_prices_minusd_0_all_window"
    assert "latest_e6db_cap_revert" not in stored


def test_cap6_revert_fires_and_rewrites_only_e6db_member(tmp_path, monkeypatch) -> None:
    guard = _install_fake_scoring(monkeypatch)
    overlay_path = tmp_path / "auto_degrade.json"
    ledger_path = tmp_path / "ledger.json"
    overlay_path.write_text(json.dumps(_overlay()), encoding="utf-8")
    ledger_path.write_text(
        json.dumps(
            {
                "orders": [
                    _order("2026-07-10T02:19:47Z", -2.4),
                    _order("2026-07-10T02:23:46Z", -2.4),
                    _order("2026-07-10T02:55:10Z", -0.25),
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "AUTO_DEGRADE_ACTIVE_SET_STATE", overlay_path)

    result = guard._maybe_execute_cap_step_revert(
        argparse.Namespace(live_ledger_state=str(ledger_path), resolutions=str(tmp_path / "resolutions.jsonl")),
        generated_at="2026-07-10T02:56:00Z",
    )

    stored = json.loads(overlay_path.read_text(encoding="utf-8"))
    e6db = stored["members"][0]
    other = stored["members"][1]
    assert result["status"] == "CAP_6_REVERTED"
    assert result["trigger_reason"] == "cum_resolved_pnl_lte_threshold"
    assert stored["latest_e6db_cap_step"]["status"] == "CAP_6_REVERTED"
    assert stored["latest_e6db_cap_step"]["reverted_at"] == "2026-07-10T02:56:00Z"
    assert e6db["policy_id"] == "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
    assert e6db["max_order_usd"] == 2.0
    assert e6db["status"] == "PASS"
    assert guard._candidate_status_live_admissible(e6db)
    assert e6db["mechanical_revert_status"] == "CAP_6_REVERTED_TO_CAP_4"
    assert e6db["status_provenance"] == {
        "flow_stage": "LIVE/DEFEND",
        "direction_id": "2026-07-10T02:54Z-fable-event-driven-cap6-revert",
        "gate_status": "PASS",
        "provenance_status": "CAP_6_REVERTED_TO_CAP_4",
        "prior_status": "FABLE_0213_E6DB_CAP6_STEP",
        "reason": "Mechanical cap-step revert changed policy/size; member status remains gate-recognized.",
        "rule": (
            "live-member status uses gate-recognized vocabulary; Fable/mechanical labels live in provenance fields"
        ),
    }
    assert e6db["policy"]["policy_id"] == "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
    assert e6db["policy"]["max_order_usd"] == 2.0
    assert other["policy_id"] == "other_policy"
    assert other["max_order_usd"] == 9.0
