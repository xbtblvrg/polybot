import json
from pathlib import Path

from scripts.report_successor_dossier import build_dossier


CANDIDATE = "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _base_fixture(tmp_path: Path, *, routing_member: dict | None = None, recent: dict | None = None) -> dict[str, Path]:
    paths = {
        "queue": tmp_path / "queue.json",
        "routing": tmp_path / "routing.json",
        "shadow_seats": tmp_path / "shadow_seats.json",
        "probe": tmp_path / "probe.json",
        "temporal": tmp_path / "temporal.json",
        "deadman": tmp_path / "deadman.json",
    }
    _write_json(
        paths["queue"],
        {
            "ranked_members": [
                {
                    "wallet": CANDIDATE,
                    "name": "leaderboard_crypto_c5391c6dfd",
                    "queue_rank": 1,
                    "queue_source": "clearance_ready",
                    "ready_for_live": True,
                    "clearance_ready": True,
                    "resolved_pnl": 1.420953,
                    "recent_fill_windows": 10,
                }
            ]
        },
    )
    by_member = {CANDIDATE: routing_member} if routing_member else {}
    _write_json(
        paths["routing"],
        {
            "summary": {
                "validation_elapsed_hours": 11.25,
                "would_submit_windows": 13,
                "runtime_selected_wallet": "0xe6dbf5caed58ae12d67344d928a9f9bb84407dac",
                "fee_gate_calibration_retained": {"by_member": by_member},
            }
        },
    )
    _write_json(paths["shadow_seats"], {"enabled": True, "seats": []})
    _write_json(
        paths["probe"],
        {
            "ranked_candidates": [
                {
                    "wallet": CANDIDATE,
                    "btc5m_buys": 493,
                    "btc5m_trades": 493,
                    "inband_025_050_buy_share_pct": 100.0,
                    "median_buy_entry_offset_s": 281.0,
                    "latest_trade_age_h": 0.146945,
                    "p1_promotion_eligible": False,
                    "p1_reject_reasons": ["median_entry_offset_not_lt_60s"],
                }
            ]
        },
    )
    _write_json(
        paths["temporal"],
        {
            "wallets": [
                {
                    "wallet": CANDIDATE,
                    "classification": "FADING",
                    "all": {"pnl_usd": 81.500708, "resolved_trades": 953},
                    "recent": recent
                    or {
                        "pnl_usd": -164.614426,
                        "resolved_trades": 50,
                        "win_rate_pct": 28.0,
                    },
                    "regime_profiles": {
                        "weekday": {"pnl_usd": -279.686792},
                        "weekend": {"pnl_usd": 361.1875},
                    },
                }
            ]
        },
    )
    _write_json(
        paths["deadman"],
        {
            "rows": [
                {
                    "wallet": CANDIDATE,
                    "admission_check_pass": True,
                    "btc5m_buy_count_24h": 394,
                    "policy_compatible_inband_buy_count_24h": 356,
                    "freshest_buy_lag_s": 236.356851,
                    "temporal_slice_gate_pass": True,
                    "fail_reasons": [],
                }
            ]
        },
    )
    return paths


def test_successor_dossier_preserves_absent_routing_digits(tmp_path: Path) -> None:
    paths = _base_fixture(tmp_path)

    report = build_dossier(
        queue_path=paths["queue"],
        routing_shadow_path=paths["routing"],
        shadow_seats_path=paths["shadow_seats"],
        corrected_probe_path=paths["probe"],
        temporal_path=paths["temporal"],
        deadman_gate_path=paths["deadman"],
    )

    summary = report["summary"]
    assert summary["candidate_wallet"] == CANDIDATE
    assert summary["queue_rank"] == 1
    assert summary["status"] == "PRESTAGED_NO_LIVE_CHANGE"
    assert summary["routing_status"] == "NOT_SEATED_IN_ROUTING_SHADOW_RETAINED"
    assert summary["routing_measured_windows"] is None
    assert summary["routing_post_fee_pnl_usd"] is None
    assert summary["routing_would_fill_count"] is None
    assert summary["fee_coverage_status"] == "NO_MEMBER_FEE_CALIBRATION_ROW"
    assert summary["gap_sigma_status"] == "NOT_COMPUTABLE_MISSING_BREAKEVEN_PAYOFF_SHAPE"
    assert report["corrected_probe"]["btc5m_buys"] == 493
    assert report["deadman_corrected_gate"]["admission_check_pass"] is True
    assert report["temporal_profile"]["classification"] == "FADING"


def test_successor_dossier_computes_sigma_when_payoff_shape_exists(tmp_path: Path) -> None:
    paths = _base_fixture(
        tmp_path,
        routing_member={
            "measured_unique_windows": 4,
            "post_fee_pnl_usd": 0.5,
            "fee_gated_intents": 4,
            "measurable_resolved_intents": 4,
            "resolved_intents": 4,
            "unresolved_intents": 0,
            "expected_fee_usd_sum": 0.2,
        },
        recent={
            "resolved_trades": 4,
            "win_rate_pct": 25.0,
            "avg_win_per_winner_usd": 3.0,
            "avg_loss_per_loser_abs_usd": 1.0,
        },
    )

    report = build_dossier(
        queue_path=paths["queue"],
        routing_shadow_path=paths["routing"],
        shadow_seats_path=paths["shadow_seats"],
        corrected_probe_path=paths["probe"],
        temporal_path=paths["temporal"],
        deadman_gate_path=paths["deadman"],
    )

    summary = report["summary"]
    assert summary["routing_status"] == "PRESENT_IN_ROUTING_SHADOW_RETAINED"
    assert summary["routing_measured_windows"] == 4
    assert summary["fee_coverage_status"] == "PRESENT"
    assert summary["gap_sigma_status"] == "COMPUTED"
    assert summary["gap_sigma_pp"] == 21.650635
    assert summary["gap_in_sigma"] == 0.0
    assert report["gap_sigma"]["required_win_rate_pct"] == 25.0


def test_successor_dossier_distinguishes_shadow_seated_without_fee_rows(tmp_path: Path) -> None:
    paths = _base_fixture(tmp_path)
    routing = json.loads(paths["routing"].read_text())
    routing["summary"]["shadow_candidate_seat_count"] = 1
    routing["member_evidence"] = [
        {
            "source_wallet": CANDIDATE,
            "probe_status": "PASS",
            "fresh_candidate_intents": 0,
            "fresh_candidate_intents_after_expected_fee_gate": 0,
            "sample_intents": 0,
            "filter_attrition": {"routeable_signals": 0, "would_submit": 0},
        }
    ]
    _write_json(paths["routing"], routing)

    report = build_dossier(
        queue_path=paths["queue"],
        routing_shadow_path=paths["routing"],
        shadow_seats_path=paths["shadow_seats"],
        corrected_probe_path=paths["probe"],
        temporal_path=paths["temporal"],
        deadman_gate_path=paths["deadman"],
    )

    summary = report["summary"]
    assert summary["routing_status"] == "SEATED_NO_FEE_CALIBRATION_ROWS_YET"
    assert summary["routing_measured_windows"] == 0
    assert summary["routing_would_fill_count"] == 0
    assert summary["fee_coverage_status"] == "SEATED_NO_FEE_CALIBRATION_ROWS_YET"
    assert report["routing_shadow"]["candidate_probe_status"] == "PASS"
    assert report["fee_calibration_coverage"]["sample_intents"] == 0


def test_successor_dossier_uses_shadow_seat_probe_when_routing_refresh_lags(tmp_path: Path, monkeypatch) -> None:
    paths = _base_fixture(tmp_path)
    _write_json(
        paths["shadow_seats"],
        {
            "enabled": True,
            "seats": [
                {
                    "source_wallet": CANDIDATE,
                    "candidate_id": "deadman_microprobe_c5391c6dfd",
                    "policy_id": "deadman_microprobe_0.10_cap_0.5_le_25",
                }
            ],
        },
    )
    probe_path = tmp_path / "data" / "research" / "wallet_copy_live_execution_probe_deadman_microprobe_c5391c6dfd.json"
    _write_json(
        probe_path,
        {
            "status": "LIVE_ARMED_NO_FRESH_INTENTS",
            "paper_only": True,
            "live_orders_allowed": False,
            "candidate_intent_summary": {
                "fresh_candidate_intents": 0,
                "fresh_candidate_intents_after_expected_fee_gate": 0,
                "sample_intents": [],
            },
        },
    )
    monkeypatch.setattr("scripts.report_successor_dossier.ROOT", tmp_path)

    report = build_dossier(
        queue_path=paths["queue"],
        routing_shadow_path=paths["routing"],
        shadow_seats_path=paths["shadow_seats"],
        corrected_probe_path=paths["probe"],
        temporal_path=paths["temporal"],
        deadman_gate_path=paths["deadman"],
    )

    summary = report["summary"]
    assert summary["routing_status"] == "SHADOW_SEAT_CONFIGURED_AWAITING_ROUTING_SHADOW_REFRESH"
    assert summary["fee_coverage_status"] == "SHADOW_SEAT_CONFIGURED_NO_FEE_ROWS_YET"
    assert report["routing_shadow"]["candidate_probe_status"] == "LIVE_ARMED_NO_FRESH_INTENTS"
    assert report["shadow_candidate_probe"]["live_orders_allowed"] is False
