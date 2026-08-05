import json
from argparse import Namespace

from scripts import run_wallet_copy_live_guard as guard
from src.wallet_copy.copy_efficiency import build_copy_efficiency_report_from_scores
from src.wallet_copy.live_tracker import (
    _intent_time_copyability_proof_scores,
    _merge_intent_time_copyability_proof,
)


def test_live_guard_writes_intent_time_copyability_proof_sidecar(tmp_path):
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "intent_time_proof.json"
    probe_path.write_text(
        json.dumps(
            {
                "candidate_id": "candidate_a",
                "inventory_best_ask_gate": {
                    "sample_decisions": [
                        {
                            "intent_id": "ci_ok",
                            "source_wallet": "0x1111111111111111111111111111111111111111",
                            "market_slug": "btc-updown-5m-1",
                            "outcome": "Up",
                            "token_id": "token",
                            "source_price": 0.5,
                            "best_ask": 0.5,
                            "max_copy_price": 0.5075,
                            "fillable_usd": 1.0,
                            "fill_ratio": 1.0,
                            "copy_size_usd": 1.0,
                            "event_ts": 10.0,
                            "observed_ts": 10.5,
                            "event_age_s": 1.0,
                            "clob_route_status": "PASS",
                            "instant_fill_status": "PASS",
                            "book_hash": "hash",
                        },
                        {
                            "intent_id": "ci_reject",
                            "source_wallet": "0x2222222222222222222222222222222222222222",
                            "market_slug": "btc-updown-5m-1",
                            "outcome": "Down",
                            "token_id": "token2",
                            "source_price": 0.5,
                            "best_ask": 0.6,
                            "max_copy_price": 0.5075,
                            "fillable_usd": 0.0,
                            "fill_ratio": 0.0,
                            "copy_size_usd": 1.0,
                            "event_ts": 10.0,
                            "observed_ts": 10.5,
                            "event_age_s": 2.0,
                            "clob_route_status": "PASS",
                            "instant_fill_status": "BLOCKED",
                        },
                    ]
                },
            }
        )
    )

    summary = guard._write_intent_time_copyability_proof_state(
        Namespace(
            intent_time_copyability_proof_state=str(output_path),
            intent_time_copyability_proof_retain_rows=100,
        ),
        live_probe_result={
            "runtime_member_count": 2,
            "rows": [{"path": str(probe_path), "candidate_id": "candidate_a"}],
        },
        generated_at="2026-07-21T07:30:00Z",
    )

    payload = json.loads(output_path.read_text())
    assert summary["status"] == "PASS"
    assert payload["summary"]["required_buy_copy_events"] == 1
    assert payload["summary"]["clob_filled_buy_copy_events"] == 1
    assert payload["summary"]["copyability_rejected_buy_events"] == 1
    assert payload["summary"]["distinct_intents"] == 2
    assert payload["summary"]["fresh_distinct_records_this_cycle"] == 2
    assert payload["summary"]["covered_members"] == [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    assert payload["summary"]["covered_members_this_cycle"] == payload["summary"]["covered_members"]
    assert payload["summary"]["covered_member_count_this_cycle"] == 2
    assert payload["summary"]["runtime_member_count_this_cycle"] == 2
    assert payload["summary"]["dropped_runtime_members_this_cycle"] == 0
    assert payload["records"][0]["live_orders_allowed"] is False

    guard._write_intent_time_copyability_proof_state(
        Namespace(
            intent_time_copyability_proof_state=str(output_path),
            intent_time_copyability_proof_retain_rows=100,
        ),
        live_probe_result={
            "runtime_member_count": 2,
            "rows": [{"path": str(probe_path), "candidate_id": "candidate_a"}],
        },
        generated_at="2026-07-21T07:31:00Z",
    )

    payload = json.loads(output_path.read_text())
    assert payload["summary"]["records"] == 2
    assert payload["summary"]["distinct_intents"] == 2
    assert {record["first_observed_at"] for record in payload["records"]} == {"2026-07-21T07:30:00Z"}
    assert {record["last_observed_at"] for record in payload["records"]} == {"2026-07-21T07:31:00Z"}


def test_tracker_merges_intent_time_proof_as_clob_filled_required_buy(tmp_path):
    sidecar = tmp_path / "intent_time_proof.json"
    sidecar.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "generated_at": "2999-01-01T00:00:00Z",
                        "wallet_action": "BUY",
                        "copy_status": "COPIED_FILLED",
                        "fill_source": "clob_book_evidence",
                        "copyability_accepted": True,
                        "profit_policy_accepted": True,
                        "event_age_s": 1.0,
                        "api_latency_s": 1.0,
                        "source_event_ts": 1.0,
                        "copy_size_usd": 1.0,
                        "filled_size_usd": 1.0,
                        "clob_book_status": "OK",
                        "clob_book_admission_relevant": True,
                        "fill_ratio": 1.0,
                    }
                ]
            }
        )
    )
    scores, proof_summary = _intent_time_copyability_proof_scores(str(sidecar))
    report = _merge_intent_time_copyability_proof(
        build_copy_efficiency_report_from_scores([]),
        scores=scores,
        proof_summary=proof_summary,
        max_api_latency_s=10.0,
        max_avg_worse_slippage_bps=500.0,
        require_clob_book_evidence=True,
    )

    assert proof_summary["accepted_records_used"] == 1
    assert report["status"] == "PASS"
    assert report["summary"]["required_buy_copy_events"] == 1
    assert report["summary"]["clob_filled_buy_copy_events"] == 1
