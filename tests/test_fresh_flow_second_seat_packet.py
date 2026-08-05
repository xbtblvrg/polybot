import json
import sys

from scripts import report_fresh_flow_second_seat_packet as packet_mod
from scripts.report_fresh_flow_second_seat_packet import build_packet
from src.wallet_copy.store import atomic_write_json as real_atomic_write_json


def _queue_row(wallet: str, *, remote_buys: int, remote_policy: int, clearance_ready: bool = True) -> dict:
    return {
        "queue_rank": 1,
        "wallet": wallet,
        "name": f"candidate_{wallet[-4:]}",
        "clearance_ready": clearance_ready,
        "clearance": {
            "paper_pnl_usd": 4.2,
            "resolved_orders": 13,
            "copyable_buy_events": 13,
            "candidate_clob_backed_orders": 13,
            "attributable_reject_ratio": 0.31,
        },
        "fresh_flow_rank": {
            "rank_source": "remote_dataapi_24h",
            "remote_dataapi_btc5m_buys_24h": remote_buys,
            "remote_dataapi_policy_compatible_inband_buy_rows_24h": remote_policy,
            "remote_dataapi_latest_trade_age_h": 0.03,
            "remote_rows_saturated": remote_buys >= 1000,
        },
    }


def test_fresh_flow_second_seat_packet_uses_fable_remote_policy_hard_filter() -> None:
    hard_wallet = "0x1111111111111111111111111111111111111111"
    remote_only_wallet = "0x2222222222222222222222222222222222222222"
    packet = build_packet(
        queue={
            "generated_at": "2099-01-01T00:00:00Z",
            "ranked_members": [
                _queue_row(hard_wallet, remote_buys=1000, remote_policy=998),
                _queue_row(remote_only_wallet, remote_buys=12, remote_policy=0),
            ],
        },
        probe={"generated_at": "2099-01-01T00:00:00Z", "summary": {"wallets": 2}},
        excluded_wallets=set(),
        fallback_wallet=remote_only_wallet,
        fallback_deadline="2099-01-01T21:00:00Z",
        top_limit=4,
        audit_limit=10,
    )

    assert packet["summary"]["hard_pass_count"] == 1
    assert packet["top_hard_passers"][0]["wallet"] == hard_wallet
    assert packet["top_hard_passers"][0]["hard_filter"][
        "remote_dataapi_policy_compatible_inband_buy_rows_24h_gt_0"
    ] is True
    assert packet["top_hard_passers"][0]["remote_rows_saturated"] is True
    assert packet["top_ranked_rows_for_audit"][1]["hard_pass"] is False
    assert packet["summary"]["recommendation"] == "FABLE_ROTATION_DECISION_READY_HARD_PASSERS_PRESENT"


def test_fresh_flow_second_seat_packet_excludes_named_wallets() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    packet = build_packet(
        queue={"ranked_members": [_queue_row(wallet, remote_buys=1000, remote_policy=1000)]},
        probe={},
        excluded_wallets={wallet},
        fallback_wallet=wallet,
        fallback_deadline="2099-01-01T21:00:00Z",
        top_limit=4,
        audit_limit=10,
    )

    assert packet["summary"]["hard_pass_count"] == 0
    assert packet["fallback"]["excluded"] is True


def test_fresh_flow_second_seat_packet_cli_uses_atomic_write_json(tmp_path, monkeypatch) -> None:
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "packet.json"
    queue_path.write_text(
        json.dumps(
            {
                "ranked_members": [
                    _queue_row(
                        "0x1111111111111111111111111111111111111111",
                        remote_buys=1000,
                        remote_policy=1000,
                    )
                ]
            }
        ),
        encoding="utf-8",
    )
    probe_path.write_text(json.dumps({"summary": {"wallets": 1}}), encoding="utf-8")
    calls: list[str] = []

    def recording_atomic(path, payload, *, compact=False):
        calls.append(str(path))
        real_atomic_write_json(path, payload, compact=compact)

    monkeypatch.setattr(packet_mod, "atomic_write_json", recording_atomic)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_fresh_flow_second_seat_packet.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
        ],
    )

    assert packet_mod.main() == 0
    assert calls == [str(output_path)]
    assert json.loads(output_path.read_text(encoding="utf-8"))["summary"]["hard_pass_count"] == 1
