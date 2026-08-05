from __future__ import annotations

from scripts.build_realtime_runtime_proof import (
    build_realtime_runtime_tracker_state,
)
from src.wallet_copy.profit_engine import fast_candidate_policies


class FakeCLOB:
    last_route_report = {
        "status": "PASS",
        "route_class": "LOCAL_RELAY",
        "route_report_id": "rr_unit",
        "routed_host": "127.0.0.1",
        "source_base_override_configured": True,
        "request_fingerprint": "req_unit",
    }

    def get_book(self, token_id: str) -> dict:
        return {
            "asset_id": token_id,
            "timestamp": "1783093545000",
            "hash": f"book-{token_id}",
            "asks": [{"price": "0.71", "size": "20"}],
            "bids": [{"price": "0.70", "size": "20"}],
        }


def _policy(policy_id: str):
    return next(policy for policy in fast_candidate_policies() if policy.policy_id == policy_id)


def _polygon_row(
    *,
    tx: str,
    asset: str,
    block_ts: float,
    received_at_s: float,
    wallet: str,
    size: float = 7.43,
) -> dict:
    return {
        "source": "polygon_wss_eth_subscribe",
        "selected_wallet": wallet,
        "maker": wallet,
        "taker": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "transaction_hash": tx,
        "block_ts": block_ts,
        "received_at_s": received_at_s,
        "decoded": {
            "decode_status": "OK",
            "side": "BUY",
            "asset": asset,
            "price": 0.71,
            "size": size,
        },
    }


def test_realtime_runtime_proof_builds_clean_paper_tracker_state():
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    policy_id = "fast_wf_0.05_cap_2_late_high_conviction_minusd_0_all_window"
    rows = [
        _polygon_row(tx=f"0x{i:064x}", asset=f"token-{i}", block_ts=1783093544 + i * 300, received_at_s=1783093545 + i * 300, wallet=wallet)
        for i in range(3)
    ]
    token_meta = {
        f"token-{i}": {
            "market_slug": f"btc-updown-5m-{1783093500 + i * 300}",
            "condition_id": f"condition-{i}",
            "outcome": "Up",
        }
        for i in range(3)
    }

    state = build_realtime_runtime_tracker_state(
        polygon_rows=rows,
        token_meta=token_meta,
        dataapi_seen_keys={(wallet, f"0x{i:064x}") for i in range(3)},
        candidate_id="candidate_rt",
        source_wallet=wallet,
        wallet_name="unit",
        policy=_policy(policy_id),
        clob=FakeCLOB(),
        max_event_age_s=30.0,
        max_proof_rows=10,
        min_required_buy_copy_events=3,
        min_required_market_windows=2,
        slippage_bps=250.0,
    )

    scores = state["summary"]["copy_efficiency"]["event_scores"]
    assert state["status"] == "PASS"
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert len(scores) == 3
    assert {row["copy_status"] for row in scores} == {"COPIED_FILLED"}
    assert {row["fill_source"] for row in scores} == {"clob_book_evidence"}
    assert {row["source"] for row in scores} == {"runtime_tracker_state"}
    assert state["summary"]["candidate_copy_truth_summary"]["fallback_filled_buy_copy_events"] == 0


def test_realtime_runtime_proof_builds_from_rtds_rows():
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    policy_id = "fast_wf_0.05_cap_2_late_high_conviction_minusd_0_all_window"
    rows = [
        {
            "event": "rtds_trade_event",
            "source_wallet": wallet,
            "side": "BUY",
            "asset": f"token-{i}",
            "condition_id": f"condition-{i}",
            "market_slug": f"btc-updown-5m-{1783093500 + i * 300}",
            "price": 0.71,
            "size": 7.43,
            "event_ts": 1783093544 + i * 300,
            "received_at_s": 1783093545 + i * 300,
            "transaction_hash": f"0x{i:064x}",
            "raw": {"outcome": "Up", "slug": f"btc-updown-5m-{1783093500 + i * 300}"},
        }
        for i in range(3)
    ]

    state = build_realtime_runtime_tracker_state(
        polygon_rows=[],
        rtds_rows=rows,
        token_meta={},
        dataapi_seen_keys={(wallet, f"0x{i:064x}") for i in range(3)},
        candidate_id="candidate_rtds",
        source_wallet=wallet,
        wallet_name="unit",
        policy=_policy(policy_id),
        clob=FakeCLOB(),
        max_event_age_s=30.0,
        max_proof_rows=10,
        min_required_buy_copy_events=3,
        min_required_market_windows=2,
        slippage_bps=250.0,
    )

    proof = state["summary"]["realtime_runtime_proof"]
    scores = state["summary"]["copy_efficiency"]["event_scores"]
    assert state["status"] == "PASS"
    assert proof["rtds_rows"] == 3
    assert proof["polygon_rows"] == 0
    assert len(scores) == 3
    assert {row["token_id"] for row in scores} == {"token-0", "token-1", "token-2"}
    assert {row["fill_source"] for row in scores} == {"clob_book_evidence"}


def test_realtime_runtime_proof_rejects_stale_or_policy_mismatched_rows():
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    policy_id = "fast_wf_0.05_cap_2_late_high_conviction_minusd_5_all_window"
    rows = [
        _polygon_row(tx="0x1", asset="token-a", block_ts=1783093544, received_at_s=1783093545, wallet=wallet, size=2.0),
        _polygon_row(tx="0x2", asset="token-b", block_ts=1783093544, received_at_s=1783093600, wallet=wallet),
    ]
    token_meta = {
        "token-a": {"market_slug": "btc-updown-5m-1783093500", "condition_id": "condition-a", "outcome": "Up"},
        "token-b": {"market_slug": "btc-updown-5m-1783093500", "condition_id": "condition-b", "outcome": "Up"},
    }

    state = build_realtime_runtime_tracker_state(
        polygon_rows=rows,
        token_meta=token_meta,
        dataapi_seen_keys={(wallet, "0x1"), (wallet, "0x2")},
        candidate_id="candidate_rt",
        source_wallet=wallet,
        wallet_name="unit",
        policy=_policy(policy_id),
        clob=FakeCLOB(),
        max_event_age_s=30.0,
        max_proof_rows=10,
        min_required_buy_copy_events=3,
        min_required_market_windows=2,
        slippage_bps=250.0,
    )

    proof = state["summary"]["realtime_runtime_proof"]
    assert state["status"] == "ANALYZE"
    assert proof["proof_rows"] == 0
    assert proof["diagnostics"]["policy_reject:wallet_size_below_minimum"] == 1
    assert proof["diagnostics"]["stale_event"] == 1


def test_realtime_runtime_proof_drops_unmapped_or_unreconciled_wss_rows():
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    policy_id = "fast_wf_0.05_cap_2_late_high_conviction_minusd_0_all_window"
    rows = [
        _polygon_row(tx="0x1", asset="token-mapped", block_ts=1783093544, received_at_s=1783093545, wallet=wallet),
        _polygon_row(tx="0x2", asset="token-unmapped", block_ts=1783093544, received_at_s=1783093545, wallet=wallet),
        _polygon_row(tx="0x3", asset="token-mapped", block_ts=1783093544, received_at_s=1783093545, wallet=wallet),
    ]
    token_meta = {
        "token-mapped": {
            "market_slug": "btc-updown-5m-1783093500",
            "condition_id": "condition-a",
            "outcome": "Up",
        },
    }

    state = build_realtime_runtime_tracker_state(
        polygon_rows=rows,
        token_meta=token_meta,
        dataapi_seen_keys={(wallet, "0x1")},
        candidate_id="candidate_rt",
        source_wallet=wallet,
        wallet_name="unit",
        policy=_policy(policy_id),
        clob=FakeCLOB(),
        max_event_age_s=30.0,
        max_proof_rows=10,
        min_required_buy_copy_events=3,
        min_required_market_windows=2,
        slippage_bps=250.0,
    )

    proof = state["summary"]["realtime_runtime_proof"]
    assert proof["proof_rows"] == 1
    assert proof["dataapi_reconciliation_required"] is True
    assert proof["diagnostics"]["token_mapping_missing"] == 1
    assert proof["diagnostics"]["dataapi_reconciliation_missing"] == 1


def test_ingest_semantic_key_dedupes_polygon_and_dataapi_by_tx():
    from src.wallet_copy.ingest import _prefer_event, _semantic_event_key
    from src.wallet_copy.models import WalletEvent

    base = {
        "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "wallet_name": "unit",
        "row_type": "trade",
        "action": "BUY",
        "condition_id": "0xcondition",
        "market_slug": "btc-updown-5m-1783093500",
        "outcome": "Up",
        "price": 0.71,
        "size": 7.43,
        "usdc_size": 5.2753,
        "event_ts": 1783093544.0,
        "token_id": "token-a",
        "transaction_hash": "0xabc",
    }
    polygon = WalletEvent(**{**base, "observed_ts": 1783093545.0, "source": "polygon_orderfilled_ws_runtime_proof"})
    dataapi = WalletEvent(**{**base, "observed_ts": 1783093600.0, "source": "polymarket_data_api"})

    assert _semantic_event_key(polygon) == _semantic_event_key(dataapi)
    assert _prefer_event(dataapi, polygon) == polygon
