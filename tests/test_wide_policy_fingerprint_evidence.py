import json
from pathlib import Path

from scripts.build_wide_policy_fingerprint_evidence import build_evidence
from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity
from src.wallet_copy.models import WalletEvent
from src.wallet_copy.profit_engine import CandidatePolicy, policy_accepts_event


WALLET = "0x" + "1" * 40
SLICE_A = "000-060|0.25-0.50"
SLICE_B = "060-120|0.25-0.50"


def _manifest(path: Path, run: str, slices: list[str]) -> None:
    path.write_text(
        json.dumps(
            {
                "manifest_id": f"manifest-{run}",
                "score_run_id": run,
                "capture_watch_wallets": [
                    {"wallet": WALLET, "move_slice_keys": slices}
                ],
            }
        ),
        encoding="utf-8",
    )


def test_fingerprint_is_order_stable_and_wallet_scoped() -> None:
    first = wide_policy_identity(
        wallet=WALLET, move_slice_keys=[SLICE_B, SLICE_A, SLICE_A]
    )
    second = wide_policy_identity(wallet=WALLET, move_slice_keys=[SLICE_A, SLICE_B])
    other = wide_policy_identity(
        wallet="0x" + "2" * 40, move_slice_keys=[SLICE_A, SLICE_B]
    )
    assert first["wide_policy_fingerprint"] == second["wide_policy_fingerprint"]
    assert first["wide_policy_fingerprint"] != other["wide_policy_fingerprint"]


def test_rescore_uses_one_fixed_filter_without_pooling_fingerprint_cells(
    tmp_path: Path,
) -> None:
    manifest_a = tmp_path / "manifest-a.json"
    manifest_b = tmp_path / "manifest-b.json"
    _manifest(manifest_a, "run-a", [SLICE_A])
    _manifest(manifest_b, "run-b", [SLICE_A, SLICE_B])
    events = []
    for index in range(200):
        order_id = f"order-{index}"
        run_id = "run-a" if index < 100 else "run-b"
        events.append(
            {
                "event": "wide_exact_policy_paper_order_filled",
                "event_id": f"fill-{index}",
                "order_id": order_id,
                "run_id": run_id,
                "wallet": WALLET,
                "transaction_hash": f"tx-{index}",
                "log_index": str(index),
                "token_id": "token",
                "market_slug": f"btc-updown-5m-{1000 + index * 300}",
                "source_event_ts": 1000 + index,
                "filled_cost_usd": 1.0,
                "alpha_move_slice": {"move_slice_key": SLICE_A},
                "resolved": False,
            }
        )
        events.append(
            {
                "event": "wide_exact_policy_paper_order_resolved",
                "event_id": f"resolve-{index}",
                "order_id": order_id,
                "post_fee_pnl_usd": 0.1,
                "resolved": True,
            }
        )
    evidence = build_evidence(
        ledger_rows=events, manifests=[manifest_a, manifest_b]
    )
    assert evidence["fingerprint_cell_count"] == 2
    assert {
        cell["observed_same_fingerprint"]["resolved"] for cell in evidence["cells"]
    } == {100}
    fixed_a = next(
        cell
        for cell in evidence["cells"]
        if cell["identity"]["move_slice_keys"] == [SLICE_A]
    )
    assert fixed_a["fixed_policy_full_stream_rescore"]["resolved"] == 200
    assert fixed_a["fixed_policy_full_stream_rescore"]["f1_pass"] is True
    assert (
        fixed_a["resolution_evidence_summary"][
            "matured_unresolved_window_count"
        ]
        == 0
    )


def test_rescore_exposes_matured_unresolved_windows_for_exact_fingerprint(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.json"
    _manifest(manifest, "run-a", [SLICE_A])
    evidence = build_evidence(
        ledger_rows=[
            {
                "event": "wide_exact_policy_paper_order_filled",
                "event_id": "fill-1",
                "order_id": "order-1",
                "run_id": "run-a",
                "wallet": WALLET,
                "transaction_hash": "tx-1",
                "log_index": "1",
                "token_id": "token",
                "market_slug": "btc-updown-5m-1000",
                "source_event_ts": 1000,
                "filled_cost_usd": 1.0,
                "alpha_move_slice": {"move_slice_key": SLICE_A},
                "resolved": False,
            }
        ],
        manifests=[manifest],
    )
    summary = evidence["cells"][0]["resolution_evidence_summary"]
    assert summary["matured_unresolved_windows"] == [
        "btc-updown-5m-1000"
    ]


def test_rescore_consumes_canonical_resolution_index_directly(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.json"
    _manifest(manifest, "run-a", [SLICE_A])
    evidence = build_evidence(
        ledger_rows=[
            {
                "event": "wide_exact_policy_paper_order_filled",
                "event_id": "fill-1",
                "order_id": "order-1",
                "run_id": "run-a",
                "wallet": WALLET,
                "transaction_hash": "tx-1",
                "log_index": "1",
                "token_id": "up-token",
                "condition_id": "condition-1",
                "market_slug": "btc-updown-5m-1000",
                "source_event_ts": 1000,
                "fill_price": 0.51,
                "filled_shares": 2.0,
                "filled_cost_usd": 1.0,
                "alpha_move_slice": {"move_slice_key": SLICE_A},
                "resolved": False,
            }
        ],
        manifests=[manifest],
        resolution_rows=[
            {
                "market_slug": "btc-updown-5m-1000",
                "condition_id": "condition-1",
                "direction": "UP",
                "yes_token": "up-token",
                "no_token": "down-token",
                "source": "canonical-test",
            }
        ],
    )
    cell = evidence["cells"][0]
    assert cell["fixed_policy_full_stream_rescore"]["resolved"] == 1
    assert cell["fixed_policy_full_stream_rescore"]["post_fee_pnl_usd"] > 0
    assert cell["fixed_policy_full_stream_rescore"]["advisory_only"] is True
    venue = cell["venue_executable_full_stream_rescore"]
    assert venue["venue_order_type"] == "taker"
    assert venue["venue_executable_resolved"] == 1
    assert venue["venue_unreachable_resolved"] == 0
    assert venue["venue_reachable_share_pct"] == 100.0
    assert venue["post_fee_pnl_usd"] == cell["fixed_policy_full_stream_rescore"][
        "post_fee_pnl_usd"
    ]
    maker = cell["maker_venue_executable_full_stream_rescore"]
    assert maker["venue_order_type"] == "maker"
    assert maker["venue_executable_resolved"] == 0
    assert maker["venue_unreachable_resolved"] == 1
    assert maker["venue_reachable_share_pct"] == 0.0
    assert (
        cell["resolution_evidence_summary"]["matured_unresolved_window_count"]
        == 0
    )


def test_source_history_requires_exact_fingerprint_and_our_price_evidence(
    tmp_path: Path,
) -> None:
    identity = wide_policy_identity(wallet=WALLET, move_slice_keys=[SLICE_A])
    manifest = tmp_path / "manifest.json"
    _manifest(manifest, "run-a", [SLICE_A])
    base = {
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": WALLET,
        "wide_policy_fingerprint": identity["wide_policy_fingerprint"],
        "wide_policy_identity": identity,
        "transaction_hash": "tx-acquired",
        "token_id": "up-token",
        "condition_id": "condition-1",
        "market_slug": "btc-updown-5m-1000",
        "source_event_ts": 1000,
        "fill_price": 0.5,
        "filled_shares": 2.0,
        "filled_cost_usd": 1.0,
        "receipt_to_book_fetch_lag_s": 3.0,
        "alpha_move_slice": {"move_slice_key": SLICE_A},
        "resolved": False,
    }
    acquisition = {
        "acquisition_authority": {
            "wallet": WALLET,
            "wide_policy_fingerprint": identity["wide_policy_fingerprint"],
            "move_slice_keys": [SLICE_A],
        },
        "orders": [
            {
                **base,
                "order_id": "honest",
                "our_price_evidence": {
                    "source": "clob_rest_book_snapshot",
                    "asks": [{"price": 0.5, "size": 10}],
                },
            },
            {**base, "order_id": "source-price-only", "transaction_hash": "tx-2"},
            {
                **base,
                "order_id": "cross-fp",
                "transaction_hash": "tx-3",
                "wide_policy_fingerprint": "wrong",
                "our_price_evidence": {
                    "source": "clob_rest_book_snapshot",
                    "asks": [{"price": 0.5, "size": 10}],
                },
            },
        ],
    }
    evidence = build_evidence(
        ledger_rows=[],
        manifests=[manifest],
        source_history_acquisition=acquisition,
    )

    assert evidence["source_history_acquisition"]["rows_seen"] == 3
    assert (
        evidence["source_history_acquisition"]["rows_admitted_exact_fp"] == 1
    )
    assert (
        evidence["source_history_acquisition"][
            "source_price_only_rows_admitted"
        ]
        == 0
    )
    assert evidence["cells"][0]["observed_same_fingerprint"]["observed_fills"] == 1


def test_source_history_does_not_pool_into_another_same_wallet_fingerprint(
    tmp_path: Path,
) -> None:
    manifest_a = tmp_path / "manifest-a.json"
    manifest_b = tmp_path / "manifest-b.json"
    _manifest(manifest_a, "run-a", [SLICE_A])
    _manifest(manifest_b, "run-b", [SLICE_A, SLICE_B])
    identity = wide_policy_identity(wallet=WALLET, move_slice_keys=[SLICE_A])
    acquired = {
        "paper_only": True,
        "live_orders_allowed": False,
        "wallet": WALLET,
        "wide_policy_fingerprint": identity["wide_policy_fingerprint"],
        "wide_policy_identity": identity,
        "transaction_hash": "tx-acquired",
        "token_id": "token",
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-1000",
        "source_event_ts": 1000,
        "fill_price": 0.5,
        "filled_shares": 2.0,
        "filled_cost_usd": 1.0,
        "receipt_to_book_fetch_lag_s": 3.0,
        "alpha_move_slice": {"move_slice_key": SLICE_A},
        "our_price_evidence": {
            "source": "clob_rest_book_snapshot",
            "asks": [{"price": 0.5, "size": 10}],
        },
        "resolved": False,
    }
    evidence = build_evidence(
        ledger_rows=[
            {
                "event": "wide_exact_policy_paper_order_filled",
                "event_id": "fill-existing",
                "order_id": "existing",
                "run_id": "run-b",
                "wallet": WALLET,
                "transaction_hash": "tx-existing",
                "log_index": "1",
                "token_id": "other-token",
                "market_slug": "btc-updown-5m-1300",
                "source_event_ts": 1300,
                "filled_cost_usd": 1.0,
                "alpha_move_slice": {"move_slice_key": SLICE_B},
                "resolved": False,
            }
        ],
        manifests=[manifest_a, manifest_b],
        source_history_acquisition={
            "acquisition_authority": {
                "wallet": WALLET,
                "wide_policy_fingerprint": identity["wide_policy_fingerprint"],
                "move_slice_keys": [SLICE_A],
            },
            "orders": [acquired],
        },
    )

    exact = next(
        cell
        for cell in evidence["cells"]
        if cell["wide_policy_fingerprint"] == identity["wide_policy_fingerprint"]
    )
    other = next(
        cell
        for cell in evidence["cells"]
        if cell["wide_policy_fingerprint"] != identity["wide_policy_fingerprint"]
    )
    assert exact["fixed_policy_full_stream_rescore"]["observed_fills"] == 1
    assert other["fixed_policy_full_stream_rescore"]["observed_fills"] == 1


def test_live_candidate_policy_enforces_exact_move_slice_set() -> None:
    event = WalletEvent(
        source_wallet=WALLET,
        wallet_name="wide",
        row_type="trade",
        action="BUY",
        condition_id="condition",
        market_slug="btc-updown-5m-1000",
        outcome="Up",
        price=0.4,
        size=10,
        usdc_size=4,
        event_ts=1010,
        observed_ts=1011,
        asset="BTC",
        duration="5m",
    )
    accepted, _ = policy_accepts_event(
        CandidatePolicy(policy_id="fp", move_slice_keys=(SLICE_A,)), event
    )
    refused, reason = policy_accepts_event(
        CandidatePolicy(policy_id="fp", move_slice_keys=(SLICE_B,)), event
    )
    assert accepted is True
    assert refused is False
    assert reason == "move_slice_outside_policy"
