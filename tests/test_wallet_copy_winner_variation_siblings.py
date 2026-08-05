from __future__ import annotations

import json
from types import SimpleNamespace

from scripts.build_wallet_copy_winner_variation_siblings import build_state
from src.wallet_copy.models import CopyIntent
from src.wallet_copy.store import atomic_write_json


def _intent(*, age_s: float, build_ts: float, window_start: int, slug_suffix: str) -> dict:
    price = 0.4
    intent = CopyIntent(
        intent_id=f"ci_{slug_suffix}",
        source_wallet="0xparent",
        wallet_name="live_parent",
        source_event_id=f"event_{slug_suffix}",
        condition_id=f"cond_{slug_suffix}",
        market_slug=f"btc-updown-5m-{window_start}",
        outcome="Up",
        side="YES",
        limit_price=price,
        wallet_usdc_size=10.0,
        copy_size_usd=1.0,
        shares=2.5,
        observed_ts=build_ts - age_s,
        strategy_family="wallet_copy_window_inventory_v3_drip",
        policy_id="parent_policy",
        sizing_policy_id="window_inventory_fraction",
        mode="live",
        action="BUY",
        token_id=f"token_{slug_suffix}",
        event_ts=build_ts - age_s,
        live_orders_allowed=True,
        metadata={"copy_model": "drip"},
    )
    return intent.asdict()


def _order(*, age_s: float, build_ts: float, slug: str, pnl: float) -> dict:
    window_start = int(slug.rsplit("-", 1)[-1])
    return {
        "order_id": f"order_{int(age_s)}",
        "intent_id": f"ci_{int(age_s)}",
        "source_wallet": "0xparent",
        "copy_model": "drip",
        "final_status": "FILLED",
        "market_slug": slug,
        "outcome": "Up",
        "requested_size_usd": 1.0,
        "limit_price": 0.4,
        "latency_budget": {"intent_built_ts": build_ts},
        "source_intent": _intent(age_s=age_s, build_ts=build_ts, window_start=window_start, slug_suffix=str(int(age_s))),
        "_expected_pnl": pnl,
    }


def test_winner_variation_siblings_hydrates_twin_and_freshness_lanes(tmp_path):
    history_state = tmp_path / "history.json"
    history_index = tmp_path / "history_index.json"
    rtds_watermark = tmp_path / "watermarks.json"
    live_ledger = tmp_path / "live_ledger.json"
    for path, payload in (
        (history_state, {"schema_version": 1, "kind": "wallet_copy_history_state", "events": []}),
        (history_index, {"schema_version": 1, "kind": "wallet_copy_history_window_index", "windows": {}}),
        (rtds_watermark, {}),
    ):
        atomic_write_json(path, payload)

    build_ts = 1100.0
    slugs = ["btc-updown-5m-1000", "btc-updown-5m-1001", "btc-updown-5m-1002"]
    orders = [
        _order(age_s=20.0, build_ts=build_ts, slug=slugs[0], pnl=0.10),
        _order(age_s=60.0, build_ts=build_ts, slug=slugs[1], pnl=0.20),
        _order(age_s=85.0, build_ts=build_ts, slug=slugs[2], pnl=0.30),
    ]
    atomic_write_json(
        live_ledger,
        {
            "orders": orders,
            "resolution_writeback": {
                "per_market_slug": {
                    slugs[0]: {"yes_pnl_usd": 0.10, "no_pnl_usd": 0.0},
                    slugs[1]: {"yes_pnl_usd": 0.20, "no_pnl_usd": 0.0},
                    slugs[2]: {"yes_pnl_usd": 0.30, "no_pnl_usd": 0.0},
                }
            },
        },
    )
    guard = {
        "active_set_runtime": {
            "set_generation_id": "active_set_gen_test",
            "selected_member": {
                "candidate_id": "parent_candidate",
                "source_wallet": "0xparent",
                "policy_id": "parent_policy",
                "max_order_usd": 1.0,
                "max_price": 0.5,
            },
            "policy_by_wallet": {
                "0xparent": {
                    "policy_id": "parent_policy",
                    "min_price": 0.0,
                    "max_price": 0.5,
                    "min_seconds_from_open": 0,
                    "max_seconds_from_open": 300,
                    "min_wallet_usdc": 0.0,
                    "max_wallet_usdc": 0.0,
                    "wallet_fraction": 0.1,
                    "max_order_usd": 1.0,
                    "min_order_usd": 1.0,
                }
            },
        },
        "guard_code_identity": {
            "started_at_utc": "1970-01-01T00:00:00Z",
            "git_head_at_launch": "abc123",
            "script_sha256": "sha",
        },
        "live_execution_runtime": {
            "argv": [
                "python",
                "scripts/run_wallet_copy_live_execution.py",
                "--max-event-age-s",
                "30",
                "--live-build-max-observed-age-s",
                "30",
                "--max-intents",
                "6",
                "--copy-model",
                "drip",
                "--min-live-order-usd",
                "1",
            ]
        },
    }
    cli_args = SimpleNamespace(
        history_state=str(history_state),
        history_window_index=str(history_index),
        rtds_watermark_state=str(rtds_watermark),
        live_ledger_state=str(live_ledger),
        freshness_siblings="45,75,90",
    )

    payload = build_state(
        cli_args=cli_args,
        guard=guard,
        live_ledger=json.loads(live_ledger.read_text()),
        latest_change={"change_id": "change-test", "golden_snapshot": "golden.json"},
    )

    lanes = {lane["lane_id"]: lane for lane in payload["lanes"]}
    assert payload["paper_only"] is True
    assert payload["live_orders_allowed"] is False
    assert payload["parent_epoch"]["latest_change_id"] == "change-test"
    assert payload["copyintent_parity"]["status"] == "PASS"
    assert lanes["paper_twin_exact_parent"]["evidence"]["resolved_paper_fills"] == 1
    assert lanes["freshness_45s_sibling"]["evidence"]["resolved_paper_fills"] == 1
    assert lanes["freshness_75s_sibling"]["evidence"]["resolved_paper_fills"] == 2
    assert lanes["freshness_90s_sibling"]["evidence"]["resolved_paper_fills"] == 3
    assert lanes["freshness_75s_sibling"]["promotion_gate"]["roi_diff_pp_vs_twin"] == 5.0
    assert lanes["freshness_90s_sibling"]["promotion_gate"]["status"] == "PAPER_ACCUMULATING"
