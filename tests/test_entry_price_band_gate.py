import json

from scripts.run_wallet_copy_live_execution import (
    _append_entry_price_band_gate_events,
    _apply_entry_price_band_gate,
)
from src.wallet_copy.models import CopyIntent


def _intent(price: float, *, action: str = "BUY") -> CopyIntent:
    return CopyIntent.from_dict(
        {
            "intent_id": f"ci_{price}_{action}",
            "source_event_id": "we_source",
            "source_wallet": "0x" + "a" * 40,
            "wallet_name": "test",
            "market_id": "m1",
            "condition_id": "c1",
            "market_slug": "btc-updown-5m-1784566500",
            "outcome": "Up",
            "side": "YES",
            "action": action,
            "limit_price": price,
            "shares": 2.0,
            "copy_size_usd": 1.0,
            "wallet_usdc_size": 10.0,
            "policy_id": "p1",
            "sizing_policy_id": "s1",
            "strategy_family": "wallet_copy",
            "reason": "test",
            "mode": "paper",
            "paper_only": True,
            "live_orders_allowed": False,
            "observed_ts": 1784566501.0,
        }
    )


def _config(tmp_path):
    path = tmp_path / "gate.json"
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "direction_id": "2026-07-20T16:53Z-fable-entry-price-loss-band-gate",
                "experiment_id": "entry-price-loss-band-gate-20260720",
                "blocked_bands": [
                    {
                        "slice": "[0.2,0.4)",
                        "min_price_inclusive": 0.2,
                        "max_price_exclusive": 0.4,
                        "n_resolved_windows": 181,
                        "post_fee_pnl_usd": -35.41294,
                    }
                ],
            }
        )
    )
    return path


def test_entry_price_band_gate_blocks_only_buy_intents_inside_loss_band(tmp_path):
    intents = [_intent(0.199), _intent(0.2), _intent(0.399), _intent(0.4), _intent(0.3, action="SELL")]
    kept, summary = _apply_entry_price_band_gate(intents, config_path=str(_config(tmp_path)))

    assert [intent.limit_price for intent in kept] == [0.199, 0.4, 0.3]
    assert summary["taxonomy_counts"] == {"entry_price_band_gate": 2}
    assert summary["sample_filtered_intents"][0]["shadow_counterfactual_retained"] is True
    assert summary["sample_filtered_intents"][0]["entry_price_band"] == "[0.2,0.4)"


def test_entry_price_band_gate_writes_approved_counterfactual_rows(tmp_path):
    _, summary = _apply_entry_price_band_gate([_intent(0.3)], config_path=str(_config(tmp_path)))
    event_log = tmp_path / "events.jsonl"
    written = _append_entry_price_band_gate_events(
        str(event_log), summary, decision_ts="2026-07-20T16:55:00Z"
    )

    assert written == 1
    row = json.loads(event_log.read_text().strip())
    assert row["taxonomy"] == "entry_price_band_gate"
    assert row["approved_suppression"] is True
    assert row["counterfactual_shadow_row"]["would_submit_without_gate"] is True
    assert row["counterfactual_shadow_row"]["live_orders_allowed"] is False


def test_removed_entry_price_band_gate_keeps_intents_and_writes_inverted_shadow(tmp_path):
    config = _config(tmp_path)
    payload = json.loads(config.read_text())
    payload.update(
        {
            "enabled": False,
            "shadow_mode": "inverted_post_removal",
            "shadow_experiment_id": "entry-price-loss-band-gate-readd-shadow-20260722",
        }
    )
    config.write_text(json.dumps(payload))

    intents = [_intent(0.3), _intent(0.4)]
    kept, summary = _apply_entry_price_band_gate(intents, config_path=str(config))

    assert kept == intents
    assert summary["blocked_intents"] == 0
    assert summary["shadow_matched_intents"] == 1
    assert summary["taxonomy_counts"] == {}
    assert summary["sample_shadow_intents"][0]["taxonomy"] == "entry_price_band_gate_shadow"

    event_log = tmp_path / "events.jsonl"
    assert _append_entry_price_band_gate_events(
        str(event_log), summary, decision_ts="2026-07-22T06:32:00Z"
    ) == 1
    row = json.loads(event_log.read_text().strip())
    assert row["event_type"] == "COUNTERFACTUAL_SHADOW_NOT_APPLIED"
    assert row["approved_suppression"] is False
    assert row["experiment_id"] == "entry-price-loss-band-gate-readd-shadow-20260722"
    assert row["counterfactual_shadow_row"]["would_be_suppressed_if_gate_enabled"] is True
    assert row["counterfactual_shadow_row"]["live_path_gate_applied"] is False
