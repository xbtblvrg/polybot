import json
from pathlib import Path

from scripts.run_wallet_copy_live_execution import _apply_toxicity_protection_gate
from src.wallet_copy.models import CopyIntent

ROOT = Path(__file__).resolve().parents[1]
D97_WALLET = "0xd97ae021645712fe5cf73139049383a100cac068"


def _intent(wallet: str, price: float, *, outcome: str = "UP") -> CopyIntent:
    return CopyIntent(
        source_wallet=wallet,
        wallet_name="test",
        source_event_id=f"event-{wallet}-{price}-{outcome}",
        condition_id="cond-1",
        market_slug="btc-updown-5m-1",
        outcome=outcome,
        side="BUY",
        limit_price=price,
        wallet_usdc_size=10.0,
        copy_size_usd=1.0,
        shares=1.0 / price,
        observed_ts=100.0,
    )


def test_toxicity_protection_filters_only_configured_wallet_bucket(tmp_path: Path) -> None:
    config = tmp_path / "toxicity_denylist.json"
    config.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "source_wallet": "0xabc",
                        "price_bucket": "00_00_25",
                        "direction": "UP",
                        "reason": "toxicity_protection",
                    }
                ],
                "criteria": {"our_fills_min_count": 20},
            }
        )
    )

    kept, summary = _apply_toxicity_protection_gate(
        [_intent("0xabc", 0.20), _intent("0xabc", 0.20, outcome="DOWN"), _intent("0xabc", 0.30), _intent("0xdef", 0.20)],
        config_path=str(config),
    )

    assert [(intent.source_wallet, intent.limit_price, intent.outcome) for intent in kept] == [
        ("0xabc", 0.20, "DOWN"),
        ("0xabc", 0.30, "UP"),
        ("0xdef", 0.20, "UP"),
    ]
    assert summary["blocked_intents"] == 1
    assert summary["rule"] == "guard_side_reject_source_wallet_x_price_bucket_x_direction_cells_with_negative_our_fill_roi"
    assert summary["sample_filtered_intents"][0]["reject_reason"] == "toxicity_protection"
    assert summary["sample_filtered_intents"][0]["direction"] == "UP"


def test_d97_wildcard_denylist_rejects_all_buy_price_buckets() -> None:
    prices = [0.20, 0.30, 0.60, 0.80]
    kept, summary = _apply_toxicity_protection_gate(
        [_intent(D97_WALLET, price, outcome="DOWN") for price in prices],
        config_path=str(ROOT / "configs/wallet_copy/toxicity_denylist.json"),
    )

    assert kept == []
    assert summary["blocked_intents"] == 4
    assert [row["price_bucket"] for row in summary["sample_filtered_intents"]] == [
        "00_00_25",
        "01_25_50",
        "02_50_70",
        "03_70_100",
    ]
    assert {row["source_wallet"] for row in summary["sample_filtered_intents"]} == {D97_WALLET}


def test_d97_wildcard_buy_denylist_rejects_all_price_buckets(tmp_path: Path) -> None:
    wallet = "0xd97ae021645712fe5cf73139049383a100cac068"
    config = tmp_path / "toxicity_denylist.json"
    config.write_text(
        json.dumps(
            {
                "cells": [
                    {"source_wallet": wallet, "price_bucket": bucket, "direction": ""}
                    for bucket in ("00_00_25", "01_25_50", "02_50_70", "03_70_100")
                ]
            }
        )
    )

    kept, summary = _apply_toxicity_protection_gate(
        [
            _intent(wallet, 0.20),
            _intent(wallet, 0.30),
            _intent(wallet, 0.60),
            _intent(wallet, 0.80),
        ],
        config_path=str(config),
    )

    assert kept == []
    assert summary["blocked_intents"] == 4
    assert summary["taxonomy_counts"] == {"toxicity_protection": 4}
    assert {row["price_bucket"] for row in summary["sample_filtered_intents"]} == {
        "00_00_25",
        "01_25_50",
        "02_50_70",
        "03_70_100",
    }
