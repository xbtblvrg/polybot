from __future__ import annotations

from scripts.report_toxicity_deny_cell_counterfactual import build_report


def test_toxicity_deny_cell_counterfactual_flags_signals_deny_with_positive_live_measures() -> None:
    wallet = "0xabc"
    report = {
        "generated_at": "2026-07-08T00:00:00Z",
        "groups": [
            {
                "source_wallet": wallet,
                "price_bucket": "01_25_50",
                "live_fills": {"count": 5, "roi_pct": 2.5, "pnl_usd": 1.2, "stake_usd": 48.0},
                "all_signals": {"count": 100, "roi_pct": -1.0, "pnl_usd": -10.0, "stake_usd": 1000.0},
                "toxicity_roi_pct": 3.5,
            }
        ],
    }
    denylist = {
        "generated_at": "2026-07-08T00:00:00Z",
        "cells": [
            {
                "source_wallet": wallet,
                "price_bucket": "01_25_50",
                "deny_rule": "signals_100_roi_le_0",
                "reason": "toxicity_protection",
            }
        ],
    }
    guard = {
        "active_set": {"members": [{"source_wallet": wallet}]},
        "window_participation": {
            "rows": [
                {
                    "source_wallet": wallet,
                    "dominant_skip_reason": "toxicity_protection",
                    "source_inventory_vwap": 0.33,
                    "guard_sized_copy_usd": 4.0,
                    "intent_id": "ci_1",
                    "market_slug": "btc-updown-5m-1",
                },
                {
                    "source_wallet": wallet,
                    "dominant_skip_reason": "toxicity_protection",
                    "source_inventory_vwap": 0.40,
                    "guard_sized_copy_usd": 2.5,
                    "intent_id": "ci_2",
                    "market_slug": "btc-updown-5m-1",
                },
            ]
        },
    }

    payload = build_report(toxicity_report=report, denylist=denylist, guard_state=guard)

    assert payload["denied_active_cells"] == 1
    assert payload["positive_live_measure_flags"] == 1
    row = payload["rows"][0]
    assert row["blocked_usd_this_cycle"] == 6.5
    assert row["blocked_intents_this_cycle"] == 2
    assert row["flag"] == "SIGNALS_DENY_BUT_LIVE_POSITIVE"
    assert "blocked_usd_this_cycle=6.500000" in payload["line_table"][0]
