import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.report_order136d_early_entry_ranking import _lead_gate_verification, build_report


WALLET_A = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


def _row(wallet, event_ts, *, slug_start=1000, asset="BTC"):
    return {
        "source_wallet": wallet,
        "row_type": "trade",
        "action": "BUY",
        "duration": "5m",
        "asset": asset,
        "market_slug": f"btc-updown-5m-{slug_start}",
        "event_ts": event_ts,
        "observed_ts": event_ts + 9999,
    }


def _write(path: Path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_ranking_uses_source_time_tracks_discards_and_pool_coverage(tmp_path: Path):
    guard = tmp_path / "guard.jsonl"
    watch = tmp_path / "watch.jsonl"
    _write(
        guard,
        [
            _row(WALLET_A, 1010),
            _row(WALLET_A, 1059),
            _row(WALLET_A, 1060),
            _row(WALLET_A, 1300),
            _row(WALLET_B, 1010, asset="ETH"),
        ],
    )
    _write(watch, [_row(WALLET_B, 1020), _row(WALLET_B, 1021)])
    deadman = {
        "policy_choke_fire_drill": {
            "rung_c_full_pool_liveness_drill": {
                "candidate_evidence": {"rows": [{"wallet": WALLET_A}]}
            }
        }
    }

    report = build_report(
        streams=[("guard", guard), ("watch_tier", watch)],
        deadman=deadman,
        now=datetime.fromtimestamp(1400, tz=timezone.utc),
        min_n=2,
    )

    row_a = next(row for row in report["all_time"]["rows"] if row["wallet"] == WALLET_A)
    assert row_a["n"] == 3
    assert row_a["early_entry_rate"] == pytest.approx(2 / 3)
    assert row_a["discarded"] == 1
    assert row_a["in_134_pool"] is True
    assert report["pool_coverage"]["observed_intersection_pool"] == 1
    assert report["pool_coverage"]["coverage_ratio"] == 1.0


def test_wallet_filter_and_seven_day_cut_are_independent(tmp_path: Path):
    guard = tmp_path / "guard.jsonl"
    watch = tmp_path / "watch.jsonl"
    now = 2_000_000.0
    _write(guard, [_row(WALLET_A, 1010), _row(WALLET_A, now - 10, slug_start=int(now - 50))])
    _write(watch, [_row(WALLET_B, now - 10, slug_start=int(now - 50))])

    report = build_report(
        streams=[("guard", guard), ("watch_tier", watch)],
        deadman={},
        now=datetime.fromtimestamp(now, tz=timezone.utc),
        wallet_filter=WALLET_A,
        min_n=1,
        recency_days=1,
    )

    assert {row["wallet"] for row in report["all_time"]["rows"]} == {WALLET_A}
    assert report["all_time"]["rows"][0]["n"] == 2
    assert report["last_7d"]["rows"][0]["n"] == 1


def test_lead_gate_verification_is_regime_scoped_and_conservative():
    result = _lead_gate_verification(
        wallet=WALLET_A,
        now=datetime(2026, 8, 1, tzinfo=timezone.utc),
        temporal={
            "wallets": [
                {
                    "wallet": WALLET_A,
                    "classification": "WEEKDAY-ONLY",
                    "regime_profiles": {
                        "weekend": {"resolved_trades": 632, "pnl_usd": -31, "roi_pct": -3}
                    },
                    "venue_executable": {
                        "regime_profiles": {
                            "weekend": {"resolved_trades": 493, "pnl_usd": -29, "roi_pct": -4}
                        }
                    },
                }
            ]
        },
        probe={
            "generated_at": "2026-08-01T15:00:00Z",
            "rows": [
                {
                    "wallet": WALLET_A,
                    "status": "PASS",
                    "btc5m_buys_30m": 0,
                    "policy_compatible_inband_buy_rows_24h": 3,
                    "latest_trade_age_h": 1.4,
                }
            ],
        },
        live_guard={"active_set": {"members": []}},
        deadman={},
    )

    assert result["checks"] == {
        "f1_measured_positive_regime_cell": False,
        "f2_fresh_rows_and_own_policy_copyable": False,
        "f3_not_enabled_or_cooloff_or_fading": True,
        "f4_external_liveness": True,
    }
    assert result["eligible"] is False
