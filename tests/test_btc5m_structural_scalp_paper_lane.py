import json
import time
from argparse import Namespace
from pathlib import Path

import pytest

from scripts.run_btc5m_structural_scalp_paper_lane import (
    _evidence_floors,
    _merge_forward_source_cache,
    build_state,
    structural_copy_intent_from_row,
)
from src.wallet_copy.models import CopyIntent


def test_structural_scalp_paper_lane_replays_fifo_gate(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Up",
                "price": 0.40,
                "size": 10.0,
                "event_ts": 1000000010,
            },
            {
                "action": "SELL",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Up",
                "price": 0.55,
                "size": 10.0,
                "event_ts": 1000000020,
            },
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Down",
                "price": 0.30,
                "size": 10.0,
                "event_ts": 1000000310,
            },
            {
                "action": "SELL",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-b",
                "outcome": "Down",
                "price": 0.45,
                "size": 10.0,
                "event_ts": 1000000320,
            },
        ]
    }
    (research / "wallet_copy_history_state.json").write_text(json.dumps(history), encoding="utf-8")
    (research / "btc_resolutions_test.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"market_slug": "btc-updown-5m-1000000000", "condition_id": "cond-a", "direction": "UP"}),
                json.dumps({"market_slug": "btc-updown-5m-1000000300", "condition_id": "cond-b", "direction": "DOWN"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (research / "btc5m_two_sided_prime_study_latest.json").write_text(
        json.dumps({"intra_window_scalp": {"ev_per_day_usd": 12.0, "oos_trades": 112}}),
        encoding="utf-8",
    )

    state, rows = build_state(
        root,
        Namespace(
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            study="data/research/btc5m_two_sided_prime_study_latest.json",
            state="data/research/btc5m_structural_scalp_paper_lane_state.json",
            events="data/research/btc5m_structural_scalp_paper_lane_events.jsonl",
            order_usd=1.0,
            tick_size=0.01,
            gate_min_fills=2,
            gate_window_hours=24.0,
            seeded_at="2001-09-09T01:40:00Z",
        ),
    )

    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert state["summary"]["paper_fills"] == 2
    assert state["metrics"]["historical_gate_24h"]["pnl_usd"] > 0
    assert state["metrics"]["forward"]["fills"] == 2
    assert state["inputs"]["freshness_pass"] is False
    assert state["live_gate"]["evidence_passed"] is False
    assert state["live_gate"]["ready_for_live"] is False
    assert state["live_gate"]["status"] == "FORWARD_GATE_PENDING"
    assert len(state["current_intents"]) == 4
    assert rows[0]["paper_fill_id"].startswith("pscalp_fill_")


def test_structural_scalp_jsonl_source_reports_freshness(tmp_path: Path) -> None:
    research = tmp_path / "data" / "research"
    research.mkdir(parents=True)
    now = time.time()
    slug = f"btc-updown-5m-{int(now // 300) * 300}"
    source = research / "wallet_copy_live_guard_wallet_events.jsonl"
    source.write_text(
        json.dumps(
            {
                "action": "BUY",
                "source_wallet": "0x2222222222222222222222222222222222222222",
                "market_slug": slug,
                "condition_id": "cond-fresh",
                "outcome": "Up",
                "price": 0.4,
                "size": 2.0,
                "event_ts": now,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (research / "btc_resolutions_test.jsonl").write_text("", encoding="utf-8")
    (research / "btc5m_two_sided_prime_study_latest.json").write_text("{}", encoding="utf-8")

    state, _rows = build_state(
        tmp_path,
        Namespace(
            history="data/research/wallet_copy_live_guard_wallet_events.jsonl",
            resolutions="data/research/btc_resolutions_test.jsonl",
            study="data/research/btc5m_two_sided_prime_study_latest.json",
            state="data/research/btc5m_structural_scalp_paper_lane_state.json",
            events="data/research/btc5m_structural_scalp_paper_lane_events.jsonl",
            order_usd=1.0,
            tick_size=0.01,
            gate_min_fills=30,
            gate_window_hours=24.0,
            seeded_at="",
        ),
    )

    assert state["inputs"]["history"].endswith("wallet_copy_live_guard_wallet_events.jsonl")
    assert state["inputs"]["freshness_pass"] is True
    assert state["inputs"]["newest_source_event_age_s"] < 5.0
    assert state["live_gate"]["evidence_passed"] is False


def test_structural_scalp_accumulator_survives_hot_buffer_rotation(tmp_path: Path) -> None:
    cache = tmp_path / "forward.jsonl"
    seeded_at = 1_800_000_000.0
    first = [
        {"event_id": "a", "event_ts": seeded_at + 10},
        {"event_id": "b", "event_ts": seeded_at + 20},
    ]
    rotated = [
        {"event_id": "b", "event_ts": seeded_at + 20},
        {"event_id": "c", "event_ts": seeded_at + 30},
    ]

    _merge_forward_source_cache(cache, first, seeded_at_ts=seeded_at)
    accumulated = _merge_forward_source_cache(cache, rotated, seeded_at_ts=seeded_at)

    assert [row["event_id"] for row in accumulated] == ["a", "b", "c"]
    assert len(cache.read_text(encoding="utf-8").splitlines()) == 3


def test_forward_clock_floor_is_fixed_while_rolling_diagnostic_floor_moves() -> None:
    forward_floor, rolling_floor = _evidence_floors(
        latest_window_start_s=1_000_259_200,
        seeded_at_ts=1_000_000_010.0,
        gate_window_hours=24.0,
    )

    assert forward_floor == 1_000_000_200
    assert rolling_floor == 1_000_173_100
    assert forward_floor < rolling_floor


def test_structural_scalp_accumulator_reads_shards_and_keeps_active_small(tmp_path: Path) -> None:
    cache = tmp_path / "forward.jsonl"
    shard = tmp_path / "forward_shard_20260721T000000Z_0001.jsonl"
    seeded_at = 1_800_000_000.0
    shard.write_text(
        json.dumps({"event_id": "a", "event_ts": seeded_at + 10}) + "\n"
        + json.dumps({"event_id": "b", "event_ts": seeded_at + 20}) + "\n",
        encoding="utf-8",
    )
    cache.write_text(json.dumps({"event_id": "c", "event_ts": seeded_at + 30}) + "\n", encoding="utf-8")

    accumulated = _merge_forward_source_cache(
        cache,
        [
            {"event_id": "b", "event_ts": seeded_at + 20},
            {"event_id": "d", "event_ts": seeded_at + 40},
        ],
        seeded_at_ts=seeded_at,
    )

    assert [row["event_id"] for row in accumulated] == ["a", "b", "c", "d"]
    active_rows = [json.loads(line) for line in cache.read_text(encoding="utf-8").splitlines()]
    assert [row["event_id"] for row in active_rows] == ["c", "d"]


def test_structural_scalp_accumulator_replace_failure_preserves_prior_file(
    tmp_path: Path, monkeypatch
) -> None:
    cache = tmp_path / "forward.jsonl"
    cache.write_text(json.dumps({"event_id": "a", "event_ts": 1_800_000_010.0}) + "\n", encoding="utf-8")
    before = cache.read_bytes()

    def fail_replace(_source, _target):
        raise OSError("simulated kill before rename")

    monkeypatch.setattr("scripts.run_btc5m_structural_scalp_paper_lane.os.replace", fail_replace)

    with pytest.raises(OSError, match="simulated kill"):
        _merge_forward_source_cache(
            cache,
            [{"event_id": "b", "event_ts": 1_800_000_020.0}],
            seeded_at_ts=1_800_000_000.0,
        )

    assert cache.read_bytes() == before
    assert list(tmp_path.glob(".*.tmp")) == []


def test_structural_scalp_paper_lane_rescinds_circular_replay_gate(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Up",
                "price": 0.40,
                "size": 10.0,
                "event_ts": 1000000010,
            },
            {
                "action": "SELL",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-a",
                "outcome": "Up",
                "price": 0.55,
                "size": 10.0,
                "event_ts": 1000000020,
            },
        ]
    }
    (research / "wallet_copy_history_state.json").write_text(json.dumps(history), encoding="utf-8")
    (research / "btc_resolutions_test.jsonl").write_text(
        json.dumps({"market_slug": "btc-updown-5m-1000000000", "condition_id": "cond-a", "direction": "UP"}) + "\n",
        encoding="utf-8",
    )
    (research / "btc5m_two_sided_prime_study_latest.json").write_text("{}", encoding="utf-8")

    state, _rows = build_state(
        root,
        Namespace(
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            study="data/research/btc5m_two_sided_prime_study_latest.json",
            state="data/research/btc5m_structural_scalp_paper_lane_state.json",
            events="data/research/btc5m_structural_scalp_paper_lane_events.jsonl",
            order_usd=1.0,
            tick_size=0.01,
            gate_min_fills=1,
            gate_window_hours=24.0,
            seeded_at="2026-07-07T18:39:26Z",
        ),
    )

    assert state["metrics"]["historical_gate_24h"]["fills"] == 1
    assert state["metrics"]["forward"]["fills"] == 0
    assert state["live_gate"]["status"] == "FORWARD_GATE_PENDING"
    assert state["live_gate"]["ready_for_live"] is False
    assert state["current_intents"] == []
    assert state["summary"]["current_intents"] == 0


def test_structural_current_intents_keep_only_post_forward_floor_rows(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    wallet = "0x2222222222222222222222222222222222222222"
    history = {
        "events": [
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-pre",
                "outcome": "Up",
                "price": 0.40,
                "size": 10.0,
                "event_ts": 1000000010,
            },
            {
                "action": "SELL",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000000",
                "condition_id": "cond-pre",
                "outcome": "Up",
                "price": 0.55,
                "size": 10.0,
                "event_ts": 1000000020,
            },
            {
                "action": "BUY",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-post",
                "outcome": "Down",
                "price": 0.30,
                "size": 10.0,
                "event_ts": 1000000310,
            },
            {
                "action": "SELL",
                "source_wallet": wallet,
                "market_slug": "btc-updown-5m-1000000300",
                "condition_id": "cond-post",
                "outcome": "Down",
                "price": 0.45,
                "size": 10.0,
                "event_ts": 1000000320,
            },
        ]
    }
    (research / "wallet_copy_history_state.json").write_text(json.dumps(history), encoding="utf-8")
    (research / "btc_resolutions_test.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"market_slug": "btc-updown-5m-1000000000", "condition_id": "cond-pre", "direction": "UP"}),
                json.dumps(
                    {"market_slug": "btc-updown-5m-1000000300", "condition_id": "cond-post", "direction": "DOWN"}
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (research / "btc5m_two_sided_prime_study_latest.json").write_text("{}", encoding="utf-8")

    state, _rows = build_state(
        root,
        Namespace(
            history="data/research/wallet_copy_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            study="data/research/btc5m_two_sided_prime_study_latest.json",
            state="data/research/btc5m_structural_scalp_paper_lane_state.json",
            events="data/research/btc5m_structural_scalp_paper_lane_events.jsonl",
            order_usd=1.0,
            tick_size=0.01,
            gate_min_fills=1,
            gate_window_hours=24.0,
            seeded_at="2001-09-09T01:46:50Z",
        ),
    )

    assert state["inputs"]["forward_floor_window_start_s"] == 1000000200
    assert [intent["condition_id"] for intent in state["current_intents"]] == ["cond-post", "cond-post"]
    assert {intent["action"] for intent in state["current_intents"]} == {"BUY", "SELL"}


def test_structural_copy_intent_round_trip_and_stable_id() -> None:
    row = {
        "action": "OPEN",
        "condition_id": "cond-live",
        "entry_price": 0.41,
        "event_ts": 1_800_000_010.0,
        "market_slug": "btc-updown-5m-1800000000",
        "outcome": "Up",
        "paper_order_id": "pscalp_open_unit",
        "window_start_s": 1_800_000_000,
    }

    intent = structural_copy_intent_from_row(
        row,
        mechanism_id="structural-intra-window-scalp",
        order_usd=1.0,
        policy_id="structural_scalp_tick_0.01_usd_1",
    )
    payload = intent.asdict()
    round_trip = CopyIntent.from_dict(payload)
    payload_without_id = dict(payload)
    payload_without_id.pop("intent_id")
    rederived = CopyIntent.from_dict(payload_without_id)

    assert round_trip.asdict() == payload
    assert rederived.intent_id == intent.intent_id
    assert intent.source_wallet == "structural::structural-intra-window-scalp"
    assert intent.strategy_family == "structural_scalp_v1"
    assert intent.sizing_policy_id == "fixed_usd_1"
    assert intent.order_type == "STRUCTURAL_ENTRY"
    assert intent.live_orders_allowed is False
    assert intent.mode == "paper"


def test_structural_copy_intent_traverses_guard_shadow_checks_without_submit_branch() -> None:
    import scripts.run_wallet_copy_live_guard as guard

    now = time.time()
    window_start = int(now // 300) * 300
    row = {
        "action": "OPEN",
        "condition_id": f"cond-{window_start}",
        "entry_price": 0.41,
        "event_ts": now,
        "market_slug": f"btc-updown-5m-{window_start}",
        "outcome": "Up",
        "paper_order_id": "pscalp_open_live",
        "window_start_s": window_start,
    }
    intent = structural_copy_intent_from_row(
        row,
        mechanism_id="structural-intra-window-scalp",
        order_usd=1.0,
        policy_id="structural_scalp_tick_0.01_usd_1",
    )

    malicious_payload = {
        **intent.asdict(),
        "mode": "live",
        "live_orders_allowed": True,
    }

    shadow_intent = guard._shadow_intent_from_row(
        malicious_payload,
        lane="btc5m_structural_scalp_v1",
        source_tag="structural::intra-window-scalp",
    )
    diagnostics = guard._intent_runtime_diagnostics(
        shadow_intent,
        now_ts=now,
        max_event_age_s=30.0,
        live_build_max_observed_age_s=30.0,
    )

    assert shadow_intent.live_orders_allowed is False
    assert shadow_intent.mode == "paper"
    assert shadow_intent.source_wallet == "structural::intra-window-scalp"
    assert diagnostics["live_tradeable_window_open"] is True
    assert diagnostics["fresh_for_live_build"] is True


def test_empty_structural_current_intents_do_not_add_guard_shadow_spec(tmp_path: Path) -> None:
    import scripts.run_wallet_copy_live_guard as guard

    structural_state = tmp_path / "structural_state.json"
    e5_state = tmp_path / "e5_state.json"
    e6_lane_state = tmp_path / "e6_lane_state.json"
    e6_paper_state = tmp_path / "e6_paper_state.json"
    structural_state.write_text(json.dumps({"current_intents": []}), encoding="utf-8")
    e5_state.write_text("{}", encoding="utf-8")
    e6_lane_state.write_text("{}", encoding="utf-8")
    e6_paper_state.write_text("{}", encoding="utf-8")

    specs = guard._shadow_lane_specs(
        Namespace(
            e5_shadow_lane_state=str(e5_state),
            e6_shadow_lane_state=str(e6_lane_state),
            e6_shadow_paper_state=str(e6_paper_state),
            structural_scalp_lane_state=str(structural_state),
        )
    )

    assert all(spec["lane"] != "btc5m_structural_scalp_v1" for spec in specs)
