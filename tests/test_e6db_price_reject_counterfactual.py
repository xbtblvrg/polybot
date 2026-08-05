import json
from pathlib import Path

import pytest

from scripts import report_e6db_price_reject_counterfactual as report


def _event(idx: int, *, price: float = 0.6, outcome: str = "Up", source_wallet: str = report.E6DB) -> dict:
    start = 1783950000 + idx * 300
    return {
        "source_wallet": source_wallet,
        "action": "BUY",
        "asset": "BTC",
        "market_slug": f"btc-updown-5m-{start}",
        "condition_id": f"0x{idx:064x}",
        "token_id": f"token-{idx}",
        "outcome": outcome,
        "price": price,
        "event_ts": start + 5,
        "observed_ts": start + 6,
        "transaction_hash": f"0x{idx:064x}",
        "source_fingerprint": f"fp-{idx}",
    }


def _resolution(idx: int, *, direction: str = "UP") -> dict:
    start = 1783950000 + idx * 300
    return {
        "condition_id": f"0x{idx:064x}",
        "yes_token": f"token-{idx}",
        "direction": direction,
        "expiry_unix_ts": start + 300,
        "window_type": "5m",
        "source": "test",
    }


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_counterfactual_filters_price_rejected_e6db_rows(tmp_path):
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "cf_events.jsonl"
    overlay = tmp_path / "overlay.json"
    _write_jsonl(
        events,
        [
            _event(1, price=0.6),
            _event(2, price=0.49),
            {**_event(3, price=0.61), "source_wallet": "0x1111111111111111111111111111111111111111"},
        ],
    )
    _write_jsonl(resolutions, [_resolution(1)])
    overlay.write_text('{"members":[]}', encoding="utf-8")

    payload = report.build_report(
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    assert payload["summary"]["candidate_events"] == 1
    assert payload["summary"]["resolved_n"] == 1
    assert payload["summary"]["hypothetical_pnl_usd"] > 0
    assert payload["status"] == "MEASURING"
    assert len(event_log.read_text(encoding="utf-8").splitlines()) == 1


def test_negative_refresh_supersedes_overlay_headline_without_roster_mutation(tmp_path):
    wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    overlay = tmp_path / "overlay.json"
    member = {"source_wallet": wallet, "max_order_usd": 1.0, "policy_id": "live-policy"}
    overlay.write_text(
        json.dumps(
            {
                "members": [member],
                "latest_a6896d11_price_reject_canary": {
                    "status": "ARMED",
                    "summary": {"hypothetical_pnl_usd": 19.624008, "resolved_n": 455},
                },
            }
        ),
        encoding="utf-8",
    )
    payload = {
        "status": "BAND_STANDS_VINDICATED",
        "state_path": "data/research/a6896d11_price_reject_counterfactual_state.json",
        "summary": {
            "hypothetical_pnl_usd": -5.754679,
            "resolved_n": 679,
            "positive_gate": False,
            "recent_200_positive_gate": False,
            "widening_rearm_gate": False,
        },
        "filters": {"canary_max_price_inclusive": 0.7, "canary_size_usd": 1.0},
    }

    report._refresh_canary_headline_only(
        overlay, report=payload, source_wallet=wallet
    )

    stored = json.loads(overlay.read_text(encoding="utf-8"))
    assert stored["members"] == [member]
    headline = stored["latest_a6896d11_price_reject_canary"]
    assert headline["status"] == "BAND_STANDS_VINDICATED"
    assert headline["summary"]["hypothetical_pnl_usd"] == -5.754679
    assert headline["widening_path"] == "DISARMED"
    assert "recent 200" in headline["rearm_rule"]


def test_counterfactual_positive_gate_arms_e6db_canary_overlay(tmp_path):
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "cf_events.jsonl"
    overlay = tmp_path / "overlay.json"
    _write_jsonl(events, [_event(idx, price=0.6) for idx in range(12)])
    _write_jsonl(resolutions, [_resolution(idx, direction="UP") for idx in range(12)])
    overlay.write_text(
        json.dumps(
            {
                "members": [
                    {
                        "candidate_id": "runtime_auto_degrade_e6db20932f",
                        "source_wallet": report.E6DB,
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "max_order_usd": 4.0,
                        "max_price": 0.5,
                        "policy": {"max_order_usd": 4.0, "max_price": 0.5},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = report.build_report(
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=True,
    )

    stored = json.loads(overlay.read_text(encoding="utf-8"))
    member = stored["members"][0]
    assert payload["status"] == "CANARY_ARMED"
    assert member["max_price"] == 0.7
    assert member["max_order_usd"] == 2.0
    assert member["policy_id"] == "e6db_price_reject_canary_0.10_cap_2_le_70"


def test_counterfactual_positive_gate_arms_non_default_source_wallet_only(tmp_path):
    source_wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "cf_events.jsonl"
    overlay = tmp_path / "overlay.json"
    _write_jsonl(events, [_event(idx, price=0.6, source_wallet=source_wallet) for idx in range(12)])
    _write_jsonl(resolutions, [_resolution(idx, direction="UP") for idx in range(12)])
    overlay.write_text(
        json.dumps(
            {
                "members": [
                    {
                        "candidate_id": "runtime_auto_degrade_e6db20932f",
                        "source_wallet": report.E6DB,
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "max_order_usd": 4.0,
                        "max_price": 0.5,
                        "policy": {"max_order_usd": 4.0, "max_price": 0.5},
                    },
                    {
                        "candidate_id": "runtime_auto_degrade_a6896d11f7",
                        "source_wallet": source_wallet,
                        "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                        "max_order_usd": 1.0,
                        "max_price": 0.5,
                        "policy": {"max_order_usd": 1.0, "max_price": 0.5},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = report.build_report(
        source_wallet=source_wallet,
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=True,
    )

    stored = json.loads(overlay.read_text(encoding="utf-8"))
    e6db_member, a689_member = stored["members"]
    assert payload["status"] == "CANARY_ARMED"
    assert payload["canary_application"]["source_wallet"] == source_wallet
    assert e6db_member["max_price"] == 0.5
    assert e6db_member["max_order_usd"] == 4.0
    assert e6db_member["policy_id"] == "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
    assert a689_member["max_price"] == 0.7
    assert a689_member["max_order_usd"] == 2.0
    assert a689_member["fable_cap_max_order_usd"] == 2.0
    assert a689_member["drip_min_tranche_usd"] == 2.0
    assert a689_member["drip_max_tranche_usd"] == 2.0
    assert a689_member["policy_id"] == "a6896d11_price_reject_canary_0.10_cap_2_le_70"
    assert a689_member["policy"]["fable_cap_max_order_usd"] == 2.0
    assert a689_member["policy"]["drip_min_tranche_usd"] == 2.0
    assert a689_member["policy"]["drip_max_tranche_usd"] == 2.0
    assert a689_member["policy"]["condition_e_effective_min_submit_cap_usd"] == 2.0
    assert a689_member["summary"]["a6896d11_price_reject_canary"]["source_wallet"] == source_wallet
    assert a689_member["summary"]["a6896d11_price_reject_canary"]["condition_e_effective_min_submit_cap_usd"] == 2.0
    assert stored["latest_a6896d11_price_reject_canary"]["source_wallet"] == source_wallet


def test_counterfactual_backfills_e6db_rows_from_rtds_capture(tmp_path):
    events = tmp_path / "events.jsonl"
    rtds = tmp_path / "rtds.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "cf_events.jsonl"
    overlay = tmp_path / "overlay.json"
    idx = 1
    source = _event(idx, price=0.61, outcome="Up")
    events.write_text("", encoding="utf-8")
    _write_jsonl(
        rtds,
        [
            {
                "event": "rtds_trade_event",
                "source_wallet": report.E6DB,
                "side": "BUY",
                "market_slug": source["market_slug"],
                "condition_id": source["condition_id"],
                "asset": source["token_id"],
                "price": source["price"],
                "size": 5.0,
                "event_ts": source["event_ts"],
                "received_at_s": source["observed_ts"],
                "transaction_hash": source["transaction_hash"],
                "raw": {
                    "proxyWallet": report.E6DB,
                    "side": "BUY",
                    "slug": source["market_slug"],
                    "conditionId": source["condition_id"],
                    "asset": source["token_id"],
                    "outcome": source["outcome"],
                    "price": source["price"],
                    "size": 5.0,
                    "timestamp": source["event_ts"],
                    "transactionHash": source["transaction_hash"],
                },
            }
        ],
    )
    _write_jsonl(resolutions, [_resolution(idx)])
    overlay.write_text('{"members":[]}', encoding="utf-8")

    payload = report.build_report(
        wallet_events=events,
        rtds_events=rtds,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    assert payload["tap_source_mode"] == "wallet_events_plus_rtds"
    assert payload["tap_provably_carries_events"] is True
    assert payload["summary"]["candidate_events"] == 1
    assert payload["events"][0]["tap_source"] == "rtds_activity"


def test_non_default_source_wallet_uses_wallet_scoped_default_paths(tmp_path, monkeypatch):
    source_wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    default_state = tmp_path / "e6db_state.json"
    default_event_log = tmp_path / "e6db_events.jsonl"
    overlay = tmp_path / "overlay.json"
    wallet_state = tmp_path / "data/research/a6896d11_price_reject_counterfactual_state.json"
    wallet_event_log = tmp_path / "data/research/a6896d11_price_reject_counterfactual_events.jsonl"
    monkeypatch.setattr(report, "ROOT", tmp_path)
    monkeypatch.setattr(report, "DEFAULT_STATE", default_state)
    monkeypatch.setattr(report, "DEFAULT_EVENT_LOG", default_event_log)
    _write_jsonl(events, [_event(1, price=0.61, source_wallet=source_wallet)])
    _write_jsonl(resolutions, [_resolution(1)])
    overlay.write_text('{"members":[]}', encoding="utf-8")

    payload = report.build_report(
        source_wallet=source_wallet,
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=report.DEFAULT_STATE,
        event_log=report.DEFAULT_EVENT_LOG,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    assert payload["state_path"] == "data/research/a6896d11_price_reject_counterfactual_state.json"
    assert payload["event_log"] == "data/research/a6896d11_price_reject_counterfactual_events.jsonl"
    assert wallet_state.exists()
    assert wallet_event_log.exists()
    assert not default_state.exists()
    assert not default_event_log.exists()


def test_wallet_scoped_default_paths_reject_same_slug_wallet_mismatch(tmp_path, monkeypatch):
    source_wallet = "0xa6896d11f76dfa2820662c1f441496f51553559b"
    typo_same_slug_wallet = "0xa6896d11f74d6913676bca3d284b6c54957d559b"
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    default_state = tmp_path / "e6db_state.json"
    default_event_log = tmp_path / "e6db_events.jsonl"
    overlay = tmp_path / "overlay.json"
    wallet_state = tmp_path / "data/research/a6896d11_price_reject_counterfactual_state.json"
    monkeypatch.setattr(report, "ROOT", tmp_path)
    monkeypatch.setattr(report, "DEFAULT_STATE", default_state)
    monkeypatch.setattr(report, "DEFAULT_EVENT_LOG", default_event_log)
    _write_jsonl(events, [_event(1, price=0.61, source_wallet=source_wallet)])
    _write_jsonl(resolutions, [_resolution(1)])
    overlay.write_text('{"members":[]}', encoding="utf-8")

    first = report.build_report(
        source_wallet=source_wallet,
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=report.DEFAULT_STATE,
        event_log=report.DEFAULT_EVENT_LOG,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    with pytest.raises(ValueError, match="identity mismatch"):
        report.build_report(
            source_wallet=typo_same_slug_wallet,
            wallet_events=events,
            rtds_events=None,
            resolutions_path=resolutions,
            state_path=report.DEFAULT_STATE,
            event_log=report.DEFAULT_EVENT_LOG,
            overlay_path=overlay,
            since="2026-07-13T00:00:00Z",
            price_floor=0.5,
            canary_max_price=0.7,
            canary_size_usd=2.0,
            min_resolved=12,
            tail_bytes=0,
            apply_canary=False,
        )

    stored = json.loads(wallet_state.read_text(encoding="utf-8"))
    assert stored["source_wallet"] == source_wallet
    assert stored["summary"] == first["summary"]


def test_counterfactual_event_log_universe_keeps_resolved_monotonic_when_source_shrinks(tmp_path):
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "cf_events.jsonl"
    overlay = tmp_path / "overlay.json"
    overlay.write_text('{"members":[]}', encoding="utf-8")
    _write_jsonl(events, [_event(1, price=0.6), _event(2, price=0.61)])
    _write_jsonl(resolutions, [_resolution(1), _resolution(2)])

    first = report.build_report(
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    _write_jsonl(events, [_event(1, price=0.6)])
    second = report.build_report(
        wallet_events=events,
        rtds_events=None,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        overlay_path=overlay,
        since="2026-07-13T00:00:00Z",
        price_floor=0.5,
        canary_max_price=0.7,
        canary_size_usd=2.0,
        min_resolved=12,
        tail_bytes=0,
        apply_canary=False,
    )

    assert first["summary"]["resolved_n"] == 2
    assert second["source_snapshot_candidate_events"] == 1
    assert second["summary"]["candidate_events"] == 2
    assert second["summary"]["resolved_n"] == 2
    assert second["prev_resolved_n"] == 2
    assert second["resolved_n_delta"] == 0
