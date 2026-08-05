import json
from pathlib import Path

from scripts.report_d60c_latency_attribution import build_report


WALLET = "0x40138697bf1a0d655593f3be6237d60c1dc7ab35"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _ruling10(target_count: int = 4) -> dict:
    return {
        "generated_at": "2026-07-17T12:04:53Z",
        "members": [
            {
                "label": "d60c",
                "all_abstain_reason_counts": {"market_closed_now": target_count},
                "guard_stamp": {"probe_generated_at": "2026-07-17T12:04:06Z"},
                "participation_rows": [{"market_slug": "btc-updown-5m-1784289600"}],
            }
        ],
    }


def _event(
    event_id: str,
    *,
    market_slug: str = "btc-updown-5m-1784289300",
    observed_ts: float,
    generated_at: str,
    event_ts: float | None = None,
    wallet: str = WALLET,
    action: str = "BUY",
) -> dict:
    return {
        "event": "wallet_copy_wallet_event",
        "event_id": event_id,
        "source_wallet": wallet,
        "action": action,
        "market_slug": market_slug,
        "event_slug": market_slug,
        "event_ts": event_ts if event_ts is not None else observed_ts,
        "observed_ts": observed_ts,
        "generated_at": generated_at,
        "source": "polygon_orderfilled_ws_premerge",
        "raw": {"_walletCopySource": "polygon_orderfilled_ws_premerge"},
        "outcome": "Up",
        "price": 0.49,
        "usdc_size": 4.9,
        "transaction_hash": f"0x{event_id[-8:]:0>64}",
        "token_id": event_id,
    }


def test_d60c_latency_attribution_classifies_a_and_b_and_dedupes_snapshots(tmp_path: Path) -> None:
    _write_json(tmp_path / "data/research/ruling10.json", _ruling10())
    _append_jsonl(
        tmp_path / "data/research/wallet_events.jsonl",
        [
            _event("older_a", observed_ts=1784289310.0, generated_at="2026-07-17T11:55:10Z"),
            _event("latest_a", observed_ts=1784289352.0, generated_at="2026-07-17T11:56:04Z"),
            _event("latest_a", observed_ts=1784289352.0, generated_at="2026-07-17T11:59:24Z"),
            _event("source_b1", observed_ts=1784289601.0, generated_at="2026-07-17T12:00:02Z"),
            _event("source_b2", observed_ts=1784289700.0, generated_at="2026-07-17T12:01:40Z"),
            _event(
                "current_window_open",
                market_slug="btc-updown-5m-1784289600",
                observed_ts=1784289700.0,
                generated_at="2026-07-17T12:01:41Z",
            ),
            _event("future_snapshot", observed_ts=1784289353.0, generated_at="2026-07-17T12:05:00Z"),
        ],
    )

    report = build_report(
        root=tmp_path,
        ruling10_probe_path="data/research/ruling10.json",
        wallet_events_path="data/research/wallet_events.jsonl",
        tail_bytes=1_000_000,
    )

    assert report["summary"]["target_market_closed_count"] == 4
    assert report["summary"]["recovered_market_closed_count"] == 4
    assert report["summary"]["exact_target_count_match"] is True
    assert report["summary"]["class_counts"] == {"A": 2, "B": 2}
    assert report["scan"]["candidate_market_closed_rows"] == 4
    assert [row["event_id"] for row in report["events"]] == [
        "source_b2",
        "source_b1",
        "latest_a",
        "older_a",
    ]
    assert {row["event_id"]: row["latency_class"] for row in report["events"]} == {
        "older_a": "A",
        "latest_a": "A",
        "source_b1": "B",
        "source_b2": "B",
    }


def test_d60c_latency_attribution_limits_to_ruling10_target_count(tmp_path: Path) -> None:
    _write_json(tmp_path / "data/research/ruling10.json", _ruling10(target_count=2))
    _append_jsonl(
        tmp_path / "data/research/wallet_events.jsonl",
        [
            _event("old_a", observed_ts=1784289310.0, generated_at="2026-07-17T11:55:10Z"),
            _event("mid_a", observed_ts=1784289320.0, generated_at="2026-07-17T11:55:20Z"),
            _event("new_a", observed_ts=1784289330.0, generated_at="2026-07-17T11:55:30Z"),
        ],
    )

    report = build_report(
        root=tmp_path,
        ruling10_probe_path="data/research/ruling10.json",
        wallet_events_path="data/research/wallet_events.jsonl",
        tail_bytes=1_000_000,
    )

    assert report["summary"]["target_market_closed_count"] == 2
    assert [row["event_id"] for row in report["events"]] == ["new_a", "mid_a"]
    assert [row["event_id"] for row in report["unselected_candidate_events"]] == ["old_a"]
