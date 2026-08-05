from scripts.launch_alpha_decay_eligible_profiles_paper_lane import build_launchd_payload
from scripts.run_alpha_decay_eligible_profile_paper_lane import (
    EXPERIMENT_ID,
    _read_new_rows,
    _price_in_band,
    build_lane_state_from_packet,
    filter_rows_to_profile_bands,
    pending_disposition,
    preserve_alpha_fields,
)


def test_price_band_boundaries_match_profile_labels() -> None:
    assert _price_in_band(0.25, "<=0.25")
    assert not _price_in_band(0.2501, "<=0.25")
    assert not _price_in_band(0.25, "0.25-0.50")
    assert _price_in_band(0.5, "0.25-0.50")
    assert not _price_in_band(0.5001, "0.25-0.50")
    assert _price_in_band(0.7501, ">0.75")


def test_build_lane_state_pins_packet_wallets_as_paper_only() -> None:
    packet = {
        "source_report": "capture/alpha_decay_report.json",
        "eligible_profile_count": 1,
        "alpha_status": "PASS",
        "profiles": [
            {
                "wallet": "0x1111111111111111111111111111111111111111",
                "fill_sample": 42,
                "copyable_rate_pct": 81.0,
                "mean_edge": 0.01,
                "median_edge": 0.005,
                "best_move_slice": {"entry_price_band": "0.25-0.50"},
            }
        ],
    }

    lane = build_lane_state_from_packet(packet)

    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["ranked_wallets"][0]["wallet"] == "0x1111111111111111111111111111111111111111"
    assert lane["ranked_wallets"][0]["alpha_entry_price_band"] == "0.25-0.50"
    assert lane["ranked_wallets"][0]["market_categories"] == ["btc_5m"]


def test_filter_rows_to_profile_bands_keeps_selected_buy_inside_band() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    lane = build_lane_state_from_packet(
        {
            "profiles": [
                {
                    "wallet": wallet,
                    "best_move_slice": {"entry_price_band": "0.25-0.50"},
                }
            ]
        }
    )
    base = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "transaction_hash": "0xaaa",
        "log_index": 1,
        "selected_wallet": wallet,
        "block_ts": 100.0,
        "received_at_s": 101.0,
        "decoded": {
            "decode_status": "OK",
            "asset": "asset-a",
            "side": "BUY",
            "price": 0.4,
            "size": 10,
        },
    }
    rows = [
        base,
        {**base, "log_index": 2, "decoded": {**base["decoded"], "price": 0.8}},
        {**base, "log_index": 3, "decoded": {**base["decoded"], "side": "SELL"}},
        {**base, "log_index": 4, "selected_wallet": "0x3333333333333333333333333333333333333333"},
    ]

    filtered, diagnostics = filter_rows_to_profile_bands(rows, lane)

    assert filtered == [base]
    assert diagnostics["selected_profile_band_rows"] == 1
    assert diagnostics["outside_profile_band"] == 1
    assert diagnostics["non_buy"] == 1
    assert diagnostics["non_selected_wallet"] == 1


def test_pending_disposition_enforces_two_second_horizon_and_lag() -> None:
    row = {
        "event": "polygon_orderfilled_log",
        "source": "polygon_ws",
        "transaction_hash": "0xabc",
        "log_index": 1,
        "received_at_s": 100.0,
        "decoded": {"decode_status": "OK", "asset": "a", "side": "BUY", "price": 0.4, "size": 2},
    }

    assert pending_disposition(row, now_s=101.9, horizon_s=2.0, max_observation_lag_s=5.0) == "WAIT"
    assert pending_disposition(row, now_s=102.0, horizon_s=2.0, max_observation_lag_s=5.0) == "OBSERVE"
    assert pending_disposition(row, now_s=107.1, horizon_s=2.0, max_observation_lag_s=5.0) == "MISSED_OBSERVATION_LAG"


def test_new_row_reader_starts_prospectively_at_eof(tmp_path) -> None:
    source = tmp_path / "feed.jsonl"
    source.write_text('{"event":"old"}\n')

    rows, offset, inode = _read_new_rows(str(source), offset=0, inode=0)
    assert rows == []
    source.write_text(source.read_text() + '{"event":"new"}\n')
    rows, _, _ = _read_new_rows(str(source), offset=offset, inode=inode)
    assert rows == [{"event": "new"}]


def test_launchd_payload_is_persistent_and_paper_only() -> None:
    payload = build_launchd_payload(python="/usr/bin/python3", stdout="/tmp/out", stderr="/tmp/err")

    assert payload["KeepAlive"] is True
    assert payload["RunAtLoad"] is True
    assert "run_alpha_decay_eligible_profile_paper_lane.py" in payload["ProgramArguments"][1]
    assert "--disable-source-base-overrides" in payload["ProgramArguments"]
    assert EXPERIMENT_ID == "alpha-decay-eligible-profiles-paper-20260719"


def test_preserve_alpha_fields_carries_profile_metadata_after_scoring() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    lane = build_lane_state_from_packet(
        {
            "profiles": [
                {
                    "wallet": wallet,
                    "fill_sample": 30,
                    "copyable_rate_pct": 75.0,
                    "mean_edge": 0.01,
                    "median_edge": 0.005,
                    "best_move_slice": {"entry_price_band": "0.25-0.50"},
                }
            ]
        }
    )
    measurement = {
        "ranked_wallets": [{"wallet": wallet}],
        "wallets": {wallet: {"wallet": wallet}},
    }

    preserve_alpha_fields(measurement, lane)

    assert measurement["ranked_wallets"][0]["alpha_entry_price_band"] == "0.25-0.50"
    assert measurement["wallets"][wallet]["alpha_copyable_rate_pct"] == 75.0
