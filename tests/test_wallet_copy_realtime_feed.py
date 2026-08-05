from __future__ import annotations

from src.wallet_copy.realtime_feed import (
    decode_polygon_orderfilled_v2,
    normalize_polygon_orderfilled_row,
    parse_rtds_trade_frame,
)
from scripts.report_detection_latency import build_summary
from scripts.capture_dataapi_first_seen import _rotated_wallets
from scripts.probe_polygon_orderfilled_ws import _comparison_rows, _dedupe_log_rows, _utc_iso_from_s, _wss_endpoints


def test_parse_rtds_trade_frame_extracts_wallet_trade() -> None:
    raw = {
        "topic": "activity",
        "type": "trades",
        "payload": {
            "proxyWallet": "0x0492102c6c7f1323f6c02f65cbafb0b633c732e9",
            "side": "BUY",
            "asset": "123",
            "conditionId": "0xabc",
            "marketSlug": "btc-updown-5m-test",
            "price": "0.51",
            "size": "12.5",
            "timestamp": 1000,
            "transactionHash": "0xtx",
        },
    }

    events = parse_rtds_trade_frame(raw, received_at_s=1001.25)

    assert len(events) == 1
    event = events[0]
    assert event.source_wallet == "0x0492102c6c7f1323f6c02f65cbafb0b633c732e9"
    assert event.side == "BUY"
    assert event.price == 0.51
    assert event.size == 12.5
    assert event.event_ts == 1000
    assert event.asdict()["event_id"].startswith("rt_")


def test_parse_rtds_orders_matched_frame_extracts_wallet_trade() -> None:
    raw = {
        "topic": "activity",
        "type": "orders_matched",
        "payload": {
            "proxyWallet": "0x0492102c6c7f1323f6c02f65cbafb0b633c732e9",
            "action": "BUY",
            "tokenId": "123",
            "condition_id": "0xabc",
            "market_slug": "btc-updown-5m-test",
            "price": 0.52,
            "amount": 7,
            "created_at": 1000,
            "transaction_hash": "0xtx",
        },
    }

    events = parse_rtds_trade_frame(raw, received_at_s=1001.25)

    assert len(events) == 1
    assert events[0].source_wallet == "0x0492102c6c7f1323f6c02f65cbafb0b633c732e9"
    assert events[0].side == "BUY"
    assert events[0].asset == "123"


def test_parse_rtds_trade_frame_accepts_site_slug_fields() -> None:
    raw = {
        "topic": "activity",
        "type": "trades",
        "payload": {
            "proxyWallet": "0x00053804ab4265667939b9d99a32051f5872222a",
            "side": "BUY",
            "asset": "123",
            "conditionId": "0xabc",
            "eventSlug": "btc-updown-15m-1783101600",
            "slug": "btc-updown-15m-1783101600",
            "price": "0.72",
            "size": "10",
            "timestamp": 1783101889,
            "transactionHash": "0xtx",
        },
    }

    events = parse_rtds_trade_frame(raw, received_at_s=1783101889.5)

    assert len(events) == 1
    assert events[0].market_slug == "btc-updown-15m-1783101600"


def test_normalize_polygon_orderfilled_row_extracts_indexed_wallets() -> None:
    row = {
        "event": "polygon_orderfilled_log",
        "received_at_s": 123.0,
        "transaction_hash": "0xabc",
        "maker": "0x6c2543afc311bfe6ba6db0d848c5dee64ec2b30c",
        "taker": "0xe29042f5d913dcc4015aab3455c13c58514ca33f",
        "decoded": {"maker_side": "BUY"},
    }

    event = normalize_polygon_orderfilled_row(row)

    assert event is not None
    assert event.source == "polygon_orderfilled"
    assert event.source_wallet == "0x6c2543afc311bfe6ba6db0d848c5dee64ec2b30c"
    assert event.taker == "0xe29042f5d913dcc4015aab3455c13c58514ca33f"
    assert event.maker_side == "BUY"


def test_decode_polygon_orderfilled_v2_empirical_buy_layout() -> None:
    data_words = [
        0,
        int("86275818593662337375044400429456228207390856962078759470222739156195386219680"),
        3419999,
        6452829,
        112510,
        0,
        0,
    ]
    row = {
        "selected_wallet": "0x0c7c5204404e9d5402d258fedac59c7212bae4cb",
        "maker": "0x0c7c5204404e9d5402d258fedac59c7212bae4cb",
        "taker": "0xe111180000d2663c0091e4f400237545b87b996b",
        "topic3": "0xe111180000d2663c0091e4f400237545b87b996b",
        "order_hash": "0x" + "12" * 32,
        "data": "0x" + "".join(f"{word:064x}" for word in data_words),
    }

    decoded = decode_polygon_orderfilled_v2(
        row,
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
    )

    assert decoded["decode_status"] == "OK"
    assert decoded["side"] == "BUY"
    assert decoded["maker_side"] == "BUY"
    assert decoded["size"] == 6.452829
    assert round(decoded["price"], 8) == 0.52999994
    assert decoded["maker"] == "0x0c7c5204404e9d5402d258fedac59c7212bae4cb"
    assert decoded["maker_is_exchange"] is False
    assert decoded["taker_is_exchange"] is True
    assert decoded["order_hash"] != decoded["maker"]


def test_decode_polygon_orderfilled_v2_empirical_sell_layout() -> None:
    data_words = [
        1,
        int("89812890269413448538379351122167722063804107105764121081599639279947378595589"),
        22260000,
        10684800,
        388920,
        0,
        0,
    ]
    row = {
        "selected_wallet": "0x30088d72c826065532ca9d0b25c66ccaa1c63c76",
        "maker": "0x30088d72c826065532ca9d0b25c66ccaa1c63c76",
        "taker": "0xe111180000d2663c0091e4f400237545b87b996b",
        "topic3": "0xe111180000d2663c0091e4f400237545b87b996b",
        "order_hash": "0x62f89354e4bd927d80fab718ad8f988ae396e218504bceabcdf3c02162fb4326",
        "data": "0x" + "".join(f"{word:064x}" for word in data_words),
    }

    decoded = decode_polygon_orderfilled_v2(
        row,
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
    )

    assert decoded["decode_status"] == "OK"
    assert decoded["side"] == "SELL"
    assert decoded["maker_side"] == "SELL"
    assert decoded["size"] == 22.26
    assert decoded["price"] == 0.48
    assert decoded["asset"] == "89812890269413448538379351122167722063804107105764121081599639279947378595589"
    assert decoded["maker_is_exchange"] is False
    assert decoded["taker_is_exchange"] is True
    assert decoded["order_hash"] != decoded["maker"]


def test_detection_latency_summary_counts_sources() -> None:
    rtds_rows = [
        {
            "event": "rtds_raw_frame",
            "data_frame_like": True,
            "captured_at_s": 1002.0,
            "raw": '{"topic":"activity","type":"trades","payload":{"proxyWallet":"0x0492102c6c7f1323f6c02f65cbafb0b633c732e9","timestamp":1000}}',
        }
    ]
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "received_at_s": 2000.0,
            "ws_receive_lag_signed_s": -0.5,
            "transaction_hash": "0xabc",
            "source": "polygon_ws",
            "maker": "0x6c2543afc311bfe6ba6db0d848c5dee64ec2b30c",
            "taker": "0xe29042f5d913dcc4015aab3455c13c58514ca33f",
            "is_registry_wallet": True,
        }
    ]

    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": "0x6c2543afc311bfe6ba6db0d848c5dee64ec2b30c",
            "transactionHash": "0xabc",
            "captured_at_s": 2001.0,
        }
    ]

    summary = build_summary(rtds_rows, polygon_rows, dataapi_rows)

    assert summary["dataapi_first_seen"]["rows"] == 1
    assert summary["rtds"]["normalized_events"] == 1
    assert summary["rtds"]["receive_lag"]["p50_s"] == 2.0
    assert summary["polygon_ws"]["normalized_events"] == 1
    assert summary["polygon_ws"]["unique_txs"] == 1
    assert summary["polygon_ws"]["registry_rows"] == 1
    assert summary["polygon_ws"]["receive_lag"]["p50_s"] == -0.5


def test_dataapi_first_seen_rotation_advances_wallet_order() -> None:
    wallets = [
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x0000000000000000000000000000000000000003",
    ]

    assert _rotated_wallets(wallets, 0) == wallets
    assert _rotated_wallets(wallets, 1) == [wallets[1], wallets[2], wallets[0]]
    assert _rotated_wallets(wallets, 4) == [wallets[1], wallets[2], wallets[0]]


def test_detection_latency_excludes_backfill_from_acceptance_stats() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "registry_wallets": [wallet],
            "received_at_s": 101.0,
            "block_ts": 99.0,
            "ws_receive_lag_signed_s": 2.0,
            "maker": wallet,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 105.0,
            "poller_started_at_s": 100.0,
            "poll_interval_s": 5.0,
            "backfill": True,
        }
    ]

    summary = build_summary([], polygon_rows, dataapi_rows)

    assert summary["polygon_ws"]["matched"]["count"] == 0
    assert summary["polygon_ws"]["matched"]["matched_backfill"]["count"] == 1


def test_detection_latency_counts_active_set_delayed_first_seen_as_match() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "registry_wallets": [wallet],
            "received_at_s": 201.0,
            "block_ts": 200.0,
            "ws_receive_lag_signed_s": 1.0,
            "maker": wallet,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 320.0,
            "poller_started_at_s": 321.0,
            "poll_interval_s": 5.0,
            "backfill": False,
            "endpoint": "active_set_dataapi_poller",
        }
    ]

    summary = build_summary([], polygon_rows, dataapi_rows)

    assert summary["polygon_ws"]["matched"]["count"] == 1
    assert summary["polygon_ws"]["matched"]["matched_backfill"]["count"] == 0
    assert summary["polygon_ws"]["matched"]["rows"][0]["ws_lead_s"] == 119.0


def test_detection_latency_matches_wallet_on_ws_maker_taker_side() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "selected_wallet": "0x0000000000000000000000000000000000000002",
            "registry_wallets": [],
            "received_at_s": 201.0,
            "block_ts": 200.0,
            "ws_receive_lag_signed_s": 1.0,
            "maker": "0x0000000000000000000000000000000000000003",
            "taker": wallet,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 320.0,
            "backfill": False,
            "endpoint": "active_set_dataapi_poller",
        }
    ]

    summary = build_summary([], polygon_rows, dataapi_rows)

    assert summary["polygon_ws"]["matched"]["count"] == 1
    assert summary["polygon_ws"]["matched"]["misses"]["dataapi_not_in_polygon_ws_count"] == 0


def test_detection_latency_reports_dataapi_present_only_in_non_ws_rows() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_http_getLogs_tail",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "received_at_s": 201.0,
            "block_ts": 200.0,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 320.0,
            "backfill": False,
            "endpoint": "active_set_dataapi_poller",
        }
    ]

    summary = build_summary([], polygon_rows, dataapi_rows)
    misses = summary["polygon_ws"]["matched"]["misses"]

    assert summary["polygon_ws"]["matched"]["count"] == 0
    assert misses["dataapi_not_in_polygon_ws_count"] == 1
    assert misses["dataapi_present_non_ws_count"] == 1
    assert misses["dataapi_absent_all_polygon_count"] == 0


def test_detection_latency_reports_unbounded_rows_explicitly() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_http_getLogs_tail",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "captured_at_s": 201.0,
            "block_number": 123,
            "block_ts": None,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 320.0,
            "backfill": False,
        }
    ]

    misses = build_summary([], polygon_rows, dataapi_rows)["polygon_ws"]["matched"]["misses"]
    unbounded = misses["unbounded_rows"]

    assert unbounded["polygon_orderfilled_count"] == 1
    assert unbounded["polygon_non_ws_count"] == 1
    assert unbounded["dataapi_first_seen_count"] == 1
    assert unbounded["polygon_sample"][0]["block_number"] == 123


def test_detection_latency_miss_stats_filter_to_overlap_window() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xcovered",
            "selected_wallet": wallet,
            "received_at_s": 201.0,
            "block_ts": 200.0,
        }
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xold",
            "timestamp": 100.0,
            "captured_at_s": 320.0,
            "backfill": False,
        },
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xfuture",
            "timestamp": 300.0,
            "captured_at_s": 320.0,
            "backfill": False,
        },
    ]

    misses = build_summary([], polygon_rows, dataapi_rows)["polygon_ws"]["matched"]["misses"]

    assert misses["dataapi_backfill_false_keys"] == 2
    assert misses["dataapi_backfill_false_keys_in_overlap"] == 0
    assert misses["dataapi_not_in_polygon_ws_count"] == 0
    assert misses["dataapi_absent_all_polygon_count"] == 0


def test_detection_latency_ws_missing_dataapi_counts_only_active_wallets() -> None:
    active_wallet = "0x0000000000000000000000000000000000000001"
    inactive_wallet = "0x0000000000000000000000000000000000000002"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xmissing",
            "selected_wallet": active_wallet,
            "received_at_s": 201.0,
            "block_ts": 200.0,
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xnoise",
            "selected_wallet": inactive_wallet,
            "received_at_s": 201.0,
            "block_ts": 200.0,
        },
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": active_wallet,
            "transactionHash": "0xseen",
            "timestamp": 200.0,
            "captured_at_s": 320.0,
            "backfill": False,
        }
    ]

    misses = build_summary([], polygon_rows, dataapi_rows)["polygon_ws"]["matched"]["misses"]

    assert misses["active_wallet_count"] == 1
    assert misses["ws_not_in_dataapi_count"] == 1
    assert misses["ws_not_in_dataapi_sample"] == [
        {"wallet": active_wallet, "tx": "0xmissing"}
    ]


def test_detection_latency_collapses_duplicate_tx_wallet_to_earliest_ws() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    polygon_rows = [
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "registry_wallets": [wallet],
            "received_at_s": 111.0,
            "block_ts": 110.0,
            "ws_receive_lag_signed_s": 1.0,
            "maker": wallet,
        },
        {
            "event": "polygon_orderfilled_log",
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "selected_wallet": wallet,
            "registry_wallets": [wallet],
            "received_at_s": 110.5,
            "block_ts": 110.0,
            "ws_receive_lag_signed_s": 0.5,
            "maker": wallet,
        },
    ]
    dataapi_rows = [
        {
            "event": "dataapi_first_seen",
            "wallet": wallet,
            "transactionHash": "0xabc",
            "captured_at_s": 112.0,
            "poller_started_at_s": 100.0,
            "poll_interval_s": 5.0,
            "backfill": False,
        }
    ]

    summary = build_summary([], polygon_rows, dataapi_rows)
    matched = summary["polygon_ws"]["matched"]

    assert matched["count"] == 1
    assert matched["rows"][0]["ws_recv_ts"] == 110.5
    assert matched["ws_before_dataapi_count"] == 1


def test_polygon_ws_probe_writes_dataapi_comparison_rows() -> None:
    wallet = "0x0000000000000000000000000000000000000001"
    rows = _comparison_rows(
        [
            {
                "source": "polygon_ws",
                "selected_wallet": wallet,
                "transaction_hash": "0xabc",
                "received_at_s": 101.5,
                "block_ts": 100.0,
            }
        ],
        dataapi_first_seen_rows=[
            {
                "event": "dataapi_first_seen",
                "wallet": wallet,
                "transactionHash": "0xabc",
                "captured_at_s": 165.0,
                "poll_interval_s": 5.0,
                "backfill": False,
            }
        ],
    )

    assert rows[0]["match_status"] == "MATCHED_DATAAPI_FIRST_SEEN"
    assert rows[0]["ws_detection_lag_s"] == 1.5
    assert rows[0]["dataapi_detection_lag_s"] == 65.0
    assert rows[0]["ws_lead_s"] == 63.5
    assert rows[0]["live_orders_allowed"] is False


def test_polygon_ws_probe_dedupes_fallback_endpoints() -> None:
    from argparse import Namespace

    args = Namespace(
        polygon_wss_url="wss://primary",
        polygon_wss_fallback_url=["wss://fallback", "wss://primary", ""],
    )

    assert _wss_endpoints(args) == ["wss://primary", "wss://fallback"]


def test_polygon_ws_probe_unions_dual_wss_rows_by_log_key() -> None:
    rows = _dedupe_log_rows(
        [
            {
                "transaction_hash": "0xabc",
                "log_index": 7,
                "captured_at_s": 101.0,
                "wss_url": "wss://publicnode",
            },
            {
                "transaction_hash": "0xabc",
                "log_index": 7,
                "captured_at_s": 100.5,
                "wss_url": "wss://drpc",
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0]["captured_at_s"] == 100.5
    assert rows[0]["wss_urls_seen"] == ["wss://drpc", "wss://publicnode"]


def test_polygon_ws_probe_formats_capture_iso_from_capture_ts() -> None:
    assert _utc_iso_from_s(1783467433.438787) == "2026-07-07T23:37:13.438787+00:00"
