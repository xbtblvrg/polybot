import argparse
import json
import os
import socket
import sys

from scripts import probe_polygon_orderfilled_ws as probe


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def _raw_log(*, block: int, tx: str, log_index: int, wallet: str) -> dict[str, object]:
    return {
        "address": "0xe111180000d2663c0091e4f400237545b87b996b",
        "blockNumber": hex(block),
        "transactionHash": tx,
        "logIndex": hex(log_index),
        "topics": [
            probe.ORDER_FILLED_TOPIC0,
            "0x" + "1" * 64,
            _topic_address(wallet),
            _topic_address("0x" + "b" * 40),
        ],
        "data": "0x",
    }


def test_http_tail_poll_accumulates_new_blocks_and_dedupes(monkeypatch) -> None:
    wallet = "0x" + "a" * 40
    args = argparse.Namespace(
        polygon_rpc_url="https://rpc.example",
        timeout_s=1.0,
        topic0=probe.ORDER_FILLED_TOPIC0,
    )
    block_numbers = [101, 102]
    logs_by_block = {
        101: [
            _raw_log(block=101, tx="0xaaa", log_index=7, wallet=wallet),
            _raw_log(block=101, tx="0xaaa", log_index=7, wallet=wallet),
        ],
        102: [_raw_log(block=102, tx="0xbbb", log_index=8, wallet=wallet)],
    }

    def fake_rpc_post(_url, method, params, *, timeout_s):
        if method == "eth_blockNumber":
            return hex(block_numbers.pop(0))
        if method == "eth_getLogs":
            request = params[0]
            from_block = int(request["fromBlock"], 16)
            to_block = int(request["toBlock"], 16)
            rows = []
            for block in range(from_block, to_block + 1):
                rows.extend(logs_by_block.get(block, []))
            return rows
        if method == "eth_getBlockByNumber":
            return {"timestamp": hex(1_783_300_000 + int(params[0], 16))}
        raise AssertionError(method)

    monkeypatch.setattr(probe, "_rpc_post", fake_rpc_post)
    seen: set[tuple[str, int | None]] = set()

    rows, latest = probe._poll_http_tail(
        args,
        ["0xe111180000d2663c0091e4f400237545b87b996b"],
        last_seen_block=100,
        seen_log_keys=seen,
        registry_wallets={wallet},
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
        block_ts_cache={},
    )
    assert latest == 101
    assert len(rows) == 1
    assert rows[0]["source"] == "polygon_http_getLogs_tail"
    assert rows[0]["http_capture_kind"] == "tail"
    assert rows[0]["is_registry_wallet"] is True

    rows, latest = probe._poll_http_tail(
        args,
        ["0xe111180000d2663c0091e4f400237545b87b996b"],
        last_seen_block=latest,
        seen_log_keys=seen,
        registry_wallets={wallet},
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
        block_ts_cache={},
    )
    assert latest == 102
    assert len(rows) == 1
    assert rows[0]["transaction_hash"] == "0xbbb"
    assert len(seen) == 2


def test_ws_idle_timeout_keeps_subscription_open(monkeypatch, tmp_path) -> None:
    class FakeWebSocketTimeoutException(Exception):
        pass

    class FakeConnection:
        def __init__(self) -> None:
            self.recv_calls = 0
            self.closed = False
            self.sent: list[str] = []
            self.timeout = None

        def settimeout(self, timeout: float) -> None:
            self.timeout = timeout

        def send(self, payload: str) -> None:
            self.sent.append(payload)

        def recv(self) -> str:
            self.recv_calls += 1
            if self.recv_calls == 1:
                return json.dumps({"jsonrpc": "2.0", "result": "0xsub", "id": 1})
            raise FakeWebSocketTimeoutException("Connection timed out")

        def close(self) -> None:
            self.closed = True

    fake_connection = FakeConnection()

    class FakeWebSocketModule:
        WebSocketTimeoutException = FakeWebSocketTimeoutException

        @staticmethod
        def create_connection(_url: str, timeout: float):
            assert timeout == 1.0
            return fake_connection

    clock = {"now": 1000.0}

    def fake_time() -> float:
        clock["now"] += 0.4
        return clock["now"]

    output = tmp_path / "polygon_ws.jsonl"
    args = argparse.Namespace(
        output=str(output),
        polygon_wss_url="wss://example.test",
        timeout_s=1.0,
        duration_s=1.0,
        topic0=probe.ORDER_FILLED_TOPIC0,
    )
    monkeypatch.setitem(sys.modules, "websocket", FakeWebSocketModule)
    monkeypatch.setattr(probe.time, "time", fake_time)

    rows, stats = probe._subscribe_ws(
        args,
        ["0xe111180000d2663c0091e4f400237545b87b996b"],
        registry_wallets=set(),
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
        block_ts_cache={},
    )

    assert rows == []
    assert stats["frames"] == 1
    assert fake_connection.closed is True
    events = [json.loads(line)["event"] for line in output.read_text(encoding="utf-8").splitlines()]
    assert events == [
        "polygon_ws_connection_open",
        "polygon_ws_raw_frame",
        "polygon_ws_subscription_ack",
        "polygon_ws_connection_close",
    ]


def test_ws_appends_decoded_logs_to_orderfilled_only_sidecar(monkeypatch, tmp_path) -> None:
    class FakeWebSocketTimeoutException(Exception):
        pass

    wallet = "0x" + "a" * 40
    raw_log = _raw_log(block=101, tx="0xaaa", log_index=7, wallet=wallet)

    class FakeConnection:
        def __init__(self) -> None:
            self.messages = [
                json.dumps({"jsonrpc": "2.0", "result": "0xsub", "id": 1}),
                json.dumps({"jsonrpc": "2.0", "result": "0xheads", "id": 2}),
                json.dumps(
                    {
                        "params": {
                            "result": {
                                "number": "0x65",
                                "timestamp": hex(999),
                            }
                        }
                    }
                ),
                json.dumps({"params": {"result": raw_log}}),
            ]

        def settimeout(self, _timeout: float) -> None:
            pass

        def send(self, _payload: str) -> None:
            pass

        def recv(self) -> str:
            if self.messages:
                return self.messages.pop(0)
            raise FakeWebSocketTimeoutException("done")

        def close(self) -> None:
            pass

    class FakeWebSocketModule:
        WebSocketTimeoutException = FakeWebSocketTimeoutException

        @staticmethod
        def create_connection(_url: str, timeout: float):
            return FakeConnection()

    clock = {"now": 1000.0}

    def fake_time() -> float:
        clock["now"] += 0.25
        return clock["now"]

    mixed = tmp_path / "mixed.jsonl"
    sidecar = tmp_path / "orderfilled.jsonl"
    args = argparse.Namespace(
        output=str(mixed),
        orderfilled_output=str(sidecar),
        polygon_wss_url="wss://example.test",
        polygon_rpc_url="https://rpc.example",
        timeout_s=1.0,
        duration_s=2.0,
        topic0=probe.ORDER_FILLED_TOPIC0,
    )
    monkeypatch.setitem(sys.modules, "websocket", FakeWebSocketModule)
    monkeypatch.setattr(probe.time, "time", fake_time)
    monkeypatch.setattr(
        probe,
        "_rpc_post",
        lambda *_args, **_kwargs: pytest.fail("newHeads cache must avoid synchronous block timestamp RPC"),
    )
    wake_calls = []
    monkeypatch.setattr(probe, "_notify_orderfilled_wake", lambda path: wake_calls.append(path) or True)

    rows, _stats = probe._subscribe_ws(
        args,
        ["0xe111180000d2663c0091e4f400237545b87b996b"],
        registry_wallets={wallet},
        exchange_addresses={"0xe111180000d2663c0091e4f400237545b87b996b"},
        block_ts_cache={},
    )

    assert len(rows) == 1
    sidecar_rows = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
    assert [row["event"] for row in sidecar_rows] == ["polygon_orderfilled_log"]
    assert sidecar_rows[0]["transaction_hash"] == "0xaaa"
    assert sidecar_rows[0]["sidecar_appended_at_s"] > sidecar_rows[0]["received_at_s"]
    assert wake_calls == [""]


def test_registry_wallets_can_disable_default_registry(tmp_path) -> None:
    active_registry = tmp_path / "active.json"
    active_registry.write_text(
        json.dumps(
            {
                "kind": "wallet_copy_registry",
                "wallets": [
                    {
                        "name": "target",
                        "address": "0x" + "a" * 40,
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        registry=[],
        active_registry=str(active_registry),
        disable_default_registry=True,
    )

    wallets = probe._registry_wallets(args)

    assert wallets == {"0x" + "a" * 40}


def test_http_backfill_never_enters_realtime_orderfilled_sidecar(monkeypatch, tmp_path) -> None:
    sidecar = tmp_path / "orderfilled.jsonl"
    wake_calls = []
    monkeypatch.setattr(probe, "_notify_orderfilled_wake", lambda path: wake_calls.append(path) or True)
    args = argparse.Namespace(
        orderfilled_output=str(sidecar),
        orderfilled_wake_socket="/tmp/unit-orderfilled.sock",
    )

    probe._append_orderfilled(args, {"transaction_hash": "0xhttp"}, realtime=False)

    assert not sidecar.exists()
    assert wake_calls == []


def test_realtime_orderfilled_fanout_carries_full_payload(tmp_path) -> None:
    fanout_path = f"/tmp/wide-fanout-{os.getpid()}.sock"
    try:
        os.unlink(fanout_path)
    except FileNotFoundError:
        pass
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.bind(fanout_path)
    receiver.settimeout(1.0)
    args = argparse.Namespace(
        orderfilled_output=str(tmp_path / "orderfilled.jsonl"),
        orderfilled_wake_socket="",
        orderfilled_fanout_socket=fanout_path,
    )

    probe._append_orderfilled(
        args,
        {"transaction_hash": "0xdirect", "recv_monotonic_s": 123.0},
        realtime=True,
    )

    payload = json.loads(receiver.recv(65535))
    receiver.close()
    os.unlink(fanout_path)
    assert payload["event"] == "polygon_orderfilled_log"
    assert payload["transaction_hash"] == "0xdirect"
    assert payload["recv_monotonic_s"] == 123.0
    assert payload["fanout_sent_monotonic_s"] > 0
    assert len(json.dumps(payload).encode("utf-8")) < 2048


def test_realtime_orderfilled_fanout_does_not_require_sidecar_output() -> None:
    fanout_path = f"/tmp/wide-fanout-only-{os.getpid()}.sock"
    try:
        os.unlink(fanout_path)
    except FileNotFoundError:
        pass
    receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    receiver.bind(fanout_path)
    receiver.settimeout(1.0)
    args = argparse.Namespace(
        orderfilled_output="",
        orderfilled_wake_socket="",
        orderfilled_fanout_socket=fanout_path,
    )

    probe._append_orderfilled(
        args,
        {"transaction_hash": "0xfanout-only", "recv_monotonic_s": 456.0},
        realtime=True,
    )

    payload = json.loads(receiver.recv(65535))
    receiver.close()
    os.unlink(fanout_path)
    assert payload["transaction_hash"] == "0xfanout-only"
    assert payload["recv_monotonic_s"] == 456.0


def test_orderfilled_fanout_drops_raw_log_bytes_but_keeps_parsed_identity() -> None:
    payload = probe._compact_orderfilled_fanout(
        {
            "source": "polygon_ws",
            "transaction_hash": "0xabc",
            "log_index": 7,
            "data": "0x" + "a" * 10_000,
            "topics": ["0x" + "b" * 64] * 4,
            "maker": "0x" + "1" * 40,
            "taker": "0x" + "2" * 40,
            "decoded": {
                "decode_status": "OK",
                "asset": "123",
                "price": 0.4,
                "size": 10.0,
                "maker_side": "BUY",
                "fee_raw": 100,
            },
        }
    )

    assert payload["transaction_hash"] == "0xabc"
    assert payload["log_index"] == 7
    assert payload["decoded"] == {
        "decode_status": "OK",
        "asset": "123",
        "price": 0.4,
        "size": 10.0,
        "maker": None,
        "taker": None,
        "maker_side": "BUY",
        "side": None,
    }
    assert "data" not in payload
    assert "topics" not in payload
    assert len(json.dumps(payload).encode("utf-8")) < 2048


def test_non_registry_orderfilled_is_not_fanned_out(monkeypatch, tmp_path) -> None:
    fanout_calls = []
    monkeypatch.setattr(
        probe,
        "_fanout_orderfilled",
        lambda path, payload: fanout_calls.append((path, payload)) or True,
    )
    args = argparse.Namespace(
        orderfilled_output=str(tmp_path / "all-orderfilled.jsonl"),
        orderfilled_wake_socket="",
        orderfilled_fanout_socket="/tmp/wide.sock",
    )

    probe._append_orderfilled(
        args,
        {"transaction_hash": "0xmarket", "is_registry_wallet": False},
        realtime=True,
    )

    assert fanout_calls == []
    assert (tmp_path / "all-orderfilled.jsonl").exists()
