from __future__ import annotations

from argparse import Namespace

from scripts.run_polygon_ws_shadow_service import _command, _rotate, parse_args


def test_polygon_ws_shadow_service_command_is_paper_only_probe(tmp_path) -> None:
    args = Namespace(
        duration_s=300.0,
        timeout_s=5.0,
        http_poll_s=5.0,
        ws_retry_s=2.0,
        lookback_blocks=20,
        output=tmp_path / "ws.jsonl",
        orderfilled_output=tmp_path / "orderfilled.jsonl",
        comparison_jsonl=tmp_path / "cmp.jsonl",
        dataapi_first_seen_jsonl=tmp_path / "dataapi.jsonl",
        polygon_wss_fallback_url=["wss://fallback"],
    )

    command = _command(args)

    assert "scripts/probe_polygon_orderfilled_ws.py" in command[1]
    assert "--comparison-jsonl" in command
    assert str(tmp_path / "cmp.jsonl") in command
    assert command[command.index("--orderfilled-output") + 1] == str(tmp_path / "orderfilled.jsonl")
    assert "--polygon-wss-fallback-url" in command
    assert "wss://fallback" in command


def test_polygon_ws_shadow_service_defaults_to_validated_wss_fallback(monkeypatch) -> None:
    monkeypatch.delenv("POLYGON_WSS_FALLBACK_URLS", raising=False)
    monkeypatch.setattr("sys.argv", ["run_polygon_ws_shadow_service.py"])

    args = parse_args()

    assert "wss://polygon.drpc.org" in args.polygon_wss_fallback_url


def test_polygon_ws_shadow_service_rotates_log(tmp_path) -> None:
    log = tmp_path / "service.log"
    log.write_text("x" * 12, encoding="utf-8")

    assert _rotate(log, max_bytes=10) is True
    assert not log.exists()
    assert (tmp_path / "service.log.1").read_text(encoding="utf-8") == "x" * 12
