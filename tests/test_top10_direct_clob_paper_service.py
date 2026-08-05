import json

from scripts.launch_top10_direct_clob_paper_service import build_launchd_payload
from scripts.run_top10_direct_clob_paper_service import DIRECT_CLOB, _read_new_rows


def test_reader_starts_prospectively_and_resumes_from_offset(tmp_path):
    source = tmp_path / "rtds.jsonl"
    source.write_text(json.dumps({"event": "rtds_trade_event", "id": "old"}) + "\n")
    rows, offset, inode = _read_new_rows(str(source), offset=0, inode=0)
    assert rows == []
    with source.open("a") as handle:
        handle.write(json.dumps({"event": "rtds_trade_event", "id": "new"}) + "\n")
    rows, next_offset, next_inode = _read_new_rows(str(source), offset=offset, inode=inode)
    assert [row["id"] for row in rows] == ["new"]
    assert next_offset > offset
    assert next_inode == inode


def test_launchd_service_is_persistent_and_direct_clob_is_explicit():
    payload = build_launchd_payload(
        python="/usr/bin/python3", stdout="/tmp/top10.out", stderr="/tmp/top10.err"
    )
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert "run_top10_direct_clob_paper_service.py" in payload["ProgramArguments"][1]
    assert payload["ProgramArguments"][-2:] == ["--max-book-fetches", "20"]
    assert DIRECT_CLOB == "https://clob.polymarket.com"
