import gzip
import json

from src.wallet_copy.store import atomic_write_json, load_json


def test_load_json_reads_gzip_payload(tmp_path):
    path = tmp_path / "state.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"wallets": [{"address": "0x1"}]}, handle)

    assert load_json(path, default={}) == {"wallets": [{"address": "0x1"}]}


def test_atomic_write_json_writes_gzip_payload(tmp_path):
    path = tmp_path / "state.json.gz"

    atomic_write_json(path, {"schema_version": 1, "wallets": [{"address": "0x2"}]})

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        assert json.load(handle) == {"schema_version": 1, "wallets": [{"address": "0x2"}]}
    assert load_json(path, default={})["wallets"][0]["address"] == "0x2"
