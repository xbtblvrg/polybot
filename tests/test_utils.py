from __future__ import annotations

import logging

from src.utils import utc_log_formatter


def test_utc_log_formatter_uses_z_suffix() -> None:
    record = logging.LogRecord(
        name="wallet-copy-test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="sample",
        args=(),
        exc_info=None,
    )
    record.created = 0.0

    assert utc_log_formatter().format(record).startswith("1970-01-01T00:00:00Z - ")
