"""Minimal utilities kept for wallet-copy execution."""

from __future__ import annotations

import asyncio
import functools
import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, TypeVar


F = TypeVar("F", bound=Callable[..., Any])

_shared_file_handler: logging.Handler | None = None
_shared_console_handler: logging.Handler | None = None


def _effective_log_path(log_path: str) -> str:
    """Keep pytest executor fixtures out of the production evidence log."""

    if log_path != "logs/wallet_copy.log":
        return log_path
    running_pytest = "PYTEST_CURRENT_TEST" in os.environ or any(
        "pytest" in part.lower() for part in (sys.argv[0], *sys.argv[1:])
    )
    return "logs/wallet_copy_test.log" if running_pytest else log_path


class SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Rotating handler tolerant of another process rotating the same log."""

    def doRollover(self) -> None:  # noqa: N802 - logging uses camelCase
        try:
            super().doRollover()
        except FileNotFoundError:
            if self.stream:
                try:
                    self.stream.close()
                finally:
                    self.stream = None
            if not self.delay:
                self.stream = self._open()


def utc_log_formatter() -> logging.Formatter:
    """Return a formatter whose timestamps are explicit UTC."""

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    formatter.converter = time.gmtime
    return formatter


def setup_logger(
    name: str,
    log_path: str = "logs/wallet_copy.log",
    level: int = logging.INFO,
) -> logging.Logger:
    """Set up a shared file and console logger."""

    global _shared_file_handler, _shared_console_handler

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    if logger.handlers:
        return logger

    log_path = _effective_log_path(log_path)
    if _shared_file_handler is None:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        formatter = utc_log_formatter()
        _shared_file_handler = SafeRotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        _shared_file_handler.setLevel(level)
        _shared_file_handler.setFormatter(formatter)

        _shared_console_handler = logging.StreamHandler()
        _shared_console_handler.setLevel(level)
        _shared_console_handler.setFormatter(formatter)

    logger.addHandler(_shared_file_handler)
    logger.addHandler(_shared_console_handler)
    return logger


def retry_async(
    max_attempts: int = 3,
    delay: float = 0.5,
    backoff: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[F], F]:
    """Retry an async function with exponential backoff."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            last_exception: BaseException | None = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exception = exc
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
            raise last_exception or RuntimeError(f"Failed after {max_attempts} attempts")

        return wrapper  # type: ignore[return-value]

    return decorator
