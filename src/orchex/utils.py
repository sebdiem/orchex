from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from typing import Any, Callable, TypeVar

from . import exceptions

logger = logging.getLogger(__name__)

T = TypeVar("T")


def run_with_timeout(fn: Callable[[], T], timeout: float) -> T:
    """
    Run `fn` inside a dedicated worker thread and enforce a timeout.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(fn)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise exceptions.TaskTimeoutError(
                f"Task exceeded timeout of {timeout} seconds"
            ) from exc


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def json_loads(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray, memoryview)):
        payload = payload.decode()
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


class RateCounter:
    """
    Track event counts and log rolling rates for worker observability.
    """

    def __init__(self, interval_seconds: float) -> None:
        self.interval = interval_seconds
        self.reset()

    def reset(self) -> None:
        self.window_start = time.monotonic()
        self.counts = {"claimed": 0, "succeeded": 0, "failed": 0}

    def tick(self, event: str) -> None:
        if event in self.counts:
            self.counts[event] += 1

    def maybe_flush(
        self, *, extra_factory: Callable[[], dict[str, Any]] | None = None
    ) -> None:
        now = time.monotonic()
        elapsed = now - self.window_start
        if elapsed < self.interval:
            return
        extra = extra_factory() if extra_factory else None
        rates = {k: v / elapsed for k, v in self.counts.items()}
        logger.info(
            "worker_metrics",
            extra={
                "action": "worker_metrics",
                "rates_per_sec": rates,
                **(extra or {}),
            },
        )
        self.reset()


__all__ = ["RateCounter", "json_dumps", "json_loads", "run_with_timeout"]
