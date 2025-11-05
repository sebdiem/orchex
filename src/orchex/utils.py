from __future__ import annotations

import json
import logging
import multiprocessing
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any, ParamSpec, TypeVar

from . import exceptions

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


def _task_entry(
    fn: Callable[P, T],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    conn: Connection,
) -> None:
    """
    Run the task function inside a subprocess and ship the result/exception
    back to the parent via a Pipe connection.
    """
    try:
        result = fn(*args, **kwargs)
        conn.send(("ok", result))
    except BaseException as exc:
        try:
            conn.send(("err", exc))
        except Exception as send_exc:  # pragma: no cover - serialization failure
            conn.send(
                (
                    "err",
                    RuntimeError(
                        f"Task failed with non-serializable error: {exc!r} ({send_exc})"
                    ),
                )
            )
    finally:
        conn.close()


def run_with_timeout(
    fn: Callable[P, T], timeout: float, *args: P.args, **kwargs: P.kwargs
) -> T:
    """
    Run `fn` inside an isolated subprocess and enforce a hard timeout.
    """
    ctx = multiprocessing.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=_task_entry,
        args=(fn, args, kwargs, child_conn),
        daemon=False,
    )
    proc.start()
    child_conn.close()
    try:
        if not parent_conn.poll(timeout):
            raise exceptions.TaskTimeoutError(
                f"Task exceeded timeout of {timeout} seconds"
            )
        status, payload = parent_conn.recv()
    except exceptions.TaskTimeoutError:
        proc.terminate()
        proc.join()
        raise
    except EOFError as exc:
        proc.join()
        raise RuntimeError("Task subprocess exited without sending a result") from exc
    except BaseException:
        proc.terminate()
        proc.join()
        raise
    finally:
        parent_conn.close()
    proc.join()
    if status == "ok":
        return payload
    if isinstance(payload, Exception):
        raise payload
    raise RuntimeError(payload)


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def json_loads(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode()
    elif isinstance(payload, memoryview):
        payload = payload.tobytes().decode()
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
