from __future__ import annotations

import logging
import multiprocessing
import time

import pytest

from orchex import exceptions, utils


def _sleepy():
    time.sleep(0.05)
    return "done"


def test_run_with_timeout_raises_custom_error():
    with pytest.raises(exceptions.TaskTimeoutError):
        utils.run_with_timeout(_sleepy, timeout=0.01)


def _slow_task(shared):
    time.sleep(0.2)
    shared["finished"] = True
    return "ok"


def test_run_with_timeout_terminates_subprocess():
    manager = multiprocessing.Manager()
    shared = manager.dict()

    with pytest.raises(exceptions.TaskTimeoutError):
        utils.run_with_timeout(_slow_task, timeout=0.05, shared=shared)

    # Give the child a moment in case it somehow escaped termination.
    time.sleep(0.05)
    assert "finished" not in shared


def test_json_helpers_handle_bytes_and_sort_keys():
    payload = {"b": 1, "a": 2}
    serialized = utils.json_dumps(payload)
    assert serialized == '{"a":2,"b":1}'
    assert utils.json_loads(serialized) == {"a": 2, "b": 1}
    assert utils.json_loads(serialized.encode()) == {"a": 2, "b": 1}
    assert utils.json_loads(None) is None


def test_rate_counter_flushes_after_interval(monkeypatch, caplog):
    class FakeTime:
        def __init__(self) -> None:
            self.current = 0.0

        def monotonic(self) -> float:
            return self.current

    fake_time = FakeTime()
    monkeypatch.setattr(utils, "time", fake_time)
    caplog.set_level(logging.INFO)

    counter = utils.RateCounter(interval_seconds=1.0)
    counter.tick("claimed")
    counter.tick("succeeded")
    counter.maybe_flush()
    assert not caplog.records

    fake_time.current = 2.0
    counter.tick("failed")
    counter.maybe_flush(extra_factory=lambda: {"worker_id": "worker-1"})

    assert caplog.records
    record = caplog.records[-1]
    assert record.action == "worker_metrics"
    assert record.worker_id == "worker-1"
    assert counter.counts == {"claimed": 0, "succeeded": 0, "failed": 0}
