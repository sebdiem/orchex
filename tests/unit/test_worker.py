from __future__ import annotations

import types
import uuid

from orchex.config import Settings
from orchex.registry import TaskDefinition, TaskRegistry
from orchex.worker import Worker


def make_settings() -> Settings:
    return Settings(
        database_url="postgresql://user:pass@localhost:5432/db",
        db_schema="orchex",
        task_timeout_seconds=120,
        max_attempts=3,
        lease_seconds=30,
        claim_poll_interval=0.5,
        db_pool_min=1,
        db_pool_max=5,
        statement_timeout_ms=1000,
        metrics_flush_interval=10.0,
    )


def make_worker() -> Worker:
    return Worker(
        registry=TaskRegistry(),
        settings=make_settings(),
        dag_name="test_dag",
        worker_id="test-worker",
    )


def test_worker_uses_task_specific_timeout():
    worker = make_worker()
    task = TaskDefinition(name="custom", fn=lambda payload: payload, timeout=5)
    assert worker._task_timeout(task) == 5


def test_worker_falls_back_to_default_timeout():
    worker = make_worker()
    task = TaskDefinition(name="default", fn=lambda payload: payload, timeout=None)
    assert worker._task_timeout(task) == worker.settings.task_timeout_seconds


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executions = []

    def execute(self, sql, params):
        self.executions.append((sql, params))

    def fetchone(self):
        return self.row

    def close(self):
        pass


class FakeConn:
    def __init__(self, row):
        self.row = row
        self.committed = False

    def cursor(self, row_factory=None):
        return FakeCursor(self.row)

    def commit(self):
        self.committed = True


def test_mark_failure_blocks_dependents_when_dead(monkeypatch):
    worker = make_worker()
    called = {}

    def fake_block(self, run_id, task_name):
        called["args"] = (run_id, task_name)

    monkeypatch.setattr(
        worker, "_block_dependents", types.MethodType(fake_block, worker)
    )
    run_id = uuid.uuid4()
    row = {"status": "dead", "attempts": worker.settings.max_attempts}
    conn = FakeConn(row)
    worker._mark_failure(conn, run_id, "task_x", RuntimeError("boom"))
    assert called["args"] == (run_id, "task_x")


def test_mark_failure_does_not_block_when_not_dead(monkeypatch):
    worker = make_worker()

    def fake_block(self, run_id, task_name):  # pragma: no cover - should not execute
        raise AssertionError("block should not be called")

    monkeypatch.setattr(
        worker, "_block_dependents", types.MethodType(fake_block, worker)
    )
    run_id = uuid.uuid4()
    row = {"status": "failed", "attempts": 1}
    conn = FakeConn(row)
    worker._mark_failure(conn, run_id, "task_y", RuntimeError("boom"))


def test_record_success_invokes_unblock(monkeypatch):
    worker = make_worker()
    called = {}

    def fake_unblock(self, run_id, task_name):
        called["args"] = (run_id, task_name)

    monkeypatch.setattr(
        worker, "_maybe_unblock_dependents", types.MethodType(fake_unblock, worker)
    )
    run_id = uuid.uuid4()
    conn = FakeConn(row=None)
    worker._record_success(conn, run_id, "task_z", {"value": 1})
    assert called["args"] == (run_id, "task_z")
    assert conn.committed
