from __future__ import annotations

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
