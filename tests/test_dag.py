from __future__ import annotations

from orchex.config import Settings
from orchex.dag import Dag


def make_settings() -> Settings:
    return Settings(
        database_url="postgresql://user:pass@localhost:5432/db",
        db_schema="orchex",
        task_timeout_seconds=60,
        max_attempts=3,
        lease_seconds=30,
        claim_poll_interval=0.5,
        db_pool_min=1,
        db_pool_max=4,
        statement_timeout_ms=1000,
        metrics_flush_interval=5.0,
    )


def test_dag_registers_tasks_via_decorator():
    dag = Dag("analytics", settings=make_settings())

    @dag.task(timeout=20)
    def sample(payload: dict[str, int]) -> dict[str, int]:
        return payload

    task = dag.registry.get("sample")
    assert task is not None
    assert task.timeout == 20


def test_dag_get_settings_lazy_load(monkeypatch):
    dag = Dag("demo")
    settings = make_settings()
    called = {}

    def fake_get_settings(force_reload: bool = False):
        called["count"] = called.get("count", 0) + 1
        return settings

    from orchex import config

    monkeypatch.setattr(config, "get_settings", fake_get_settings)

    assert dag.get_settings() is settings
    assert dag.get_settings() is settings  # cached
    assert called["count"] == 1
