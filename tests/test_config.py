from __future__ import annotations

import pytest

from orchex.config import Settings


def test_settings_from_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
    # Ensure defaults are exercised regardless of the runner's environment.
    for var in (
        "DB_SCHEMA",
        "TASK_TIMEOUT_SECONDS",
        "MAX_ATTEMPTS",
        "LEASE_SECONDS",
        "CLAIM_POLL_INTERVAL",
        "DB_POOL_MIN",
        "DB_POOL_MAX",
        "STATEMENT_TIMEOUT_MS",
        "METRICS_FLUSH_INTERVAL",
    ):
        monkeypatch.delenv(var, raising=False)

    settings = Settings.from_env()
    assert settings.db_schema == "orchex"
    assert settings.task_timeout_seconds == 120
    assert settings.claim_poll_interval == 0.5
    assert settings.db_pool_min == 1
    assert settings.statement_timeout_ms == 30000


def test_settings_from_env_validates_schema(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
    monkeypatch.setenv("DB_SCHEMA", "123-bad")
    with pytest.raises(ValueError):
        Settings.from_env()
