from __future__ import annotations

import os
import re
from dataclasses import dataclass

SchemaRe = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {raw!r})") from exc


def _parse_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float (got {raw!r})") from exc


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    db_schema: str
    task_timeout_seconds: int
    max_attempts: int
    lease_seconds: int
    claim_poll_interval: float
    db_pool_min: int
    db_pool_max: int
    statement_timeout_ms: int
    metrics_flush_interval: float

    @property
    def heartbeat_interval(self) -> float:
        return max(5.0, self.lease_seconds / 3.0)

    @classmethod
    def from_env(cls) -> Settings:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError(
                "Set DATABASE_URL env var like postgresql://user:pass@host:5432/db"
            )

        schema = os.getenv("DB_SCHEMA", "orchex")
        if not SchemaRe.match(schema):
            raise ValueError(
                "DB_SCHEMA must contain only letters, digits, and underscores, and start with a letter or underscore"
            )

        return cls(
            database_url=database_url,
            db_schema=schema,
            task_timeout_seconds=_parse_int("TASK_TIMEOUT_SECONDS", 120),
            max_attempts=_parse_int("MAX_ATTEMPTS", 10),
            lease_seconds=_parse_int("LEASE_SECONDS", 90),
            claim_poll_interval=_parse_float("CLAIM_POLL_INTERVAL", 0.5),
            db_pool_min=_parse_int("DB_POOL_MIN", 1),
            db_pool_max=_parse_int("DB_POOL_MAX", 10),
            statement_timeout_ms=_parse_int("STATEMENT_TIMEOUT_MS", 30000),
            metrics_flush_interval=_parse_float("METRICS_FLUSH_INTERVAL", 15.0),
        )


_SETTINGS: Settings | None = None


def get_settings(force_reload: bool = False) -> Settings:
    global _SETTINGS
    if force_reload or _SETTINGS is None:
        _SETTINGS = Settings.from_env()
    return _SETTINGS


__all__ = ["Settings", "get_settings"]
