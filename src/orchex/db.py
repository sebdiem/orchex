from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import Any, cast

from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import Settings, get_settings

logger = logging.getLogger(__name__)

_POOL: ConnectionPool | None = None


def get_pool(settings: Settings | None = None) -> ConnectionPool:
    settings = settings or get_settings()
    global _POOL
    if _POOL is None:
        logger.info(
            "Creating connection pool",
            extra={
                "action": "create_pool",
                "db_pool_min": settings.db_pool_min,
                "db_pool_max": settings.db_pool_max,
            },
        )
        _POOL = ConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            kwargs={"autocommit": False},
        )
    return _POOL


def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.close()
        _POOL = None


def _configure_connection(conn: Connection, settings: Settings) -> None:
    if getattr(cast(Any, conn), "_orchex_configured", False):
        return
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(f"{settings.statement_timeout_ms}ms")
            )
        )
    cast(Any, conn)._orchex_configured = True


@contextlib.contextmanager
def connection(settings: Settings | None = None) -> Iterator[Connection]:
    settings = settings or get_settings()
    pool = get_pool(settings)
    conn = pool.getconn()
    try:
        _configure_connection(conn, settings)
        yield conn
    finally:
        with contextlib.suppress(Exception):
            conn.rollback()
        pool.putconn(conn)


@contextlib.contextmanager
def cursor(conn: Connection):
    cur = conn.cursor(row_factory=dict_row)
    try:
        yield cur
    finally:
        cur.close()


__all__ = ["close_pool", "connection", "cursor", "get_pool"]
