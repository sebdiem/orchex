from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

# Ensure ``orchex`` can be imported without installing the package.
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(scope="session")
def db_initialized():
    """
    Session-scoped fixture that initializes the test database with migrations.
    Runs once per test session. Requires TEST_DATABASE_URL and DB_SCHEMA env vars.

    Example:
        TEST_DATABASE_URL=postgresql://localhost/orchex_test DB_SCHEMA=test_schema pytest
    """
    from orchex.config import get_settings
    from orchex.service import OrchestratorService

    os.environ["DB_SCHEMA"] = "test_orchex"
    settings = get_settings()
    OrchestratorService.init_schema(settings)

    yield settings

    # Cleanup: drop the test schema after all tests
    from orchex.db import close_pool, connection, cursor

    try:
        with connection(settings) as conn, cursor(conn) as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {settings.db_schema} CASCADE")
            conn.commit()
    finally:
        close_pool()


@pytest.fixture
def db_transaction(db_initialized):
    """
    Function-scoped fixture that wraps each test in a transaction with rollback.
    Provides test isolation by ensuring database changes are rolled back after each test.

    Usage:
        def test_something(db_transaction):
            conn = db_transaction
            with cursor(conn) as cur:
                cur.execute("INSERT INTO ...")
                # All changes will be rolled back after the test
    """
    from orchex.db import get_pool

    pool = get_pool(db_initialized)
    conn = pool.getconn()

    try:
        # Start a transaction for the test
        conn.execute("BEGIN")

        yield conn

        # Rollback all changes made during the test
        conn.rollback()
    finally:
        pool.putconn(conn)


@pytest.fixture
def db_settings(db_initialized):
    """
    Provides the test Settings instance configured for the test database.
    """
    return db_initialized
