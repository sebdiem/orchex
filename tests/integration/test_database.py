from __future__ import annotations

import uuid

from orchex import utils
from orchex.db import cursor


def test_insert_and_query_run(db_transaction, db_settings):
    """Test that we can insert and query a run with transaction rollback."""
    conn = db_transaction
    run_id = uuid.uuid4()
    metadata = {"test": "value", "number": 42}

    with cursor(conn) as cur:
        # Insert a run (metadata must be JSON-serialized)
        cur.execute(
            f"""
            INSERT INTO {db_settings.db_schema}.runs(run_id, metadata)
            VALUES (%s, %s)
            """,
            (run_id, utils.json_dumps(metadata)),
        )

        # Query it back
        cur.execute(
            f"""
            SELECT run_id, metadata
            FROM {db_settings.db_schema}.runs
            WHERE run_id = %s
            """,
            (run_id,),
        )
        row = cur.fetchone()

    assert row is not None
    assert row["run_id"] == run_id
    # Metadata comes back as a dict from psycopg's JSON handling
    assert row["metadata"] == metadata


def test_insert_job_and_verify_status(db_transaction, db_settings):
    """Test inserting a job and verifying its status."""
    conn = db_transaction
    run_id = uuid.uuid4()
    task_name = "test_task"
    dag_version = uuid.uuid4()

    with cursor(conn) as cur:
        # Insert a job
        cur.execute(
            f"""
            INSERT INTO {db_settings.db_schema}.run_task_jobs(
                run_id, task_name, status, dag_version
            )
            VALUES (%s, %s, 'pending', %s)
            """,
            (run_id, task_name, dag_version),
        )

        # Query it back
        cur.execute(
            f"""
            SELECT run_id, task_name, status, attempts
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s AND task_name = %s
            """,
            (run_id, task_name),
        )
        row = cur.fetchone()

    assert row is not None
    assert row["run_id"] == run_id
    assert row["task_name"] == task_name
    assert row["status"] == "pending"
    assert row["attempts"] == 0


def test_transaction_isolation(db_transaction, db_settings):
    """Test that changes are isolated and rolled back between tests."""
    conn = db_transaction

    with cursor(conn) as cur:
        # Count existing runs (should be 0 due to rollback from previous tests)
        cur.execute(f"SELECT COUNT(*) as count FROM {db_settings.db_schema}.runs")
        row = cur.fetchone()
        # If previous tests were properly isolated, count should be 0
        assert row["count"] == 0

        # Insert a run
        run_id = uuid.uuid4()
        cur.execute(
            f"INSERT INTO {db_settings.db_schema}.runs(run_id) VALUES (%s)",
            (run_id,),
        )

        # Verify it exists
        cur.execute(f"SELECT COUNT(*) as count FROM {db_settings.db_schema}.runs")
        row = cur.fetchone()
        assert row["count"] == 1
