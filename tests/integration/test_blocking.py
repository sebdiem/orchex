from __future__ import annotations

import uuid

from orchex.config import Settings
from orchex.db import cursor
from orchex.registry import TaskRegistry
from orchex.worker import Worker


def make_worker(settings: Settings) -> Worker:
    """Helper to create a Worker instance for testing."""
    return Worker(
        registry=TaskRegistry(),
        settings=settings,
        worker_id="test-worker",
    )


def setup_dag_snapshot(
    conn, schema: str, tasks_with_deps: dict[str, list[str]]
) -> uuid.UUID:
    """
    Helper to create a DAG snapshot with specified task dependencies.

    Args:
        conn: Database connection
        schema: Database schema name
        tasks_with_deps: Dict mapping task_name -> list of dependencies

    Returns:
        The dag_version UUID
    """
    dag_version = uuid.uuid4()

    with cursor(conn) as cur:
        # Create DAG snapshot
        cur.execute(
            f"INSERT INTO {schema}.dag_snapshots(dag_version, dag_name) VALUES (%s, %s)",
            (dag_version, "test_dag"),
        )

        # Create tasks with dependencies
        for task_name, deps in tasks_with_deps.items():
            cur.execute(
                f"""
                INSERT INTO {schema}.dag_snapshot_tasks(dag_version, task_name, requires)
                VALUES (%s, %s, %s)
                """,
                (dag_version, task_name, deps),
            )

    return dag_version


def create_run_with_jobs(
    conn,
    schema: str,
    run_id: uuid.UUID,
    dag_version: uuid.UUID,
    task_statuses: dict[str, str],
) -> None:
    """
    Helper to create a run with jobs in specified statuses.

    Args:
        conn: Database connection
        schema: Database schema name
        run_id: Run UUID
        dag_version: DAG version UUID
        task_statuses: Dict mapping task_name -> status
    """
    with cursor(conn) as cur:
        # Create run
        cur.execute(
            f"INSERT INTO {schema}.runs(run_id) VALUES (%s)",
            (run_id,),
        )

        # Create jobs
        for task_name, status in task_statuses.items():
            cur.execute(
                f"""
                INSERT INTO {schema}.run_task_jobs(run_id, task_name, status, dag_version)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, task_name, status, dag_version),
            )


def test_block_dependents_simple_chain(db_transaction, db_settings):
    """
    Test blocking dependents in a simple chain: A -> B -> C
    When A dies, both B and C should be blocked.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG: A -> B -> C
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": ["task_a"],
            "task_c": ["task_b"],
        },
    )

    # Create run with all tasks pending
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "dead",
            "task_b": "pending",
            "task_c": "pending",
        },
    )
    conn.commit()

    # Block dependents of task_a
    worker._block_dependents(run_id, "task_a")

    # Verify both B and C are blocked
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT task_name, status, last_error
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s
            ORDER BY task_name
            """,
            (run_id,),
        )
        rows = {row["task_name"]: row for row in cur.fetchall()}

    assert rows["task_a"]["status"] == "dead"
    assert rows["task_b"]["status"] == "blocked"
    assert "blocked due to upstream dead task task_a" in rows["task_b"]["last_error"]
    assert rows["task_c"]["status"] == "blocked"
    assert "blocked due to upstream dead task task_a" in rows["task_c"]["last_error"]


def test_block_dependents_diamond_dag(db_transaction, db_settings):
    """
    Test blocking in a diamond DAG:
        A
       / \\
      B   C
       \\ /
        D
    When A dies, B, C, and D should all be blocked.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup diamond DAG
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": ["task_a"],
            "task_c": ["task_a"],
            "task_d": ["task_b", "task_c"],
        },
    )

    # Create run with all tasks pending
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "dead",
            "task_b": "pending",
            "task_c": "pending",
            "task_d": "pending",
        },
    )
    conn.commit()

    # Block dependents of task_a
    worker._block_dependents(run_id, "task_a")

    # Verify all downstream tasks are blocked
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT task_name, status
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s AND status = 'blocked'
            ORDER BY task_name
            """,
            (run_id,),
        )
        blocked_tasks = [row["task_name"] for row in cur.fetchall()]

    assert set(blocked_tasks) == {"task_b", "task_c", "task_d"}


def test_block_dependents_only_pending_and_failed(db_transaction, db_settings):
    """
    Test that only pending and failed tasks are blocked, not succeeded/processing ones.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG: A -> B, C, D, E
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": ["task_a"],
            "task_c": ["task_a"],
            "task_d": ["task_a"],
            "task_e": ["task_a"],
        },
    )

    # Create run with tasks in various statuses
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "dead",
            "task_b": "pending",
            "task_c": "failed",
            "task_d": "succeeded",
            "task_e": "processing",
        },
    )
    conn.commit()

    # Block dependents of task_a
    worker._block_dependents(run_id, "task_a")

    # Verify only pending and failed tasks are blocked
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT task_name, status
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s
            ORDER BY task_name
            """,
            (run_id,),
        )
        rows = {row["task_name"]: row["status"] for row in cur.fetchall()}

    assert rows["task_b"] == "blocked"  # was pending
    assert rows["task_c"] == "blocked"  # was failed
    assert rows["task_d"] == "succeeded"  # unchanged
    assert rows["task_e"] == "processing"  # unchanged


def test_maybe_unblock_dependents_all_deps_succeeded(db_transaction, db_settings):
    """
    Test unblocking when all dependencies are satisfied.
    DAG: A, B -> C
    When both A and B succeed, C should be unblocked.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": [],
            "task_c": ["task_a", "task_b"],
        },
    )

    # Create run with C blocked, A succeeded, B succeeded
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "succeeded",
            "task_b": "succeeded",
            "task_c": "blocked",
        },
    )
    conn.commit()

    # Try to unblock dependents of task_b (the last one to succeed)
    worker._maybe_unblock_dependents(run_id, "task_b")

    # Verify C is unblocked
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT status, last_error
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s AND task_name = 'task_c'
            """,
            (run_id,),
        )
        row = cur.fetchone()

    assert row["status"] == "pending"
    assert row["last_error"] is None


def test_maybe_unblock_dependents_partial_deps(db_transaction, db_settings):
    """
    Test that tasks remain blocked when not all dependencies are satisfied.
    DAG: A, B, C -> D
    When only A and B succeed but C is still failed, D should remain blocked.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": [],
            "task_c": [],
            "task_d": ["task_a", "task_b", "task_c"],
        },
    )

    # Create run with D blocked, A and B succeeded, C still failed
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "succeeded",
            "task_b": "succeeded",
            "task_c": "failed",
            "task_d": "blocked",
        },
    )
    conn.commit()

    # Try to unblock dependents of task_b
    worker._maybe_unblock_dependents(run_id, "task_b")

    # Verify D is still blocked (because C is failed)
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT status
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s AND task_name = 'task_d'
            """,
            (run_id,),
        )
        row = cur.fetchone()

    assert row["status"] == "blocked"


def test_maybe_unblock_dependents_skipped_counts_as_success(
    db_transaction, db_settings
):
    """
    Test that skipped dependencies are treated as satisfied.
    DAG: A, B -> C
    When A is skipped and B succeeds, C should be unblocked.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": [],
            "task_c": ["task_a", "task_b"],
        },
    )

    # Create run with C blocked, A skipped, B succeeded
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "skipped",
            "task_b": "succeeded",
            "task_c": "blocked",
        },
    )
    conn.commit()

    # Try to unblock dependents of task_b
    worker._maybe_unblock_dependents(run_id, "task_b")

    # Verify C is unblocked
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT status
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s AND task_name = 'task_c'
            """,
            (run_id,),
        )
        row = cur.fetchone()

    assert row["status"] == "pending"


def test_maybe_unblock_only_affects_blocked_tasks(db_transaction, db_settings):
    """
    Test that unblocking only changes tasks that are in 'blocked' status.
    """
    worker = make_worker(db_settings)
    conn = db_transaction
    run_id = uuid.uuid4()

    # Setup DAG: A -> B, C, D
    dag_version = setup_dag_snapshot(
        conn,
        db_settings.db_schema,
        {
            "task_a": [],
            "task_b": ["task_a"],
            "task_c": ["task_a"],
            "task_d": ["task_a"],
        },
    )

    # Create run with A succeeded, B blocked, C failed, D pending
    create_run_with_jobs(
        conn,
        db_settings.db_schema,
        run_id,
        dag_version,
        {
            "task_a": "succeeded",
            "task_b": "blocked",
            "task_c": "failed",
            "task_d": "pending",
        },
    )
    conn.commit()

    # Try to unblock dependents of task_a
    worker._maybe_unblock_dependents(run_id, "task_a")

    # Verify only blocked task is changed
    with cursor(conn) as cur:
        cur.execute(
            f"""
            SELECT task_name, status
            FROM {db_settings.db_schema}.run_task_jobs
            WHERE run_id = %s
            ORDER BY task_name
            """,
            (run_id,),
        )
        rows = {row["task_name"]: row["status"] for row in cur.fetchall()}

    assert rows["task_b"] == "pending"  # was blocked, now unblocked
    assert rows["task_c"] == "failed"  # unchanged
    assert rows["task_d"] == "pending"  # unchanged
