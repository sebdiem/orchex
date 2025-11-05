from __future__ import annotations

import logging
import uuid
from typing import Any

from . import utils
from .config import Settings, get_settings
from .dag import Dag
from .db import connection, cursor
from .schema import render_schema_sql
from .worker import Worker

logger = logging.getLogger(__name__)


class OrchestratorService:
    def __init__(
        self,
        *,
        dag: Dag,
        settings: Settings | None = None,
    ) -> None:
        self.dag = dag
        self.registry = dag.registry
        self.settings = settings or dag.get_settings()
        self.schema = self.settings.db_schema
        self.dag_name = dag.name

    # ----- Schema & snapshots -------------------------------------------------
    @staticmethod
    def init_schema(settings: Settings | None = None) -> None:
        """Initialize the database schema without requiring a DAG instance."""
        settings = settings or get_settings()
        schema_sql = render_schema_sql(settings.db_schema)
        with connection(settings) as conn, cursor(conn) as cur:
            logger.info(
                "Ensuring orchestrator schema exists", extra={"action": "init_db"}
            )
            cur.execute(schema_sql)
            conn.commit()

    def init_db(self) -> None:
        """Initialize the database schema and ensure an active snapshot for this DAG."""
        self.init_schema(self.settings)
        self.ensure_active_snapshot()

    def ensure_active_snapshot(self) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"SELECT COUNT(*) AS n FROM {self.schema}.dag_snapshots WHERE dag_name=%s",
                (self.dag_name,),
            )
            has_snapshot = cur.fetchone()["n"] > 0
            dag_version = None
            if not has_snapshot:
                dag_version = self._create_snapshot(cur)
                self._set_current_dag_version(cur, dag_version)
            else:
                dag_version = self._get_current_dag_version(cur)
                if dag_version is None:
                    cur.execute(
                        f"""
                        SELECT dag_version
                        FROM {self.schema}.dag_snapshots
                        WHERE dag_name = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (self.dag_name,),
                    )
                    row = cur.fetchone()
                    if row:
                        dag_version = row["dag_version"]
                        self._set_current_dag_version(cur, dag_version)
            conn.commit()
        if dag_version:
            logger.info(
                "Active DAG snapshot ensured",
                extra={"action": "ensure_snapshot", "dag_version": str(dag_version)},
            )

    def create_snapshot(self, *, activate: bool = False) -> uuid.UUID:
        with connection(self.settings) as conn, cursor(conn) as cur:
            dag_version = self._create_snapshot(cur)
            if activate:
                self._set_current_dag_version(cur, dag_version)
            conn.commit()
        return dag_version

    def list_snapshots(self) -> list[dict[str, Any]]:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                SELECT dag_version, created_at,
                       dag_version = (
                         SELECT value::uuid FROM {self.schema}.settings WHERE key=%s
                       ) AS is_active
                FROM {self.schema}.dag_snapshots
                WHERE dag_name = %s
                ORDER BY created_at DESC
                """,
                (self._current_dag_settings_key(), self.dag_name),
            )
            return cur.fetchall()

    def _create_snapshot(self, cur) -> uuid.UUID:
        dag_version = uuid.uuid4()
        cur.execute(
            f"INSERT INTO {self.schema}.dag_snapshots(dag_version, dag_name) VALUES (%s, %s)",
            (dag_version, self.dag_name),
        )
        for task_name in self.registry.topsort():
            task = self.registry.tasks[task_name]
            cur.execute(
                f"""
                INSERT INTO {self.schema}.dag_snapshot_tasks(dag_version, task_name, requires)
                VALUES (%s, %s, %s)
                """,
                (dag_version, task_name, list(task.requires)),
            )
        logger.info(
            "Created DAG snapshot",
            extra={"action": "create_snapshot", "dag_version": str(dag_version)},
        )
        return dag_version

    def _get_current_dag_version(self, cur) -> uuid.UUID | None:
        cur.execute(
            f"SELECT value FROM {self.schema}.settings WHERE key=%s",
            (self._current_dag_settings_key(),),
        )
        row = cur.fetchone()
        return uuid.UUID(row["value"]) if row else None

    def _set_current_dag_version(self, cur, dag_version: uuid.UUID) -> None:
        cur.execute(
            f"""
            INSERT INTO {self.schema}.settings(key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (self._current_dag_settings_key(), str(dag_version)),
        )

    def _current_dag_settings_key(self) -> str:
        return f"current_dag_version:{self.dag_name}"

    # ----- Enqueue -----------------------------------------------------------
    def enqueue_run(
        self,
        *,
        run_id: uuid.UUID,
        initial_inputs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        dag_version: uuid.UUID | None = None,
    ) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            dag_version = dag_version or self._get_current_dag_version(cur)
            if dag_version is None:
                raise RuntimeError(
                    f"No active DAG snapshot for DAG '{self.dag_name}'. "
                    "Run `orchex snapshot --activate` first."
                )
            cur.execute(
                f"""
                INSERT INTO {self.schema}.runs(run_id, metadata)
                VALUES (%s, %s)
                ON CONFLICT (run_id) DO UPDATE SET metadata = EXCLUDED.metadata
                """,
                (run_id, utils.json_dumps(metadata or {})),
            )
            cur.execute(
                f"""
                SELECT task_name, requires
                FROM {self.schema}.dag_snapshot_tasks
                WHERE dag_version = %s
                """,
                (dag_version,),
            )
            tasks = cur.fetchall()
            root_tasks = {task["task_name"] for task in tasks if not task["requires"]}
            for task in tasks:
                payload = (
                    utils.json_dumps(initial_inputs)
                    if initial_inputs and task["task_name"] in root_tasks
                    else None
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.run_task_jobs(run_id, task_name, status, dag_version, initial_inputs)
                    VALUES (%s, %s, 'pending', %s, %s)
                    ON CONFLICT (run_id, task_name) DO NOTHING
                    """,
                    (run_id, task["task_name"], dag_version, payload),
                )
            conn.commit()
        logger.info(
            "Run enqueued",
            extra={
                "action": "enqueue_run",
                "run_id": str(run_id),
                "dag_version": str(dag_version),
            },
        )

    # ----- Operational helpers ----------------------------------------------
    def retry_dead(
        self,
        *,
        task_name: str | None = None,
        since: str | None = None,
    ) -> int:
        return self._retry_jobs(
            from_status="dead",
            task_name=task_name,
            since=since,
        )

    def retry_failed(
        self,
        *,
        task_name: str | None = None,
        since: str | None = None,
        min_attempts: int | None = None,
    ) -> int:
        return self._retry_jobs(
            from_status="failed",
            task_name=task_name,
            since=since,
            min_attempts=min_attempts,
        )

    def _retry_jobs(
        self,
        *,
        from_status: str,
        task_name: str | None,
        since: str | None,
        min_attempts: int | None = None,
    ) -> int:
        clauses = ["status = %s"]
        params: list[Any] = [from_status]
        if task_name:
            clauses.append("task_name = %s")
            params.append(task_name)
        if since:
            clauses.append("updated_at >= %s")
            params.append(since)
        if min_attempts is not None:
            clauses.append("attempts >= %s")
            params.append(min_attempts)
        where_sql = " AND ".join(clauses)
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET status='pending', attempts=0, updated_at=now(), last_error=NULL
                WHERE {where_sql}
                RETURNING 1
                """,
                tuple(params),
            )
            count = cur.rowcount or 0
            conn.commit()
            return count

    def queue_depth(self) -> list[dict[str, Any]]:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                SELECT status, COUNT(*) AS count
                FROM {self.schema}.run_task_jobs
                GROUP BY status
                ORDER BY status
                """
            )
            return cur.fetchall()

    def stale_locks(self) -> list[dict[str, Any]]:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                SELECT * FROM {self.schema}.job_locks
                WHERE lease_until < now()
                ORDER BY lease_until ASC
                """
            )
            return cur.fetchall()

    def top_failures(self) -> list[dict[str, Any]]:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                SELECT task_name, COUNT(*) AS failures
                FROM {self.schema}.run_task_jobs
                WHERE status='failed' AND updated_at > now() - interval '24 hours'
                GROUP BY task_name
                ORDER BY failures DESC
                LIMIT 20
                """
            )
            return cur.fetchall()

    # ----- Worker ------------------------------------------------------------
    def run_worker(
        self,
        *,
        concurrency: int = 1,
        poll_interval: float | None = None,
        worker_id: str | None = None,
    ) -> None:
        worker = Worker(
            registry=self.registry,
            settings=self.settings,
            worker_id=worker_id,
        )
        worker.run(
            concurrency=concurrency,
            poll_interval=poll_interval or self.settings.claim_poll_interval,
        )


__all__ = ["OrchestratorService"]
