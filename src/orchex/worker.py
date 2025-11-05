from __future__ import annotations

import contextlib
import logging
import signal
import threading
import time
import uuid
from typing import Any

from . import utils
from .config import Settings
from .db import connection, cursor
from .exceptions import TaskMissingError
from .registry import TaskDefinition, TaskRegistry

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        *,
        registry: TaskRegistry,
        settings: Settings,
        dag_name: str,
        worker_id: str | None = None,
    ) -> None:
        self.registry = registry
        self.settings = settings
        self.dag_name = dag_name
        self.worker_id = worker_id or f"worker-{uuid.uuid4()}"
        self.stop_event = threading.Event()
        self.schema = settings.db_schema
        self.metrics = utils.RateCounter(settings.metrics_flush_interval)

    def run(self, *, concurrency: int, poll_interval: float) -> None:
        logger.info(
            "Starting worker",
            extra={
                "action": "worker_start",
                "worker_id": self.worker_id,
                "concurrency": concurrency,
            },
        )
        threads = [
            threading.Thread(
                target=self._worker_loop,
                args=(poll_interval,),
                daemon=True,
                name=f"{self.worker_id}-{idx}",
            )
            for idx in range(concurrency)
        ]
        for t in threads:
            t.start()

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)

        try:
            while not self.stop_event.is_set():
                time.sleep(0.2)
                self.metrics.maybe_flush(
                    extra_factory=lambda: {
                        "worker_id": self.worker_id,
                        "queue_depth": self._queue_depth_snapshot(),
                    }
                )
        finally:
            self.stop_event.set()
            for t in threads:
                t.join()
            logger.info(
                "Worker stopped",
                extra={"action": "worker_stop", "worker_id": self.worker_id},
            )

    def _handle_signal(self, signum, frame):  # type: ignore[override]
        logger.info(
            "Signal received, stopping worker",
            extra={"action": "worker_signal", "signal": signum},
        )
        self.stop_event.set()

    # ----- loops -------------------------------------------------------------
    def _worker_loop(self, poll_interval: float) -> None:
        while not self.stop_event.is_set():
            job = self._claim_ready_job()
            if not job:
                time.sleep(poll_interval)
                continue
            run_id, task_name = job
            self.metrics.tick("claimed")
            start = time.monotonic()
            try:
                self._process_job(run_id, task_name)
                self.metrics.tick("succeeded")
                dur = time.monotonic() - start
                logger.info(
                    "Job succeeded",
                    extra={
                        "action": "job_success",
                        "worker_id": self.worker_id,
                        "run_id": str(run_id),
                        "task_name": task_name,
                        "duration_s": round(dur, 3),
                    },
                )
            except Exception as exc:
                self.metrics.tick("failed")
                dur = time.monotonic() - start
                logger.exception(
                    "Job failed",
                    extra={
                        "action": "job_failure",
                        "worker_id": self.worker_id,
                        "run_id": str(run_id),
                        "task_name": task_name,
                        "duration_s": round(dur, 3),
                        "error": str(exc),
                    },
                )

    # ----- claim -------------------------------------------------------------
    def _claim_ready_job(self) -> tuple[uuid.UUID, str] | None:
        lease_seconds = self.settings.lease_seconds
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                WITH ready AS (
                  SELECT j.run_id, j.task_name
                  FROM {self.schema}.run_task_jobs j
                  JOIN {self.schema}.dag_snapshots s
                    ON s.dag_version = j.dag_version
                  WHERE (
                    j.status = 'pending'
                    OR (
                      j.status = 'failed'
                      AND j.updated_at < now() - make_interval(
                        secs => LEAST(
                          (power(2, LEAST(j.attempts, 8))::int + (random() * 5)::int),
                          600
                        )
                      )
                    )
                  )
                    AND s.dag_name = %s
                    AND NOT EXISTS (
                      SELECT 1 FROM {self.schema}.job_locks l
                      WHERE l.run_id = j.run_id
                        AND l.task_name = j.task_name
                        AND l.lease_until > now()
                    )
                    AND NOT EXISTS (
                      SELECT 1
                      FROM {self.schema}.dag_snapshot_tasks t, LATERAL unnest(t.requires) AS r(dep)
                      WHERE t.dag_version = j.dag_version
                        AND t.task_name = j.task_name
                        AND EXISTS (
                          SELECT 1 FROM {self.schema}.run_task_jobs pj
                          WHERE pj.run_id = j.run_id
                            AND pj.task_name = r.dep
                            AND pj.status NOT IN ('succeeded','skipped')
                        )
                    )
                  ORDER BY j.updated_at ASC
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED
                )
                INSERT INTO {self.schema}.job_locks(run_id, task_name, locked_by, lease_until)
                SELECT run_id, task_name, %s, now() + (%s || ' seconds')::interval FROM ready
                ON CONFLICT (run_id, task_name) DO UPDATE
                  SET locked_by = EXCLUDED.locked_by,
                      locked_at = now(),
                      lease_until = EXCLUDED.lease_until
                RETURNING run_id, task_name
                """,
                (self.dag_name, self.worker_id, str(lease_seconds)),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            run_id, task_name = row["run_id"], row["task_name"]
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET status='processing', updated_at=now()
                WHERE run_id=%s AND task_name=%s
                """,
                (run_id, task_name),
            )
            conn.commit()
            logger.info(
                "Claimed job",
                extra={
                    "action": "job_claimed",
                    "worker_id": self.worker_id,
                    "run_id": str(run_id),
                    "task_name": task_name,
                },
            )
            return run_id, task_name

    # ----- processing --------------------------------------------------------
    def _process_job(self, run_id: uuid.UUID, task_name: str) -> None:
        task = self.registry.get(task_name)
        if task is None:
            logger.warning(
                "Task missing in registry; marking as succeeded",
                extra={"action": "task_missing", "task_name": task_name},
            )
            self._write_noop_result(run_id, task_name)
            return

        with connection(self.settings) as conn:
            alive = threading.Event()
            alive.set()

            def heartbeater():
                interval = max(1.0, self.settings.heartbeat_interval)
                while not alive.wait(interval):
                    try:
                        self._extend_lease(run_id, task_name)
                    except Exception as exc:  # pragma: no cover - best effort logging
                        logger.error(
                            "Lease heartbeat failed",
                            extra={
                                "action": "heartbeat_failed",
                                "worker_id": self.worker_id,
                                "run_id": str(run_id),
                                "task_name": task_name,
                                "error": str(exc),
                            },
                        )
                        break

            thread = threading.Thread(target=heartbeater, daemon=True)
            thread.start()
            try:
                inputs = self._load_inputs(conn, run_id, task_name, task)
                timeout = self._task_timeout(task)
                result = utils.run_with_timeout(lambda: task.fn(inputs), timeout)
                self._record_success(conn, run_id, task_name, result)
            except Exception as exc:
                conn.rollback()
                self._mark_failure(conn, run_id, task_name, exc)
                raise
            finally:
                alive.clear()
                thread.join(timeout=1)
                with contextlib.suppress(Exception):
                    self._release_lock(run_id, task_name)

    def _write_noop_result(self, run_id: uuid.UUID, task_name: str) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.task_results(run_id, task_name, result_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, task_name) DO UPDATE SET result_json = EXCLUDED.result_json
                """,
                (run_id, task_name, utils.json_dumps({"noop": True})),
            )
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET status='skipped', attempts=attempts+1, updated_at=now(), last_error='task missing in registry'
                WHERE run_id=%s AND task_name=%s
                """,
                (run_id, task_name),
            )
            conn.commit()
        logger.warning(
            "Task missing in registry; marked as skipped",
            extra={
                "action": "task_skipped",
                "run_id": str(run_id),
                "task_name": task_name,
            },
        )

    def _mark_failure(
        self, conn, run_id: uuid.UUID, task_name: str, exc: Exception
    ) -> None:
        with cursor(conn) as cur:
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET attempts = attempts + 1,
                    status = CASE
                        WHEN attempts + 1 >= %s THEN 'dead'
                        ELSE 'failed'
                    END,
                    updated_at = now(),
                    last_error = %s
                WHERE run_id=%s AND task_name=%s
                RETURNING status, attempts
                """,
                (self.settings.max_attempts, str(exc), run_id, task_name),
            )
            row = cur.fetchone()
            status = row["status"] if row else "failed"
            attempts = row["attempts"] if row else None
        conn.commit()
        if status == "dead":
            self._block_dependents(run_id, task_name)
        logger.warning(
            "Marked job failure",
            extra={
                "action": "job_mark_failure",
                "run_id": str(run_id),
                "task_name": task_name,
                "status": status,
                "attempts": attempts,
                "error": str(exc),
            },
        )

    def _load_inputs(
        self, conn, run_id: uuid.UUID, task_name: str, task
    ) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        with cursor(conn) as cur:
            for dep in task.requires:
                cur.execute(
                    f"""
                    SELECT result_json FROM {self.schema}.task_results
                    WHERE run_id=%s AND task_name=%s
                    """,
                    (run_id, dep),
                )
                row = cur.fetchone()
                if not row:
                    raise TaskMissingError(
                        f"Missing result for dependency {dep} of {task_name}"
                    )
                inputs.setdefault(dep, utils.json_loads(row["result_json"]))
            cur.execute(
                f"""
                SELECT initial_inputs FROM {self.schema}.run_task_jobs
                WHERE run_id=%s AND task_name=%s
                """,
                (run_id, task_name),
            )
            row = cur.fetchone()
            if row and row["initial_inputs"]:
                initial_inputs = utils.json_loads(row["initial_inputs"])
                if isinstance(initial_inputs, dict):
                    inputs.update(initial_inputs)
        return inputs

    # ----- locks -------------------------------------------------------------
    def _extend_lease(self, run_id: uuid.UUID, task_name: str) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                UPDATE {self.schema}.job_locks
                SET lease_until = now() + (%s || ' seconds')::interval
                WHERE run_id=%s AND task_name=%s
                """,
                (str(self.settings.lease_seconds), run_id, task_name),
            )
            conn.commit()

    def _release_lock(self, run_id: uuid.UUID, task_name: str) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                DELETE FROM {self.schema}.job_locks
                WHERE run_id=%s AND task_name=%s
                """,
                (run_id, task_name),
            )
            conn.commit()

    def _queue_depth_snapshot(self) -> dict[str, int]:
        with connection(self.settings) as conn, cursor(conn) as cur:
            cur.execute(
                f"""
                SELECT status, COUNT(*) AS count
                FROM {self.schema}.run_task_jobs j
                JOIN {self.schema}.dag_snapshots s
                  ON s.dag_version = j.dag_version
                WHERE s.dag_name = %s
                GROUP BY status
                """,
                (self.dag_name,),
            )
            rows = cur.fetchall()
        return {row["status"]: row["count"] for row in rows}

    def _task_timeout(self, task: TaskDefinition) -> int:
        if task.timeout is not None:
            return task.timeout
        return self.settings.task_timeout_seconds

    def _record_success(
        self, conn, run_id: uuid.UUID, task_name: str, result: dict[str, Any]
    ) -> None:
        with cursor(conn) as cur:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.task_results(run_id, task_name, result_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (run_id, task_name) DO UPDATE SET result_json = EXCLUDED.result_json
                """,
                (run_id, task_name, utils.json_dumps(result)),
            )
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET status='succeeded', attempts=attempts+1, updated_at=now(), last_error=NULL
                WHERE run_id=%s AND task_name=%s
                """,
                (run_id, task_name),
            )
        conn.commit()
        self._maybe_unblock_dependents(run_id, task_name)

    def _block_dependents(self, run_id: uuid.UUID, dead_task: str) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            dag_version = self._dag_version_for(cur, run_id, dead_task)
            if dag_version is None:
                return
            blocked = self._collect_descendants(cur, dag_version, [dead_task])
            if not blocked:
                return
            message = f"blocked due to upstream dead task {dead_task}"
            cur.execute(
                f"""
                UPDATE {self.schema}.run_task_jobs
                SET status='blocked', updated_at=now(), last_error=%s
                WHERE run_id=%s AND task_name = ANY(%s)
                  AND status IN ('pending','failed')
                """,
                (message, run_id, list(blocked)),
            )
            conn.commit()

    def _maybe_unblock_dependents(self, run_id: uuid.UUID, task_name: str) -> None:
        with connection(self.settings) as conn, cursor(conn) as cur:
            dag_version = self._dag_version_for(cur, run_id, task_name)
            if dag_version is None:
                return
            candidates = self._collect_descendants(cur, dag_version, [task_name])
            if not candidates:
                return
            for candidate in candidates:
                cur.execute(
                    f"""
                    SELECT requires
                    FROM {self.schema}.dag_snapshot_tasks
                    WHERE dag_version=%s AND task_name=%s
                    """,
                    (dag_version, candidate),
                )
                row = cur.fetchone()
                if not row:
                    continue
                requires = row["requires"] or []
                if not requires:
                    continue
                cur.execute(
                    f"""
                    SELECT task_name, status
                    FROM {self.schema}.run_task_jobs
                    WHERE run_id=%s AND task_name = ANY(%s)
                    """,
                    (run_id, requires),
                )
                statuses = {dep["task_name"]: dep["status"] for dep in cur.fetchall()}
                if any(
                    statuses.get(dep) not in ("succeeded", "skipped")
                    for dep in requires
                ):
                    continue
                cur.execute(
                    f"""
                    UPDATE {self.schema}.run_task_jobs
                    SET status='pending', updated_at=now(), last_error=NULL
                    WHERE run_id=%s AND task_name=%s AND status='blocked'
                    """,
                    (run_id, candidate),
                )
            conn.commit()

    def _dag_version_for(
        self, cur, run_id: uuid.UUID, task_name: str
    ) -> uuid.UUID | None:
        cur.execute(
            f"""
            SELECT dag_version
            FROM {self.schema}.run_task_jobs
            WHERE run_id=%s AND task_name=%s
            """,
            (run_id, task_name),
        )
        row = cur.fetchone()
        return row["dag_version"] if row else None

    def _collect_descendants(
        self, cur, dag_version: uuid.UUID, roots: list[str]
    ) -> set[str]:
        to_visit = list(roots)
        descendants: set[str] = set()
        while to_visit:
            current = to_visit.pop()
            cur.execute(
                f"""
                SELECT task_name
                FROM {self.schema}.dag_snapshot_tasks
                WHERE dag_version=%s AND %s = ANY(requires)
                """,
                (dag_version, current),
            )
            for child in cur.fetchall():
                name = child["task_name"]
                if name not in descendants:
                    descendants.add(name)
                    to_visit.append(name)
        return descendants


__all__ = ["Worker"]
