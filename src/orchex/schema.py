from __future__ import annotations

from .naming import qualify, quote_ident


def render_schema_sql(schema: str) -> str:
    schema_ident = quote_ident(schema)
    settings = qualify(schema, "settings")
    run_task_jobs = qualify(schema, "run_task_jobs")
    job_locks = qualify(schema, "job_locks")
    task_results = qualify(schema, "task_results")
    dag_snapshots = qualify(schema, "dag_snapshots")
    dag_snapshot_tasks = qualify(schema, "dag_snapshot_tasks")
    runs = qualify(schema, "runs")

    def idx(name: str) -> str:
        return quote_ident(f"{schema}_{name}")

    idx_jobs_status = idx("idx_jobs_status")
    idx_jobs_status_updated = idx("idx_jobs_status_updated")
    idx_locks_lease = idx("idx_locks_lease")
    idx_snapshots_dag = idx("idx_snapshots_dag")

    return f"""
    CREATE SCHEMA IF NOT EXISTS {schema_ident};
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

    CREATE TABLE IF NOT EXISTS {settings} (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS {run_task_jobs} (
      run_id         UUID NOT NULL,
      task_name      TEXT NOT NULL,
      status         TEXT NOT NULL CHECK (status IN ('pending','processing','succeeded','failed','dead','skipped','blocked')),
      attempts       INT  NOT NULL DEFAULT 0,
      last_error     TEXT,
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      dag_version    UUID NOT NULL,
      initial_inputs JSON,
      PRIMARY KEY (run_id, task_name)
    );

    CREATE TABLE IF NOT EXISTS {job_locks} (
      run_id      UUID NOT NULL,
      task_name   TEXT NOT NULL,
      locked_by   TEXT NOT NULL,
      locked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      lease_until TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (run_id, task_name)
    );

    CREATE TABLE IF NOT EXISTS {task_results} (
      run_id      UUID NOT NULL,
      task_name   TEXT NOT NULL,
      result_json JSON NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (run_id, task_name)
    );

    CREATE TABLE IF NOT EXISTS {dag_snapshots} (
      dag_version UUID PRIMARY KEY,
      dag_name    TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    ALTER TABLE {dag_snapshots}
      ADD COLUMN IF NOT EXISTS dag_name TEXT;

    UPDATE {dag_snapshots}
      SET dag_name = COALESCE(dag_name, 'default')
      WHERE dag_name IS NULL;

    ALTER TABLE {dag_snapshots}
      ALTER COLUMN dag_name SET NOT NULL;

    CREATE TABLE IF NOT EXISTS {dag_snapshot_tasks} (
      dag_version UUID NOT NULL REFERENCES {dag_snapshots}(dag_version) ON DELETE CASCADE,
      task_name   TEXT NOT NULL,
      requires    TEXT[] NOT NULL DEFAULT '{{}}',
      PRIMARY KEY (dag_version, task_name)
    );

    CREATE TABLE IF NOT EXISTS {runs} (
      run_id      UUID PRIMARY KEY,
      metadata    JSON NOT NULL DEFAULT '{{}}',
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS {idx_jobs_status} ON {run_task_jobs} (task_name, status, updated_at);
    CREATE INDEX IF NOT EXISTS {idx_jobs_status_updated} ON {run_task_jobs} (status, updated_at);
    CREATE INDEX IF NOT EXISTS {idx_locks_lease} ON {job_locks} (lease_until);
    CREATE INDEX IF NOT EXISTS {idx_snapshots_dag} ON {dag_snapshots} (dag_name, created_at DESC);
    """


__all__ = ["render_schema_sql"]
