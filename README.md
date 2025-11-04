# Orchex

## Features

- **Code-defined DAGs** – declare tasks with `@orchestrator.task`, wire dependencies
 via `requires`, and keep everything in pure Python.
- **Snapshot safety** – every run is bound to a DAG snapshot stored in Postgres, so
code changes never mutate in-flight runs.
- **Postgres job ledger** – one table tracks `(run_id, task_name)` rows, job locks e
nforce single execution, and task results are persisted as JSON.
- **Resilient workers** – concurrency-limited threads claim ready jobs with `FOR UPD
ATE SKIP LOCKED`, renew leases, enforce per-task timeouts, and promote exhausted job
s to `dead`.
- **Operational CLI** – `init-db`, `snapshot`, `enqueue`, `worker`, `queue-depth`, `
stale-locks`, `retry-dead`, and more in a single script.
- **Transparent statuses** – `pending`, `processing`, `succeeded`, `failed`, `dead`,
 and `skipped` (for missing tasks) give you instant insight into every run.

## Quickstart

Requirements: Python 3.11+, PostgreSQL 13+, `psycopg[binary,pool]`.

```bash
uv add https://github.com/sebdiem/orchex.git
export DATABASE_URL="postgresql://user:pass@localhost:5432/orchex"
```

### 1. Define tasks

Create (or edit) a module and decorate your pure functions:

```python
# my_tasks.py
from orchex import task

@task(name="extract")
def extract(inputs: dict) -> dict:
    return {"text": f"Hello {inputs['uri']}"}

@task(name="classify", requires=["extract"], timeout=5*60)
def classify(inputs: dict) -> dict:
    return {"topic": "demo" if "Hello" in inputs["extract"]["text"] else "other"}
```

Import your module somewhere during startup so task registration happens before the
CLI runs (the demo tasks under `orchex/demo.py` show the pattern).

### 2. Initialize the database

```bash
orchex init-db
orchex snapshot --activate
```

This creates the schema under `DB_SCHEMA` (default `orchestrator`), stores your DAG
snapshot, and marks it as the active version for new runs.

### 3. Enqueue work

```bash
orchex enqueue --uri "memory://example" --metadata '{"customer_id":
42}'
```

You can pass arbitrary JSON via `--inputs` and `--metadata`. Root tasks automaticall
y receive the initial input payload.

### 4. Run workers

```bash
orchex worker --concurrency 4
```

Workers:

- Poll Postgres for jobs whose dependencies have succeeded or been skipped.
- Acquire a lease row to guarantee exclusive execution.
- Renew the lease while running, respect `TASK_TIMEOUT_SECONDS`, and write results i
dempotently.
- Retry failed jobs with exponential backoff + jitter until `MAX_ATTEMPTS` pushes th
em to `dead`.

Press `Ctrl+C` for a graceful shutdown (no new claims, in-flight jobs finish before
exit).

## CLI reference

| Command | Description |
| --- | --- |
| `init-db` | Create schema, ensure a snapshot exists, set active DAG if missing. |
| `snapshot [--activate] [--list]` | Persist current registry to Postgres, optionall
y activate or list versions. |
| `enqueue [--run-id UUID] [--uri ...] [--inputs JSON] [--metadata JSON]` | Fan out
`(run_id, task)` rows for the active snapshot. |
| `worker [--concurrency N] [--poll-interval S] [--worker-id STR]` | Start a threade
d worker that claims and executes jobs. |
| `demo` | Seed the demo DAG, enqueue three runs, and launch a 2-thread worker. |
| `retry-dead / retry-failed` | Reset matching jobs back to `pending` (filters: `--t
ask`, `--since`, `--min-attempts`). |
| `queue-depth` | Show counts per status. |
| `stale-locks` | List expired locks (useful for spotting crashed workers). |
| `failures` | Top failing tasks over the last 24 hours. |

Run `orchex --help` for the full option list.

## Configuration

es).
Set environment variables (or use a `.env` file, the CLI loads it via `python-dotenv
`):

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | (required) | Postgres DSN. |
| `DB_SCHEMA` | `orchestrator` | Schema used for all tables/indexes. |
| `TASK_TIMEOUT_SECONDS` | `120` | Hard wall time per task execution. |
| `MAX_ATTEMPTS` | `10` | Move jobs to `dead` after this many tries. |
| `LEASE_SECONDS` | `90` | Base lease duration; heartbeats renew every ~30s. |
| `CLAIM_POLL_INTERVAL` | `0.5` | Sleep between claim attempts when queue is empty.
|
| `DB_POOL_MIN` / `DB_POOL_MAX` | `1` / `10` | Connection pool sizing (psycopg_pool)
. |
| `STATEMENT_TIMEOUT_MS` | `30000` | Applied via `SET statement_timeout` per connect
ion. |
| `METRICS_FLUSH_INTERVAL` | `15` | Interval for worker rate logs (claimed/succeeded
/failed). |

## DAG snapshots & registry

- Use `@orchestrator.task(name="foo", requires=["bar", "baz"])` to declare nodes.
- The registry enforces deterministic ordering and validates for cycles.
- Running `snapshot --activate` captures the entire registry into `dag_snapshot_task
s`.
- Enqueued runs reference a specific `dag_version`, so future code changes are isola
ted.
- If a task disappears from the registry, workers mark it as `skipped` (with a warni
ng) so legacy runs still complete.

## Job lifecycle

1. **pending** – fan-out inserted rows awaiting dependencies.
2. **processing** – worker claimed and set a lease.
3. **succeeded** – task produced output, result stored in `task_results`.
4. **failed** – exception occurred; backoff makes it reclaimable later.
5. **dead** – attempts exceeded `MAX_ATTEMPTS`. Use `retry-dead` to reset after fixe
s.
6. **skipped** – task missing from current code but present in the snapshot (logged
ith jitter, keeping pressure off downstream systems.

## Operational queries

The CLI exposes common insights (`queue-depth`, `stale-locks`, `failures`). You can
also run the SQL statements in `spec_orchestrator_prod_ready.md` for dashboards or a
lerting.

## Developing your own tasks

1. Create a module, import `orchestrator.task`, and define pure callables that accep
t/return dictionaries.
2. Import that module from `orchestrator/__init__.py` (or any startup path) to regis
ter tasks before `snapshot`/`worker` commands run.
3. Keep task functions idempotent—workers may retry, and you should be able to repro
cess safely.

For a full example, read `orchestrator/demo.py` and run `orchex demo
`.

## Contributing & Support

- Found a bug or have ideas? Open an issue or pull request.
- See `spec_orchestrator_prod_ready.md` for the roadmap and hardening checklist.
- Please follow the existing coding style (type hints, logging extras, ASCII files)
         when contributing.
