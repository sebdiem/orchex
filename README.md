# Orchex

Postgres-backed mini Python orchestrator.

## Features

- **Code-defined DAGs** – declare tasks with `@dag.task`, wire dependencies via `requires`, and keep everything in pure Python.
- **Snapshot safety** – every run is bound to a DAG snapshot stored in Postgres, so code changes never mutate in-flight runs.
- **Postgres job ledger** – one table tracks `(run_id, task_name)` rows, job locks enforce single execution, and task results are persisted as JSON.
- **Resilient workers** – concurrency-limited threads claim ready jobs with `FOR UPDATE SKIP LOCKED`, renew leases, enforce per-task timeouts, and promote exhausted jobs to `dead`.

## Quickstart

Requirements: Python 3.12+, PostgreSQL 13+, `psycopg[pool]`.

```bash
uv add https://github.com/sebdiem/orchex.git
export DATABASE_URL="postgresql://user:pass@localhost:5432/orchex"
```

### 1. Define tasks

Create (or edit) a module and decorate your pure functions:

```python
# my_tasks.py
from orchex import Dag


dag = Dag("my_dag")


@dag.task(name="extract")
def extract(inputs: dict) -> dict:
    return {"text": f"Hello {inputs['uri']}"}


@dag.task(name="classify", requires=["extract"], timeout=5 * 60)
def classify(inputs: dict) -> dict:
    return {"topic": "demo" if "Hello" in inputs["extract"]["text"] else "other"}
```

Export the `dag` instance (as in the demo module) and point the CLI at it once via `--dag path.module:dag` or `ORCHEX_DAG=path.module:dag`. Importing the module initializes the registry, so users never worry about manual registration order. Use the optional `timeout` argument when a task needs a stricter limit than the global `TASK_TIMEOUT_SECONDS`; workers honor the per-task value automatically.

### 2. Initialize the database

```bash
export ORCHEX_DAG="my_project.tasks:dag"  # or pass --dag on each command
orchex init-db
orchex snapshot --activate
```

This creates the schema under `DB_SCHEMA` (default `orchex`), stores your DAG snapshot, and marks it as the active version for new runs.

### 3. Enqueue work

```bash
orchex enqueue --uri "memory://example" --metadata '{"customer_id": 42}'
```

You can pass arbitrary JSON via `--inputs` and `--metadata`. Root tasks automatically receive the initial input payload.

### 4. Run workers

```bash
orchex worker --concurrency 4
```

Workers:

- Poll Postgres for jobs whose dependencies have succeeded or been skipped.
- Acquire a lease row to guarantee exclusive execution.
- Renew the lease while running, respect per-task or global timeouts, and write results idempotently.
- Retry failed jobs with exponential backoff + jitter until `MAX_ATTEMPTS` pushes them to `dead`.

Press `Ctrl+C` for a graceful shutdown (no new claims, in-flight jobs finish before exit).

## CLI reference

| Command | Description |
| --- | --- |
| `init-db` | Create schema, ensure a snapshot exists, set active DAG if missing. |
| `snapshot [--activate] [--list]` | Persist current registry to Postgres, optionally activate or list versions. |
| `enqueue [--run-id UUID] [--uri ...] [--inputs JSON] [--metadata JSON]` | Fan out `(run_id, task)` rows for the active snapshot. |
| `worker [--concurrency N] [--poll-interval S] [--worker-id STR]` | Start a threaded worker that claims and executes jobs. |
| `demo` | Seed the demo DAG, enqueue three runs, and launch a 2-thread worker. |
| `retry-dead / retry-failed` | Reset matching jobs back to `pending` (filters: `--task`, `--since`, `--min-attempts`). |
| `queue-depth` | Show counts per status. |
| `stale-locks` | List expired locks (useful for spotting crashed workers). |
| `failures` | Top failing tasks over the last 24 hours. |

Run `orchex --help` for the full option list. Every command accepts `--dag module:object`; skipping it falls back to `ORCHEX_DAG`.

## DAGs in Orchex

- **One Dag object per workflow** – Instantiate `Dag("name")` once per logical graph. The name becomes the durable identifier stored with snapshots and used by workers/CLI commands.
- **Pure-python task graph** – Decorating functions with `@dag.task` registers them on that Dag. Dependencies are declared via `requires=["other_task"]`, so the graph lives entirely in code (no YAML/DSL).
- **Snapshots make DAGs immutable** – When you run `orchex snapshot`, every task + its dependencies are persisted under a generated `dag_version`. Runs always reference a specific snapshot, guaranteeing code churn doesn’t affect in-flight executions.
- **Multi-DAG friendly** – A single Postgres schema can store multiple DAGs. Each snapshot row records `dag_name`, and workers now filter claims/metrics by their Dag, so you can run isolated worker pools per workflow or share the same queue if you prefer.
- **Registry, service, worker flow** – `Dag.registry` feeds `OrchestratorService`, which writes snapshots and fan-outs jobs. `Worker` takes the same registry + dag name to execute tasks. This round-trip guarantees the worker only ever sees tasks that belong to its Dag.

## Configuration

Set environment variables (or use a `.env` file, the CLI loads it via `python-dotenv`):

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | (required) | Postgres DSN. |
| `DB_SCHEMA` | `orchex` | Schema used for all tables/indexes. |
| `TASK_TIMEOUT_SECONDS` | `120` | Hard wall time per task execution. |
| `MAX_ATTEMPTS` | `10` | Move jobs to `dead` after this many tries. |
| `LEASE_SECONDS` | `90` | Base lease duration; heartbeats renew every ~30s. |
| `CLAIM_POLL_INTERVAL` | `0.5` | Sleep between claim attempts when queue is empty. |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `1` / `10` | Connection pool sizing (psycopg_pool). |
| `STATEMENT_TIMEOUT_MS` | `30000` | Applied via `SET statement_timeout` per connection. |
| `METRICS_FLUSH_INTERVAL` | `15` | Interval for worker rate logs (claimed/succeeded/failed). |

## Job lifecycle

1. **pending** – fan-out inserted rows awaiting dependencies.
2. **processing** – worker claimed and set a lease.
3. **succeeded** – task produced output, result stored in `task_results`.
4. **failed** – exception occurred; backoff makes it reclaimable later.
5. **dead** – attempts exceeded `MAX_ATTEMPTS`. Use `retry-dead` to reset after fixes.
6. **skipped** – task missing from current code but present in the snapshot (logged so you can prune unused tasks safely).
7. **blocked** – task was never run because an upstream dependency is `dead`; once that dependency eventually succeeds or is skipped, the blocked task is reset to `pending` automatically.

## Operational queries

The CLI exposes common insights (`queue-depth`, `stale-locks`, `failures`).

## Contributing & Support

Found a bug or have ideas? Open an issue or pull request.
