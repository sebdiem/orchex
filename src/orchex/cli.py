from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import uuid
from typing import Any

from .dag import Dag
from .service import OrchestratorService

logger = logging.getLogger(__name__)


def _import_from_path(path: str) -> Any:
    module_path, sep, attr_path = path.partition(":")
    if not sep:
        raise ValueError("Use module:attribute syntax to reference a Dag instance")
    module = importlib.import_module(module_path)
    obj = module
    for attr in attr_path.split("."):
        obj = getattr(obj, attr)
    return obj


def load_dag(target: str | None, *, default: str | None = None) -> Dag:
    actual = target or os.getenv("ORCHEX_DAG") or default
    if not actual:
        raise SystemExit(
            "Provide --dag module:object or set ORCHEX_DAG to point at a Dag instance"
        )
    obj = _import_from_path(actual)
    if not isinstance(obj, Dag):
        raise SystemExit(
            f"Object at {actual!r} is not an orchex.Dag (got {type(obj).__name__})"
        )
    return obj


def configure_logging(verbosity: int) -> None:
    level = logging.INFO if verbosity == 0 else logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _json_arg(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    return json.loads(raw)


def _service_for_args(
    args: argparse.Namespace, *, default_dag: str | None = None
) -> OrchestratorService:
    dag = load_dag(getattr(args, "dag_target", None), default=default_dag)
    return OrchestratorService(dag=dag)


def cmd_init_db(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    svc.init_db()
    print("Database initialized and snapshot ensured.")


def cmd_snapshot(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    if args.list:
        rows = svc.list_snapshots()
        for row in rows:
            marker = "*" if row["is_active"] else " "
            print(f"{marker} {row['dag_version']}  {row['created_at']}")
        return
    dag_version = svc.create_snapshot(activate=args.activate)
    print(f"Created snapshot {dag_version}")
    if args.activate:
        print("Activated snapshot.")


def cmd_worker(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    svc.run_worker(
        concurrency=args.concurrency,
        poll_interval=args.poll_interval,
        worker_id=args.worker_id,
    )


def cmd_enqueue(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    run_id = uuid.UUID(args.run_id) if args.run_id else uuid.uuid4()
    initial_inputs: dict[str, Any] = {}
    if args.uri:
        initial_inputs["uri"] = args.uri
    initial_inputs.update(_json_arg(args.inputs))
    metadata = _json_arg(args.metadata)
    svc.enqueue_run(
        run_id=run_id,
        initial_inputs=initial_inputs or None,
        metadata=metadata or None,
    )
    print(f"Enqueued run {run_id}")


def cmd_demo(args: argparse.Namespace) -> None:
    svc = _service_for_args(args, default_dag="orchex.demo:dag")
    svc.init_db()
    svc.create_snapshot(activate=True)
    for i in range(3):
        run_id = uuid.uuid4()
        svc.enqueue_run(
            run_id=run_id,
            initial_inputs={"uri": f"memory://{run_id}"},
            metadata={"demo_run": True, "index": i},
        )
    print("Starting demo worker. Press Ctrl+C to stop…")
    try:
        svc.run_worker(concurrency=2, poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        pass


def cmd_retry_dead(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    updated = svc.retry_dead(task_name=args.task, since=args.since)
    print(f"Requeued {updated} dead jobs.")


def cmd_retry_failed(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    updated = svc.retry_failed(
        task_name=args.task, since=args.since, min_attempts=args.min_attempts
    )
    print(f"Requeued {updated} failed jobs.")


def cmd_queue_depth(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    rows = svc.queue_depth()
    for row in rows:
        print(f"{row['status']:>10}: {row['count']}")


def cmd_locks(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    rows = svc.stale_locks()
    for row in rows:
        print(
            f"{row['run_id']} {row['task_name']} locked_by={row['locked_by']} lease_until={row['lease_until']}"
        )


def cmd_failures(args: argparse.Namespace) -> None:
    svc = _service_for_args(args)
    rows = svc.top_failures()
    for row in rows:
        print(f"{row['task_name']:<30} {row['failures']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Snapshot-based orchestrator with DAG registry."
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase log verbosity"
    )
    parser.add_argument(
        "--dag",
        dest="dag_target",
        default=None,
        help="Import path to a Dag instance (module:object). Defaults to ORCHEX_DAG env var.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("init-db", help="Initialize schema and ensure snapshot")
    sp.set_defaults(func=cmd_init_db)

    sp = sub.add_parser("snapshot", help="Create or inspect DAG snapshots")
    sp.add_argument("--activate", action="store_true", help="Activate after creation")
    sp.add_argument("--list", action="store_true", help="List snapshots")
    sp.set_defaults(func=cmd_snapshot)

    sp = sub.add_parser("worker", help="Run worker loop")
    sp.add_argument("--concurrency", type=int, default=1)
    sp.add_argument("--poll-interval", type=float, default=0.5)
    sp.add_argument("--worker-id", type=str, default=None)
    sp.set_defaults(func=cmd_worker)

    sp = sub.add_parser("enqueue", help="Enqueue a run")
    sp.add_argument("--run-id", type=str, default=None)
    sp.add_argument("--uri", type=str, default=None)
    sp.add_argument("--inputs", type=str, default=None, help="JSON payload for inputs")
    sp.add_argument("--metadata", type=str, default=None, help="JSON metadata")
    sp.set_defaults(func=cmd_enqueue)

    sp = sub.add_parser("demo", help="Run demo registry and worker")
    sp.add_argument("--poll-interval", type=float, default=0.5)
    sp.set_defaults(func=cmd_demo)

    sp = sub.add_parser("retry-dead", help="Reset dead jobs to pending")
    sp.add_argument("--task", type=str, default=None, help="Filter by task name")
    sp.add_argument("--since", type=str, default=None, help="ISO timestamp filter")
    sp.set_defaults(func=cmd_retry_dead)

    sp = sub.add_parser("retry-failed", help="Reset failed jobs to pending")
    sp.add_argument("--task", type=str, default=None)
    sp.add_argument("--since", type=str, default=None)
    sp.add_argument("--min-attempts", type=int, default=None)
    sp.set_defaults(func=cmd_retry_failed)

    sp = sub.add_parser("queue-depth", help="Show jobs grouped by status")
    sp.set_defaults(func=cmd_queue_depth)

    sp = sub.add_parser("stale-locks", help="Show expired locks")
    sp.set_defaults(func=cmd_locks)

    sp = sub.add_parser("failures", help="Show top failing tasks (24h)")
    sp.set_defaults(func=cmd_failures)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    args.func(args)


__all__ = ["build_parser", "main"]
