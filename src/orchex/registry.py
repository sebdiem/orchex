from __future__ import annotations

import logging

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

TaskFn = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(slots=True)
class TaskDefinition:
    name: str
    fn: TaskFn
    requires: frozenset[str] = field(default_factory=frozenset)
    timeout: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "requires": sorted(self.requires),
            "timeout": self.timeout,
        }


class TaskRegistry:
    """
    Minimal Dagster-like registry used to declare callables as DAG tasks.
    """

    def __init__(self) -> None:
        self._tasks: dict[str, TaskDefinition] = {}

    def register(
        self,
        fn: TaskFn,
        *,
        name: str | None = None,
        requires: Iterable[str] | None = None,
        timeout: int | None = None,
    ) -> TaskFn:
        task_name = name or fn.__name__
        if task_name in self._tasks:
            raise ValueError(f"Task {task_name} already registered")
        requirement_set = frozenset(requires or [])
        self._tasks[task_name] = TaskDefinition(
            name=task_name,
            fn=fn,
            requires=requirement_set,
            timeout=timeout,
        )
        logger.debug(
            "Registered orchestrator task",
            extra={
                "action": "register_task",
                "task_name": task_name,
                "requires": sorted(requirement_set),
                "timeout": timeout,
            },
        )
        return fn

    def task(
        self,
        name: str | None = None,
        requires: Iterable[str] | None = None,
        timeout: int | None = None,
    ) -> Callable[[TaskFn], TaskFn]:
        def decorator(fn: TaskFn) -> TaskFn:
            return self.register(fn, name=name, requires=requires, timeout=timeout)

        return decorator

    @property
    def tasks(self) -> dict[str, TaskDefinition]:
        return self._tasks

    def get(self, name: str) -> TaskDefinition | None:
        return self._tasks.get(name)

    def roots(self) -> list[str]:
        return sorted(
            task_name for task_name, task in self._tasks.items() if not task.requires
        )

    def topsort(self) -> list[str]:
        """
        Return a stable topological ordering of the registered tasks.
        Raises ValueError when the graph is invalid.
        """

        incoming = {name: set(task.requires) for name, task in self._tasks.items()}
        order: list[str] = []
        ready = [name for name, deps in incoming.items() if not deps]
        while ready:
            node = ready.pop()
            order.append(node)
            for candidate, deps in incoming.items():
                if node in deps:
                    deps.remove(node)
                    if not deps:
                        ready.append(candidate)
        if any(deps for deps in incoming.values()):
            raise ValueError("Cycle detected in task graph")
        return order


REGISTRY = TaskRegistry()


def task(
    *,
    name: str | None = None,
    requires: Iterable[str] | None = None,
    timeout: int | None = None,
) -> Callable[[TaskFn], TaskFn]:
    return REGISTRY.task(name=name, requires=requires, timeout=timeout)


__all__ = [
    "TaskDefinition",
    "TaskFn",
    "TaskRegistry",
    "task",
    "REGISTRY",
]
