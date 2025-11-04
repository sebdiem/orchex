from __future__ import annotations

import pytest

from orchex.registry import TaskRegistry


def test_registers_tasks_and_topsort_respects_dependencies():
    registry = TaskRegistry()

    @registry.task()
    def extract(payload: dict[str, int]) -> dict[str, int]:
        return {"value": payload["value"] + 1}

    @registry.task(requires=["extract"])
    def load(payload: dict[str, int]) -> dict[str, int]:
        return payload

    order = registry.topsort()
    assert order.index("extract") < order.index("load")
    assert registry.roots() == ["extract"]
    assert registry.get("load").requires == frozenset({"extract"})


def test_register_duplicate_task_name_raises():
    registry = TaskRegistry()

    def noop(payload: dict[str, int]) -> dict[str, int]:
        return payload

    registry.register(noop, name="task")
    with pytest.raises(ValueError):
        registry.register(noop, name="task")


def test_task_decorator_supports_timeout():
    registry = TaskRegistry()

    @registry.task(timeout=45)
    def timed(payload: dict[str, int]) -> dict[str, int]:
        return payload

    task = registry.get("timed")
    assert task is not None
    assert task.timeout == 45
