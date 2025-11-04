from __future__ import annotations

from collections.abc import Callable, Iterable

from . import config as config_module
from .config import Settings
from .registry import TaskFn, TaskRegistry


class Dag:
    """
    Thin wrapper around a TaskRegistry with a stable name and optional settings.
    """

    def __init__(
        self,
        name: str,
        *,
        settings: Settings | None = None,
        registry: TaskRegistry | None = None,
    ) -> None:
        if not name:
            raise ValueError("Dag name must be a non-empty string")
        self.name = name
        self._settings = settings
        self.registry = registry or TaskRegistry()

    def task(
        self,
        *,
        name: str | None = None,
        requires: Iterable[str] | None = None,
        timeout: int | None = None,
    ) -> Callable[[TaskFn], TaskFn]:
        return self.registry.task(name=name, requires=requires, timeout=timeout)

    def get_settings(self) -> Settings:
        if self._settings is None:
            self._settings = config_module.get_settings()
        return self._settings


__all__ = ["Dag"]
