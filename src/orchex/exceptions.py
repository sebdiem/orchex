from __future__ import annotations


class OrchestratorError(Exception):
    """Base class for orchestrator specific errors."""


class ConfigurationError(OrchestratorError):
    """Raised when runtime configuration is invalid."""


class TaskTimeoutError(OrchestratorError):
    """Raised when a task execution exceeds its allotted time."""


class TaskMissingError(OrchestratorError):
    """Raised when a DAG snapshot references a task that is no longer registered."""


__all__ = [
    "ConfigurationError",
    "OrchestratorError",
    "TaskMissingError",
    "TaskTimeoutError",
]
