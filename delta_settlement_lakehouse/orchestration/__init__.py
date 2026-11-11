"""Orchestration package supporting Dagster assets and framework-agnostic Medallion tasks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from delta_settlement_lakehouse.orchestration.definitions import (
        OrchestrationRunResult,
        build_definitions,
        defs,
        run_orchestrated_pipeline,
    )

__all__ = [
    "OrchestrationRunResult",
    "build_definitions",
    "defs",
    "run_orchestrated_pipeline",
]


def __getattr__(name: str) -> Any:
    """Lazy-load Dagster definitions so shared task primitives import cleanly without Dagster."""
    if name in __all__:
        from delta_settlement_lakehouse.orchestration import definitions

        return getattr(definitions, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
