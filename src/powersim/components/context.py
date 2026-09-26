"""Canonical context passed to components during model construction."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable


@dataclass(frozen=True)
class BoundaryState:
    asset_id: str
    values: dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass
class ValidationIssue:
    asset_id: str
    code: str
    message: str
    severity: str = "error"
    compatibility: bool = False


@dataclass
class BuildContext:
    builder: Any
    periods: tuple[int, ...]
    duration_hours: float
    study_start: datetime | int | float | None = None
    profiles: dict[str, Any] = field(default_factory=dict)
    scenario_id: str | None = None
    initial_state: dict[str, BoundaryState] = field(default_factory=dict)
    capabilities: frozenset[str] = frozenset({"deterministic"})
    assets: dict[str, dict[str, Any]] = field(default_factory=dict)
    power_ports: list[Any] = field(default_factory=list)
    cost_terms: list[Any] = field(default_factory=list)
    validation_issues: list[ValidationIssue] = field(default_factory=list)
    offset_hours: float = 0.0

    def require_capabilities(self, supported: Iterable[str], asset_id: str) -> None:
        unsupported = self.capabilities.difference(supported)
        if unsupported:
            raise ValueError(f"component {asset_id!r} does not support capabilities: "
                             f"{', '.join(sorted(unsupported))}")

