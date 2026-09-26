"""Canonical context passed to components during model construction."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

from powersim.contracts import IssueSeverity, ValidationIssue


@dataclass(frozen=True)
class BoundaryState:
    asset_id: str
    values: dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


def make_validation_issue(
    asset_id: str,
    code: str,
    message: str,
    *,
    field_name: str | None = None,
    severity: IssueSeverity = IssueSeverity.ERROR,
    compatibility: bool = False,
) -> ValidationIssue:
    """Build the canonical Stage-1 ValidationIssue for a component field."""
    path = f"assets.{asset_id}"
    if field_name:
        path += f".{field_name}"
    return ValidationIssue(
        code=code,
        severity=severity,
        path=path,
        message=message,
        context={"asset_id": asset_id, "compatibility": compatibility},
    )


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
    compatibility_mode: bool = True

    def require_capabilities(self, supported: Iterable[str], asset_id: str) -> None:
        unsupported = self.capabilities.difference(supported)
        if unsupported:
            raise ValueError(
                f"component {asset_id!r} does not support capabilities: "
                f"{', '.join(sorted(unsupported))}"
            )

    def period_coordinate(self, index: int) -> int:
        """Return the stable global period coordinate for a local window index."""
        if self.duration_hours <= 0:
            raise ValueError("duration_hours must be positive")
        return int(round(float(self.offset_hours) / self.duration_hours)) + int(index)
