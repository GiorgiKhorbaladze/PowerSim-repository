"""Backend-independent component result and objective records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class CostTerm:
    """One objective contribution, expressed in dollars per period."""

    asset_id: str
    name: str
    unit: str = "USD"
    expression: Any = None
    evaluate: Callable[[], float] | None = field(default=None, compare=False, repr=False)

    def value(self) -> float:
        value = self.evaluate() if self.evaluate is not None else self.expression
        try:
            import pyomo.environ as pyo
            return float(pyo.value(value))
        except ImportError:
            return float(value)


@dataclass(frozen=True)
class ComponentResult:
    """Canonical, full-precision physical result for one asset and period."""

    asset_id: str
    component_kind: str
    period: int
    unit: str = "MW"
    available_mw: float | None = None
    injection_mw: float = 0.0
    withdrawal_mw: float = 0.0
    curtailed_mw: float | None = None
    curtailed_mwh: float | None = None
    cost_usd: float | None = None


@dataclass(frozen=True)
class ComponentQAMetadata:
    kind: str
    checks: tuple[str, ...]
    convention: str = "positive injection supplies the electricity balance"
