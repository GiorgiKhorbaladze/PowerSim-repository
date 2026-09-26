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
        return float(self.evaluate() if self.evaluate is not None else self.expression)


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
    # UC fields are intentionally optional so passive components retain the
    # compact Stage-3A record shape.  Thermal components populate these from
    # canonical solver extraction; QA combines them with resolved inputs.
    commitment: float | None = None
    startup: float | None = None
    shutdown: float | None = None
    startup_hot: float | None = None
    pmin_mw: float | None = None
    pmax_mw: float | None = None
    variable_cost_usd: float | None = None
    startup_cost_usd: float | None = None
    no_load_cost_usd: float | None = None
    co2_t: float | None = None
    co2_cost_usd: float | None = None


@dataclass(frozen=True)
class ComponentQAMetadata:
    kind: str
    checks: tuple[str, ...]
    convention: str = "positive injection supplies the electricity balance"
