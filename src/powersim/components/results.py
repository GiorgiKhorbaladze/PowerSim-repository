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
    state_of_charge_mwh: float | None = None
    # Stateful storage records carry the physical state immediately before
    # this period.  This makes the first committed period of a rolling window
    # independently auditable without reaching back into the Pyomo model.
    previous_state_of_charge_mwh: float | None = None
    previous_injection_mw: float | None = None
    previous_withdrawal_mw: float | None = None
    # Pumped-hydro uses two physical efficiency segments.  Totals alone are
    # insufficient to reconstruct its energy balance, so extraction preserves
    # each flow explicitly.
    generation_high_mw: float | None = None
    generation_deep_mw: float | None = None
    pumping_high_mw: float | None = None
    pumping_deep_mw: float | None = None
    # These BESS fields expose legacy-owned depth accounting for audit only;
    # they do not assert that the current Big-M selector is an iff relation.
    deep_discharge_mw: float | None = None
    shallow_selector: float | None = None


@dataclass(frozen=True)
class ComponentQAMetadata:
    kind: str
    checks: tuple[str, ...]
    convention: str = "positive injection supplies the electricity balance"
