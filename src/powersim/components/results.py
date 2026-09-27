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
    # Reservoir-hydro records use physical water quantities explicitly.  The
    # state is Mm3, while inflow/release/spill are Mm3/h rates.  Keeping the
    # rates alongside the period duration makes water-balance QA independent
    # of the Pyomo constraint which produced the solution.
    state_of_water_mm3: float | None = None
    previous_state_of_water_mm3: float | None = None
    water_inflow_mm3h: float | None = None
    water_release_mm3h: float | None = None
    water_spill_mm3h: float | None = None
    water_cascade_inflow_mm3h: float | None = None
    water_efficiency_mwh_per_mm3: float | None = None
    spill_cost_usd: float | None = None
    # Physical gas consumed in this period, in Mm3.  This is deliberately a
    # volume rather than an hourly rate so sub-hourly QA cannot omit dt.
    gas_consumption_mm3: float | None = None


@dataclass(frozen=True)
class ReserveResult:
    """Canonical reserve allocation, separate from electricity injection.

    ``provided_mw`` is the provider's raw MW allocation.  The product-level
    requirement is met by ``effective_provided_mw`` after the declared
    derating factor.  ``physical_capability_mw`` is the contemporaneous raw
    capability shared by all products in the same direction, which lets QA
    detect reserve stacking without reading the Pyomo constraints.
    """

    period: int
    product_id: str
    direction: str
    provider_id: str | None = None
    provided_mw: float = 0.0
    effective_provided_mw: float = 0.0
    requirement_mw: float | None = None
    shortfall_mw: float | None = None
    physical_capability_mw: float | None = None
    energy_capability_mw: float | None = None
    derating_factor: float = 1.0
    shortfall_penalty_usd: float | None = None


@dataclass(frozen=True)
class ComponentQAMetadata:
    kind: str
    checks: tuple[str, ...]
    convention: str = "positive injection supplies the electricity balance"
