"""Shared reservoir-hydro core preserving the legacy non-cascade equations.

Cascade transport remains a separate Stage-3D coupling migration.  Assets
declaring an upstream cascade deliberately stay on the legacy implementation
until that coupler is authoritative; this prevents a partial migration from
silently dropping water.
"""
from __future__ import annotations

import math
import re
from hashlib import sha1
from typing import Any

from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import BoundaryStatePort, PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm


def hydro_efficiency_at(hydro: dict[str, Any], storage_mm3: float) -> float:
    """Legacy window-level MWh/Mm3 interpolation, retained verbatim."""
    curve = hydro.get("head_efficiency_curve")
    if not curve:
        return float(hydro.get("efficiency", 350))
    points = sorted(curve, key=lambda point: float(point[0]))
    storage = float(storage_mm3)
    if storage <= float(points[0][0]):
        return float(points[0][1])
    if storage >= float(points[-1][0]):
        return float(points[-1][1])
    for left, right in zip(points, points[1:]):
        x0, y0, x1, y1 = map(float, (*left, *right))
        if x0 <= storage <= x1:
            return y0 + (storage - x0) * (y1 - y0) / (x1 - x0)
    return float(points[-1][1])


class ReservoirHydroComponent:
    kind = "hydro_reg"
    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})

    def __init__(self, asset: dict[str, Any]):
        self.asset, self.asset_id = asset, str(asset["id"])

    @staticmethod
    def supports_shared(asset: dict[str, Any]) -> bool:
        """Return false where legacy-only water/economic behaviour remains."""
        hydro = asset.get("hydro") or {}
        # The legacy soft end-level penalty is declared after the Pyomo
        # objective, so it is not actually charged.  Activating it here
        # would be a material dispatch/economic change, not a migration.
        strategic_penalty = float(hydro.get("end_level_penalty", 0) or 0) > 0 and float(hydro.get("target_end_level_frac", 0) or 0) > 0
        # The shared reservoir component owns water physics for dispatch-only
        # reservoir units.  Legacy hydro commitment has UC, pmin and startup
        # ownership which has not been migrated to this component; retaining
        # it on the legacy path is required for parity.
        committable = bool(asset.get("_committable", asset.get("committable", True)))
        return not strategic_penalty and not committable

    def _name(self, stem: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_]", "_", self.asset_id)
        return f"SharedReservoir{stem}_{safe}_{sha1(self.asset_id.encode()).hexdigest()[:8]}"

    def validate(self, context: BuildContext) -> list:
        context.require_capabilities(self.supported_capabilities, self.asset_id)
        issues = []
        hydro = self.asset.get("hydro")
        if not isinstance(hydro, dict):
            issues.append(make_validation_issue(self.asset_id, "invalid_reservoir_hydro", "hydro must be an object", field_name="hydro"))
            context.validation_issues.extend(issues)
            return issues
        def number(key, *, minimum=None):
            value = hydro.get(key)
            try: numeric = float(value)
            except (TypeError, ValueError): numeric = float("nan")
            if not math.isfinite(numeric) or (minimum is not None and numeric < minimum):
                suffix = " and non-negative" if minimum == 0 else " and positive" if minimum else ""
                issues.append(make_validation_issue(self.asset_id, "invalid_reservoir_parameter", f"hydro.{key} must be numeric, finite{suffix}", field_name=f"hydro.{key}"))
            return numeric
        reservoir_min = number("reservoir_min", minimum=0)
        reservoir_max = number("reservoir_max", minimum=0)
        reservoir_init = number("reservoir_init", minimum=0)
        number("reservoir_end_min", minimum=0)
        number("efficiency", minimum=1e-12)
        for key in ("min_release_mm3h", "spill_cost_usd_per_mm3", "spill_cost", "end_level_penalty"):
            if key in hydro: number(key, minimum=0)
        if "target_end_level_frac" in hydro:
            fraction = number("target_end_level_frac", minimum=0)
            if math.isfinite(fraction) and fraction > 1:
                issues.append(make_validation_issue(self.asset_id, "invalid_reservoir_parameter", "hydro.target_end_level_frac must not exceed 1", field_name="hydro.target_end_level_frac"))
        if all(math.isfinite(value) for value in (reservoir_min, reservoir_init, reservoir_max)) and not reservoir_min <= reservoir_init <= reservoir_max:
            issues.append(make_validation_issue(self.asset_id, "invalid_reservoir_range", "hydro.reservoir_min <= reservoir_init <= reservoir_max is required", field_name="hydro.reservoir_init"))
        curve = hydro.get("head_efficiency_curve")
        if curve is not None:
            try:
                valid_curve = len(curve) >= 2 and all(len(point) == 2 and all(math.isfinite(float(x)) for x in point) and float(point[1]) > 0 for point in curve)
            except (TypeError, ValueError): valid_curve = False
            if not valid_curve:
                issues.append(make_validation_issue(self.asset_id, "invalid_head_efficiency_curve", "hydro.head_efficiency_curve must contain at least two finite [storage_mm3, efficiency_mwh_per_mm3] points with positive efficiency", field_name="hydro.head_efficiency_curve"))
        profile = self.asset.get("inflow_profile")
        if profile is not None and (profile not in context.profiles or not isinstance(context.profiles[profile], list) or len(context.profiles[profile]) < len(context.periods)):
            issues.append(make_validation_issue(self.asset_id, "invalid_reservoir_inflow_profile", "inflow_profile must resolve to a list covering all model periods", field_name="inflow_profile"))
        # The existing cascade equation references an upstream variable in the
        # local Pyomo window.  A non-zero delay needs committed upstream flow
        # from an earlier rolling window, which this migration does not carry.
        # Reject this shared combination rather than silently bootstrapping it
        # with zero water. Non-rolling delayed cascades and zero-delay rolling
        # cascades retain the characterized legacy equation.
        if (
            hydro.get("cascade_upstream")
            and float(hydro.get("cascade_travel_delay_h", 0) or 0) > 0
            and bool(context.legacy_initial_state.get("_powersim_rolling"))
        ):
            issues.append(make_validation_issue(
                self.asset_id,
                "unsupported_rolling_cascade_delay",
                "shared rolling reservoir cascades require cascade_travel_delay_h=0; nonzero delayed transport is legacy compatibility only",
                field_name="hydro.cascade_travel_delay_h",
                compatibility=True,
            ))
        context.validation_issues.extend(issues)
        return issues

    def declare_parameters(self, context: BuildContext) -> None: pass

    def declare_variables(self, context: BuildContext) -> None:
        missing = [name for name in ("p", "stor", "spill") if not hasattr(context.builder, name)]
        if missing:
            raise ValueError(f"reservoir hydro component requires model variable(s): {', '.join(missing)}")

    def _initial_storage(self, context: BuildContext) -> float:
        hydro = self.asset["hydro"]
        return float(context.legacy_initial_state.get(self.asset_id, {}).get("stor", hydro.get("reservoir_init", 700)))

    def _efficiency(self, context: BuildContext) -> float:
        return hydro_efficiency_at(self.asset["hydro"], self._initial_storage(context))

    def _inflow(self, context: BuildContext, index: int) -> float:
        profile = self.asset.get("inflow_profile")
        if profile and profile in context.profiles:
            return float(context.profiles[profile][index])
        return float(self.asset["hydro"].get("inflow", 0) or 0)

    def _available(self, context: BuildContext, index: int) -> float:
        resolver = context.availability_resolver
        if resolver is not None:
            return float(resolver(self.asset, index))
        return float(self.asset.get("pmax", 0) or 0)

    def _cascade_inflow(self, model: Any, context: BuildContext, t: int) -> Any:
        """Exact legacy cascade transport: delayed turbine flow plus optional spill."""
        hydro = self.asset["hydro"]
        upstream = hydro.get("cascade_upstream")
        if not upstream or upstream not in context.assets:
            return 0.0
        periods_per_hour = max(1, int(round(1 / context.duration_hours)))
        upstream_t = t - int(hydro.get("cascade_travel_delay_h", 0) or 0) * periods_per_hour
        if upstream_t < context.periods[0]:
            return 0.0
        upstream_hydro = context.assets[upstream].get("hydro") or {}
        upstream_initial = float(context.legacy_initial_state.get(upstream, {}).get("stor", upstream_hydro.get("reservoir_init", 700)))
        upstream_efficiency = max(hydro_efficiency_at(upstream_hydro, upstream_initial), .001)
        turbine = model.p[upstream, upstream_t] / upstream_efficiency
        if hydro.get("cascade_flow_mode", "turbined_only") == "release_plus_spill":
            turbine += model.spill[upstream, upstream_t]
        return float(hydro.get("cascade_gain", 1.0) or 0) * turbine

    def add_constraints(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        m, aid, periods, dt, hydro = context.builder, self.asset_id, context.periods, context.duration_hours, self.asset["hydro"]
        efficiency = max(self._efficiency(context), .001)
        # GenUB is intentionally skipped for all migrated assets by the
        # transitional assembler.  This component is therefore the sole
        # owner of the non-committable reservoir dispatch limit.  Use the
        # legacy availability seam so maintenance/temperature derating keeps
        # its established semantics.
        m.add_component(
            self._name("Pmax"),
            pyo.Constraint(periods, rule=lambda model, t: model.p[aid, t] <= self._available(context, periods.index(t))),
        )
        def balance(model, t):
            index = periods.index(t)
            previous = self._initial_storage(context) if index == 0 else model.stor[aid, t - 1]
            return model.stor[aid, t] == previous + (self._inflow(context, index) + self._cascade_inflow(model, context, t) - model.p[aid, t] / efficiency - model.spill[aid, t]) * dt
        m.add_component(self._name("Balance"), pyo.Constraint(periods, rule=balance))
        m.add_component(self._name("LB"), pyo.Constraint(periods, rule=lambda model, t: model.stor[aid, t] >= float(hydro.get("reservoir_min", 0))))
        m.add_component(self._name("UB"), pyo.Constraint(periods, rule=lambda model, t: model.stor[aid, t] <= float(hydro.get("reservoir_max", 9999))))
        minimum_release = float(hydro.get("min_release_mm3h", 0) or 0)
        if minimum_release > 0:
            m.add_component(self._name("MinRelease"), pyo.Constraint(periods, rule=lambda model, t: model.p[aid, t] / efficiency + model.spill[aid, t] >= minimum_release))
        if bool(getattr(m, "_powersim_is_last_window", True)):
            m.add_component(self._name("EndFloor"), pyo.Constraint(expr=m.stor[aid, periods[-1]] >= float(hydro.get("reservoir_end_min", hydro.get("reservoir_min", 0)))))
        # Storage target shortfall variables are declared before the legacy
        # objective.  Reuse them so its authoritative cost remains unchanged.
        if hasattr(m, "StorTgt"):
            for key in m.StorTgt:
                if key[0] != aid:
                    continue
                period = m._stor_target_period[key]
                target = m._stor_target_target[key]
                m.add_component(self._name(f"Target_{key[1]}"), pyo.Constraint(expr=m.stor_target_short[key] >= target - m.stor[aid, period]))
        context.power_ports.extend((PowerInjectionPort(aid, m.p[aid, :]), BoundaryStatePort(aid, m.stor[aid, :])))

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        m, aid, dt = context.builder, self.asset_id, context.duration_hours
        price = float(self.asset["hydro"].get("spill_cost_usd_per_mm3", self.asset["hydro"].get("spill_cost", 0)) or 0)
        terms = [CostTerm(aid, "reservoir_spill_cost", expression=price * m.spill[aid, t] * dt) for t in context.periods]
        context.cost_terms.extend(terms)
        return terms

    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        import pyomo.environ as pyo
        return BoundaryState(self.asset_id, {"stor": float(pyo.value(solution.stor[self.asset_id, at]) or 0)})

    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        m, aid, hydro, dt = solution, self.asset_id, self.asset["hydro"], context.duration_hours
        efficiency, spill_price = self._efficiency(context), float(hydro.get("spill_cost_usd_per_mm3", hydro.get("spill_cost", 0)) or 0)
        output = []
        for index, t in enumerate(context.periods):
            dispatch = float(pyo.value(m.p[aid, t]) or 0)
            spill = float(pyo.value(m.spill[aid, t]) or 0)
            storage = float(pyo.value(m.stor[aid, t]) or 0)
            previous = self._initial_storage(context) if index == 0 else float(pyo.value(m.stor[aid, t - 1]) or 0)
            cascade = float(pyo.value(self._cascade_inflow(m, context, t)) or 0)
            output.append(ComponentResult(aid, self.kind, context.period_coordinate(index), available_mw=self._available(context, index), injection_mw=dispatch, cost_usd=spill_price * spill * dt, state_of_water_mm3=storage, previous_state_of_water_mm3=previous, water_inflow_mm3h=self._inflow(context, index), water_release_mm3h=dispatch / max(efficiency, .001), water_spill_mm3h=spill, water_cascade_inflow_mm3h=cascade, water_efficiency_mwh_per_mm3=efficiency, spill_cost_usd=spill_price * spill * dt))
        return output

    def qa_spec(self) -> ComponentQAMetadata:
        return ComponentQAMetadata(self.kind, ("reservoir_bounds", "reservoir_water_balance", "reservoir_min_release", "reservoir_spill_cost"))
