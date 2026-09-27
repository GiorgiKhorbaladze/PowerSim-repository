"""Shared thermal unit-commitment component.

This is a deliberately narrow Stage-3B extraction.  It owns thermal
operational constraints in the opt-in shared deterministic path while the
legacy objective remains authoritative until Stage 4C objective integration.
That prevents accidental cost double counting during parity migration.
"""
from __future__ import annotations

import math
import re
from hashlib import sha1
from typing import Any

from powersim.contracts import IssueSeverity

from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import AvailabilityPort, PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm


class ThermalComponent:
    kind = "thermal"
    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})

    def __init__(self, asset: dict[str, Any]):
        self.asset = asset
        self.asset_id = str(asset["id"])

    def _issue(self, code: str, message: str, field_name: str | None = None):
        return make_validation_issue(self.asset_id, code, message, field_name=field_name)

    def validate(self, context: BuildContext) -> list:
        context.require_capabilities(self.supported_capabilities, self.asset_id)
        issues = []
        for key in ("pmin", "pmax", "ramp_up", "ramp_down", "min_up", "min_down",
                    "startup_cost", "no_load_cost", "vom"):
            value = self.asset.get(key, 0)
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                numeric = float("nan")
            if not math.isfinite(numeric) or numeric < 0:
                issues.append(self._issue("invalid_thermal_parameter",
                    f"{key} must be numeric, finite and non-negative", key))
        try:
            if float(self.asset.get("pmin", 0) or 0) > float(self.asset.get("pmax", 0) or 0):
                issues.append(self._issue("invalid_thermal_range", "pmin must not exceed pmax", "pmin"))
        except (TypeError, ValueError):
            pass
        hot, cold = self.asset.get("startup_cost_hot"), self.asset.get("startup_cost_cold")
        if (hot is None) != (cold is None):
            issues.append(self._issue("incomplete_startup_buckets",
                "startup_cost_hot and startup_cost_cold must be supplied together", "startup_cost_hot"))
        context.validation_issues.extend(issues)
        return issues

    def declare_parameters(self, context: BuildContext) -> None:
        context.require_capabilities(self.supported_capabilities, self.asset_id)

    def declare_variables(self, context: BuildContext) -> None:
        # The transitional deterministic assembler provides common p/u/y/z
        # variables.  Thermal owns their physical constraints, not duplicate
        # variables with an incompatible extraction surface.
        required = ("p", "u", "y", "z") if self.asset.get("_committable") else ("p",)
        missing = [name for name in required if not hasattr(context.builder, name)]
        if missing:
            raise ValueError(f"thermal component requires model variable(s): {', '.join(missing)}")

    def _name(self, stem: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_]", "_", self.asset_id)
        return f"SharedThermal{stem}_{safe}_{sha1(self.asset_id.encode()).hexdigest()[:8]}"

    def _periods(self, value: float, dt: float) -> int:
        return max(1, int(math.ceil(value / dt)))

    def add_constraints(self, context: BuildContext) -> None:
        import pyomo.environ as pyo

        m, aid, periods, dt = context.builder, self.asset_id, context.periods, context.duration_hours
        committable = bool(self.asset.get("_committable"))
        pmax = float(self.asset.get("pmax", 0) or 0)
        pmin = float(self.asset.get("pmin", 0) or 0)
        def available(t):
            resolver = context.availability_resolver
            return float(resolver(self.asset, periods.index(t)) if resolver else pmax)
        m.add_component(self._name("LB"), pyo.Constraint(periods,
            rule=lambda model, t: model.p[aid, t] >= pmin * model.u[aid, t]
            if committable else model.p[aid, t] >= 0))
        m.add_component(self._name("UB"), pyo.Constraint(periods,
            rule=lambda model, t: model.p[aid, t] <= available(t) * model.u[aid, t]
            if committable else model.p[aid, t] <= available(t)))
        context.power_ports.extend((
            PowerInjectionPort(aid, m.p[aid, :]),
            AvailabilityPort(aid, {t: available(t) for t in periods}),
        ))
        if not committable:
            self._add_ramps(context)
            return

        init = context.legacy_initial_state.get(aid, {})
        m.add_component(self._name("UCLogic"), pyo.Constraint(periods,
            rule=lambda model, t: model.u[aid, t] - (int(init.get("u", 0)) if t == periods[0] else model.u[aid, t - 1])
            == model.y[aid, t] - model.z[aid, t]))
        m.add_component(self._name("YZUB"), pyo.Constraint(periods,
            rule=lambda model, t: model.y[aid, t] + model.z[aid, t] <= 1))
        min_up, min_down = self._periods(float(self.asset.get("min_up", 0) or 0), dt), self._periods(float(self.asset.get("min_down", 0) or 0), dt)
        if min_up >= 2:
            m.add_component(self._name("MinUp"), pyo.Constraint(periods,
                rule=lambda model, t: sum(model.y[aid, tau] for tau in range(t, min(t + min_up - 1, periods[-1]) + 1)) <= model.u[aid, t]))
        if min_down >= 2:
            m.add_component(self._name("MinDown"), pyo.Constraint(periods,
                rule=lambda model, t: sum(model.z[aid, tau] for tau in range(t, min(t + min_down - 1, periods[-1]) + 1)) <= 1 - model.u[aid, t]))
        self._add_boundary_fixes(context, min_up, min_down)
        self._add_hot_start(context)
        self._add_ramps(context)

    def _add_boundary_fixes(self, context: BuildContext, min_up: int, min_down: int) -> None:
        m, aid, periods = context.builder, self.asset_id, context.periods
        prev = context.legacy_initial_state.get(aid, {})
        u0, on0, off0 = int(prev.get("u", 0)), int(prev.get("periods_on", 0)), int(prev.get("periods_off", 0))
        if u0 and 0 < on0 < min_up:
            for t in periods[: min(min_up - on0, len(periods))]:
                m.u[aid, t].fix(1)
        if not u0 and 0 < off0 < min_down:
            for t in periods[: min(min_down - off0, len(periods))]:
                m.u[aid, t].fix(0)

    def _add_hot_start(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        m, aid, periods = context.builder, self.asset_id, context.periods
        if not (hasattr(m, "y_hot") and aid in getattr(m, "MSStart", ())):
            return
        threshold = max(1, int(self.asset.get("hot_start_threshold_h", 3) or 3))
        initial_on = int(context.legacy_initial_state.get(aid, {}).get("periods_on", 0))
        def history(t, k):
            prior = t - k
            return m.u[aid, prior] if prior >= periods[0] else (1.0 if initial_on >= 1 - prior else 0.0)
        m.add_component(self._name("HotY"), pyo.Constraint(periods,
            rule=lambda model, t: model.y_hot[aid, t] <= model.y[aid, t]))
        m.add_component(self._name("HotHistory"), pyo.Constraint(periods,
            rule=lambda model, t: model.y_hot[aid, t] <= sum(history(t, k) for k in range(1, threshold + 1))))

    def _add_ramps(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        m, aid, periods, dt = context.builder, self.asset_id, context.periods, context.duration_hours
        prev = context.legacy_initial_state.get(aid, {}).get("p")
        for stem, field, sign in (("RampUp", "ramp_up", 1), ("RampDn", "ramp_down", -1)):
            rate = float(self.asset.get(field, 9999) or 9999)
            if rate >= 9999:
                continue
            def rule(model, t, rate=rate, sign=sign):
                if t == periods[0]:
                    if prev is None:
                        return pyo.Constraint.Skip
                    return sign * (model.p[aid, t] - float(prev)) <= rate * dt
                return sign * (model.p[aid, t] - model.p[aid, t - 1]) <= rate * dt
            m.add_component(self._name(stem), pyo.Constraint(periods, rule=rule))

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        # Audit-only terms.  The legacy objective remains authoritative in
        # this transition; these records mirror its per-period thermal terms
        # without adding them to a second objective.
        m, aid, dt = context.builder, self.asset_id, context.duration_hours
        terms = []
        for t in context.periods:
            terms.extend((
                CostTerm(aid, "variable_cost", expression=self._variable_cost_expression(m, t, dt)),
                CostTerm(aid, "startup_cost", expression=self._startup_cost_expression(m, t)),
                CostTerm(aid, "no_load_cost", expression=self._no_load_cost_expression(m, t, dt)),
                CostTerm(aid, "co2_cost", expression=self._co2_cost_expression(m, t, dt, context.co2_price_usd_per_t)),
            ))
        context.cost_terms.extend(terms)
        return terms

    def _variable_cost_expression(self, model: Any, period: int, dt: float):
        curve = self.asset.get("heat_rate_curve")
        if isinstance(curve, list) and len(curve) >= 2 and hasattr(model, "hrc_lam"):
            fuel_price = float(self.asset.get("fuel_price", 0) or 0)
            return fuel_price * 0.9478 * dt * sum(
                model.hrc_lam[self.asset_id, index, period] * float(point[0]) * float(point[1])
                for index, point in enumerate(curve)
            )
        marginal_cost = float(self.asset.get("_dispMC", self.asset.get("mc", 0)) or 0)
        return (marginal_cost + float(self.asset.get("vom", 0) or 0)) * model.p[self.asset_id, period] * dt

    def _startup_cost_expression(self, model: Any, period: int):
        if not self.asset.get("_committable"):
            return 0.0
        if hasattr(model, "y_hot") and self.asset_id in getattr(model, "MSStart", ()):
            hot = float(self.asset.get("startup_cost_hot", 0) or 0)
            cold = float(self.asset.get("startup_cost_cold", 0) or 0)
            return hot * model.y_hot[self.asset_id, period] + cold * (model.y[self.asset_id, period] - model.y_hot[self.asset_id, period])
        return float(self.asset.get("startup_cost", 0) or 0) * model.y[self.asset_id, period]

    def _no_load_cost_expression(self, model: Any, period: int, dt: float):
        if not self.asset.get("_committable"):
            return 0.0
        return float(self.asset.get("no_load_cost", 0) or 0) * model.u[self.asset_id, period] * dt

    def _co2_cost_expression(self, model: Any, period: int, dt: float, price: float):
        return float(self.asset.get("co2_factor_t_per_mwh", 0) or 0) * model.p[self.asset_id, period] * dt * price

    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        import pyomo.environ as pyo
        return BoundaryState(self.asset_id, {"u": int(round(pyo.value(solution.u[self.asset_id, at]) or 0)), "p": float(pyo.value(solution.p[self.asset_id, at]) or 0)})

    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        m, aid = solution, self.asset_id
        pmin, pmax = float(self.asset.get("pmin", 0) or 0), float(self.asset.get("pmax", 0) or 0)
        committable = bool(self.asset.get("_committable"))
        out = []
        for index, t in enumerate(context.periods):
            u = float(pyo.value(m.u[aid, t]) or 0) if committable else 1.0
            dispatch = float(pyo.value(m.p[aid, t]) or 0)
            resolver = context.availability_resolver
            available = float(resolver(self.asset, index) if resolver else pmax)
            variable_cost = float(pyo.value(self._variable_cost_expression(m, t, context.duration_hours)) or 0)
            startup_cost = float(pyo.value(self._startup_cost_expression(m, t)) or 0)
            no_load_cost = float(pyo.value(self._no_load_cost_expression(m, t, context.duration_hours)) or 0)
            co2_t = dispatch * context.duration_hours * float(self.asset.get("co2_factor_t_per_mwh", 0) or 0)
            co2_cost = co2_t * context.co2_price_usd_per_t
            gas_mm3 = dispatch * context.duration_hours * float(self.asset.get("_gas_rate", 0) or 0)
            out.append(ComponentResult(aid, self.kind, context.period_coordinate(index), available_mw=available * u,
                injection_mw=dispatch, cost_usd=variable_cost + startup_cost + no_load_cost + co2_cost, commitment=u,
                startup=float(pyo.value(m.y[aid, t]) or 0) if committable else 0.0,
                shutdown=float(pyo.value(m.z[aid, t]) or 0) if committable else 0.0,
                startup_hot=float(pyo.value(m.y_hot[aid, t]) or 0) if committable and hasattr(m, "y_hot") and aid in getattr(m, "MSStart", ()) else None,
                pmin_mw=pmin * u, pmax_mw=available * u,
                variable_cost_usd=variable_cost, startup_cost_usd=startup_cost,
                no_load_cost_usd=no_load_cost, co2_t=co2_t, co2_cost_usd=co2_cost,
                gas_consumption_mm3=gas_mm3))
        return out

    def qa_spec(self) -> ComponentQAMetadata:
        return ComponentQAMetadata(self.kind, ("thermal_bounds", "thermal_uc_transition", "thermal_ramp"))
