"""Shared pumped-hydro core with the legacy two-head formulation."""
from __future__ import annotations
import math

import re
from hashlib import sha1
from typing import Any

from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import BoundaryStatePort, PowerInjectionPort, PowerWithdrawalPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm


class PumpedHydroComponent:
    kind = "pumped_hydro"
    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})

    def __init__(self, asset: dict[str, Any]):
        self.asset, self.asset_id = asset, str(asset["id"])

    def _name(self, stem: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_]", "_", self.asset_id)
        return f"SharedPH{stem}_{safe}_{sha1(self.asset_id.encode()).hexdigest()[:8]}"

    def validate(self, context: BuildContext) -> list:
        context.require_capabilities(self.supported_capabilities, self.asset_id)
        issues=[]
        def number(key, *, minimum=0, maximum=None, default=None):
            try: value=float(self.asset.get(key, default))
            except (TypeError, ValueError): value=float("nan")
            if not math.isfinite(value) or value < minimum or (maximum is not None and value > maximum):
                range_text=f"between {minimum} and {maximum}" if maximum is not None else f"at least {minimum}"
                issues.append(make_validation_issue(self.asset_id,"invalid_pumped_hydro_parameter",f"{key} must be numeric, finite, and {range_text}",field_name=key))
            return value
        number("pmax"); number("pump_mw"); number("energy_mwh")
        soc_min=number("soc_min", maximum=1); soc_max=number("soc_max", maximum=1); soc_init=number("soc_init", maximum=1)
        for key in ("efficiency_pump", "efficiency_gen"):
            number(key, minimum=1e-12, maximum=1)
        for key, fallback in (("efficiency_pump_deep", self.asset.get("efficiency_pump")), ("efficiency_gen_deep", self.asset.get("efficiency_gen"))):
            if key in self.asset: number(key, minimum=1e-12, maximum=1)
        number("soc_deep_threshold", maximum=1, default=.3); number("vom", default=0)
        if all(math.isfinite(v) for v in (soc_min,soc_max,soc_init)) and not (soc_min <= soc_init <= soc_max):
            issues.append(make_validation_issue(self.asset_id,"invalid_pumped_hydro_soc_range","soc_min <= soc_init <= soc_max is required",field_name="soc_init"))
        context.validation_issues.extend(issues); return issues

    def declare_parameters(self, context: BuildContext) -> None: pass

    def declare_variables(self, context: BuildContext) -> None:
        need = ("ph_soc", "ph_mode", "ph_zhi", "ph_gen_hi", "ph_gen_lo", "ph_pmp_hi", "ph_pmp_lo")
        missing = [name for name in need if not hasattr(context.builder, name)]
        if missing: raise ValueError(f"pumped_hydro component requires model variable(s): {', '.join(missing)}")

    def add_constraints(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        m, aid, a, periods, dt = context.builder, self.asset_id, self.asset, context.periods, context.duration_hours
        cap = float(a["energy_mwh"]); pmax, pump = float(a["pmax"]), float(a["pump_mw"])
        ep_hi = float(a["efficiency_pump"]); ep_lo = float(a.get("efficiency_pump_deep", ep_hi * .85))
        eg_hi = float(a["efficiency_gen"]); eg_lo = float(a.get("efficiency_gen_deep", eg_hi * .85))
        init = context.legacy_initial_state.get(aid, {})
        def soc(model, t):
            prior = init.get("ph_soc", float(a["soc_init"]) * cap) if t == periods[0] else model.ph_soc[aid, t - 1]
            return model.ph_soc[aid, t] == prior + (ep_hi * model.ph_pmp_hi[aid,t] + ep_lo * model.ph_pmp_lo[aid,t] - model.ph_gen_hi[aid,t] / max(eg_hi,.001) - model.ph_gen_lo[aid,t] / max(eg_lo,.001)) * dt
        m.add_component(self._name("SOC"), pyo.Constraint(periods, rule=soc))
        m.add_component(self._name("LB"), pyo.Constraint(periods, rule=lambda model,t: model.ph_soc[aid,t] >= float(a["soc_min"]) * cap))
        m.add_component(self._name("UB"), pyo.Constraint(periods, rule=lambda model,t: model.ph_soc[aid,t] <= float(a["soc_max"]) * cap))
        threshold, big_m = float(a.get("soc_deep_threshold", .3)) * cap, cap
        m.add_component(self._name("Seg"), pyo.Constraint(periods, rule=lambda model,t: model.ph_soc[aid,t] >= threshold - big_m * (1 - model.ph_zhi[aid,t])))
        specs = (("GenHiMode", m.ph_gen_hi, pmax, lambda model,t: model.ph_mode[aid,t]), ("GenLoMode", m.ph_gen_lo, pmax, lambda model,t: model.ph_mode[aid,t]), ("GenHiSeg", m.ph_gen_hi, pmax, lambda model,t: model.ph_zhi[aid,t]), ("GenLoSeg", m.ph_gen_lo, pmax, lambda model,t: 1-model.ph_zhi[aid,t]), ("PmpHiMode", m.ph_pmp_hi, pump, lambda model,t: 1-model.ph_mode[aid,t]), ("PmpLoMode", m.ph_pmp_lo, pump, lambda model,t: 1-model.ph_mode[aid,t]), ("PmpHiSeg", m.ph_pmp_hi, pump, lambda model,t: model.ph_zhi[aid,t]), ("PmpLoSeg", m.ph_pmp_lo, pump, lambda model,t: 1-model.ph_zhi[aid,t]))
        for stem, variable, limit, selector in specs:
            m.add_component(self._name(stem), pyo.Constraint(periods, rule=lambda model,t,variable=variable,limit=limit,selector=selector: variable[aid,t] <= limit * selector(model,t)))
        context.power_ports.extend((PowerInjectionPort(aid, {t:m.ph_gen_hi[aid,t]+m.ph_gen_lo[aid,t] for t in periods}), PowerWithdrawalPort(aid, {t:m.ph_pmp_hi[aid,t]+m.ph_pmp_lo[aid,t] for t in periods}), BoundaryStatePort(aid, m.ph_soc[aid,:])))

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        vom=float(self.asset.get("vom", .1) or 0)
        terms=[CostTerm(self.asset_id,"pumped_hydro_vom",expression=vom*(context.builder.ph_gen_hi[self.asset_id,t]+context.builder.ph_gen_lo[self.asset_id,t])*context.duration_hours) for t in context.periods]
        context.cost_terms.extend(terms); return terms
    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        import pyomo.environ as pyo
        return BoundaryState(self.asset_id, {"ph_soc":float(pyo.value(solution.ph_soc[self.asset_id,at]) or 0)})
    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        m, aid = solution, self.asset_id
        out=[]
        initial=context.legacy_initial_state.get(aid, {})
        for i,t in enumerate(context.periods):
            gen_hi=float(pyo.value(m.ph_gen_hi[aid,t]) or 0); gen_lo=float(pyo.value(m.ph_gen_lo[aid,t]) or 0)
            pmp_hi=float(pyo.value(m.ph_pmp_hi[aid,t]) or 0); pmp_lo=float(pyo.value(m.ph_pmp_lo[aid,t]) or 0)
            prior=initial.get("ph_soc",float(self.asset["soc_init"])*float(self.asset["energy_mwh"])) if i == 0 else float(pyo.value(m.ph_soc[aid,t-1]) or 0)
            cost=float(pyo.value(float(self.asset.get("vom", .1) or 0)*(m.ph_gen_hi[aid,t]+m.ph_gen_lo[aid,t])*context.duration_hours) or 0)
            out.append(ComponentResult(aid,self.kind,context.period_coordinate(i),injection_mw=gen_hi+gen_lo,withdrawal_mw=pmp_hi+pmp_lo,state_of_charge_mwh=float(pyo.value(m.ph_soc[aid,t]) or 0),previous_state_of_charge_mwh=float(prior),generation_high_mw=gen_hi,generation_deep_mw=gen_lo,pumping_high_mw=pmp_hi,pumping_deep_mw=pmp_lo,cost_usd=cost))
        return out
    def qa_spec(self) -> ComponentQAMetadata: return ComponentQAMetadata(self.kind, ("pumped_hydro_bounds", "pumped_hydro_mode", "pumped_hydro_soc_recurrence", "pumped_hydro_cost"))
