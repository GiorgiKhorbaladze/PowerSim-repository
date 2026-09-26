"""Shared pumped-hydro core with the legacy two-head formulation."""
from __future__ import annotations

import re
from hashlib import sha1
from typing import Any

from .context import BoundaryState, BuildContext
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
        return []

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

    def objective_terms(self, context: BuildContext) -> list[CostTerm]: return []
    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        import pyomo.environ as pyo
        return BoundaryState(self.asset_id, {"ph_soc":float(pyo.value(solution.ph_soc[self.asset_id,at]) or 0)})
    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        m, aid = solution, self.asset_id
        return [ComponentResult(aid, self.kind, context.period_coordinate(i), injection_mw=float(pyo.value(m.ph_gen_hi[aid,t]+m.ph_gen_lo[aid,t]) or 0), withdrawal_mw=float(pyo.value(m.ph_pmp_hi[aid,t]+m.ph_pmp_lo[aid,t]) or 0), state_of_charge_mwh=float(pyo.value(m.ph_soc[aid,t]) or 0)) for i,t in enumerate(context.periods)]
    def qa_spec(self) -> ComponentQAMetadata: return ComponentQAMetadata(self.kind, ("pumped_hydro_bounds", "pumped_hydro_mode"))
