"""Shared battery energy storage system component (Stage 3C)."""
from __future__ import annotations

import math
import re
from hashlib import sha1
from typing import Any

from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import PowerInjectionPort, PowerWithdrawalPort, BoundaryStatePort
from .results import ComponentQAMetadata, ComponentResult, CostTerm


class BESSComponent:
    kind = "bess"
    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})

    def __init__(self, asset: dict[str, Any]):
        self.asset = asset
        self.asset_id = str(asset["id"])

    def _name(self, stem: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_]", "_", self.asset_id)
        return f"SharedBESS{stem}_{safe}_{sha1(self.asset_id.encode()).hexdigest()[:8]}"

    def validate(self, context: BuildContext) -> list:
        context.require_capabilities(self.supported_capabilities, self.asset_id)
        issues = []
        for key in ("power_mw", "energy_mwh", "eta_charge", "eta_discharge", "soc_min", "soc_max", "soc_init"):
            try:
                value = float(self.asset.get(key, 0))
            except (TypeError, ValueError):
                value = float("nan")
            if not math.isfinite(value) or value < 0:
                issues.append(make_validation_issue(self.asset_id, "invalid_bess_parameter", f"{key} must be finite and non-negative", field_name=key))
        try:
            if float(self.asset.get("soc_min", 0)) > float(self.asset.get("soc_max", 1)):
                issues.append(make_validation_issue(self.asset_id, "invalid_bess_soc_range", "soc_min must not exceed soc_max", field_name="soc_min"))
        except (TypeError, ValueError):
            pass
        context.validation_issues.extend(issues)
        return issues

    def declare_parameters(self, context: BuildContext) -> None:
        context.require_capabilities(self.supported_capabilities, self.asset_id)

    def declare_variables(self, context: BuildContext) -> None:
        missing = [name for name in ("ch", "dis", "soc", "xch") if not hasattr(context.builder, name)]
        if missing:
            raise ValueError(f"bess component requires model variable(s): {', '.join(missing)}")

    def _charge_cap(self) -> float:
        cap = float(self.asset.get("charge_power_mw", self.asset.get("power_mw", 0)) or 0)
        if self.asset.get("c_rate_max") is not None:
            cap = min(cap, float(self.asset["c_rate_max"]) * float(self.asset.get("energy_mwh", 0) or 0))
        return cap

    def _discharge_cap(self) -> float:
        cap = float(self.asset.get("discharge_power_mw", self.asset.get("power_mw", 0)) or 0)
        if self.asset.get("c_rate_max") is not None:
            cap = min(cap, float(self.asset["c_rate_max"]) * float(self.asset.get("energy_mwh", 0) or 0))
        return cap

    def add_constraints(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        m, aid, periods, dt = context.builder, self.asset_id, context.periods, context.duration_hours
        a = self.asset
        eta_c, eta_d = float(a.get("eta_charge", 1) or 1), max(float(a.get("eta_discharge", 1) or 1), 0.001)
        cap = float(a.get("energy_mwh", 0) or 0)
        self_discharge = float(a.get("self_discharge_pct_per_h", 0) or 0) / 100.0
        aux = float(a.get("aux_mw", 0) or 0)
        initial = context.legacy_initial_state.get(aid, {})
        def balance(model, t):
            previous = initial.get("soc", float(a.get("soc_init", 0) or 0) * cap) if t == periods[0] else model.soc[aid, t - 1]
            return model.soc[aid, t] == previous * (1 - self_discharge * dt) + (eta_c * model.ch[aid, t] - model.dis[aid, t] / eta_d - aux) * dt
        m.add_component(self._name("SOC"), pyo.Constraint(periods, rule=balance))
        m.add_component(self._name("SOCLB"), pyo.Constraint(periods, rule=lambda model, t: model.soc[aid, t] >= float(a.get("soc_min", 0) or 0) * cap))
        m.add_component(self._name("SOCUB"), pyo.Constraint(periods, rule=lambda model, t: model.soc[aid, t] <= float(a.get("soc_max", 1) or 1) * cap))
        m.add_component(self._name("CHUB"), pyo.Constraint(periods, rule=lambda model, t: model.ch[aid, t] <= self._charge_cap() * model.xch[aid, t]))
        m.add_component(self._name("DISUB"), pyo.Constraint(periods, rule=lambda model, t: model.dis[aid, t] <= self._discharge_cap() * (1 - model.xch[aid, t])))
        for stem, variable, field, sign in (("ChRampU", "ch", "ramp_up_mw_min", 1), ("ChRampD", "ch", "ramp_down_mw_min", -1), ("DisRampU", "dis", "ramp_up_mw_min", 1), ("DisRampD", "dis", "ramp_down_mw_min", -1)):
            rate = float(a.get(field, 0) or 0) * 60 * dt
            if rate <= 0:
                continue
            prior_key = "ch_prev" if variable == "ch" else "dis_prev"
            def ramp(model, t, rate=rate, variable=variable, sign=sign, prior_key=prior_key):
                prior = float(initial.get(prior_key, 0) or 0) if t == periods[0] else getattr(model, variable)[aid, t - 1]
                return sign * (getattr(model, variable)[aid, t] - prior) <= rate
            m.add_component(self._name(stem), pyo.Constraint(periods, rule=ramp))
        context.power_ports.extend((PowerInjectionPort(aid, m.dis[aid, :]), PowerWithdrawalPort(aid, m.ch[aid, :])))
        context.power_ports.append(BoundaryStatePort(aid, m.soc[aid, :]))

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        m, aid, dt, a = context.builder, self.asset_id, context.duration_hours, self.asset
        terms = []
        for t in context.periods:
            expr = float(a.get("vom_discharge", 0) or 0) * m.dis[aid, t] * dt
            expr += float(a.get("cycle_cost_per_mwh", 0) or 0) * (m.ch[aid, t] + m.dis[aid, t]) * dt
            terms.append(CostTerm(aid, "storage_cost", expression=expr))
        context.cost_terms.extend(terms)
        return terms

    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        import pyomo.environ as pyo
        return BoundaryState(self.asset_id, {"soc": float(pyo.value(solution.soc[self.asset_id, at]) or 0), "ch_prev": float(pyo.value(solution.ch[self.asset_id, at]) or 0), "dis_prev": float(pyo.value(solution.dis[self.asset_id, at]) or 0)})

    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        m, aid = solution, self.asset_id
        out = []
        for index, t in enumerate(context.periods):
            charge, discharge = float(pyo.value(m.ch[aid, t]) or 0), float(pyo.value(m.dis[aid, t]) or 0)
            soc = float(pyo.value(m.soc[aid, t]) or 0)
            previous_soc = (context.legacy_initial_state.get(aid, {}).get("soc", float(self.asset.get("soc_init", 0) or 0) * float(self.asset.get("energy_mwh", 0) or 0)) if index == 0 else float(pyo.value(m.soc[aid, t - 1]) or 0))
            previous_charge = (context.legacy_initial_state.get(aid, {}).get("ch_prev", 0) if index == 0 else float(pyo.value(m.ch[aid, t - 1]) or 0))
            previous_discharge = (context.legacy_initial_state.get(aid, {}).get("dis_prev", 0) if index == 0 else float(pyo.value(m.dis[aid, t - 1]) or 0))
            cost = float(pyo.value(
                float(self.asset.get("vom_discharge", 0) or 0) * m.dis[aid, t] * context.duration_hours
                + float(self.asset.get("cycle_cost_per_mwh", 0) or 0) * (m.ch[aid, t] + m.dis[aid, t]) * context.duration_hours
            ) or 0)
            deep = float(pyo.value(m.dis_deep[aid, t]) or 0) if hasattr(m, "dis_deep") and aid in m.DeepBESS else None
            selector = float(pyo.value(m.z_shallow[aid, t]) or 0) if hasattr(m, "z_shallow") and aid in m.DeepBESS else None
            out.append(ComponentResult(aid, self.kind, context.period_coordinate(index), injection_mw=discharge, withdrawal_mw=charge, available_mw=self._discharge_cap(), cost_usd=cost, state_of_charge_mwh=soc, previous_state_of_charge_mwh=float(previous_soc or 0), previous_withdrawal_mw=float(previous_charge or 0), previous_injection_mw=float(previous_discharge or 0), deep_discharge_mw=deep, shallow_selector=selector))
        return out

    def qa_spec(self) -> ComponentQAMetadata:
        return ComponentQAMetadata(self.kind, ("bess_bounds", "bess_mode", "bess_soc_recurrence", "bess_ramp"))
