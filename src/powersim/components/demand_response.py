"""Shared deterministic demand-response curtailment component."""
from __future__ import annotations
from typing import Any
from .context import BoundaryState, BuildContext
from .ports import PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm

class DemandResponseComponent:
    kind="dr"; contract_version="1.0"; supported_capabilities=frozenset({"deterministic","subhourly"})
    def __init__(self, asset:dict[str,Any]): self.asset,self.asset_id=asset,str(asset["id"])
    def validate(self, context:BuildContext)->list: context.require_capabilities(self.supported_capabilities,self.asset_id); return []
    def declare_parameters(self,context:BuildContext)->None: pass
    def declare_variables(self,context:BuildContext)->None:
        if not hasattr(context.builder,"dr"): raise ValueError("dr component requires model.dr")
    def _available(self,context,t):
        cap=float(self.asset["pmax_curtail"]); key=self.asset.get("availability_profile")
        return cap*float(context.profiles[key][t-1]) if key and key in context.profiles else cap
    def add_constraints(self,context:BuildContext)->None:
        import pyomo.environ as pyo
        m,aid,T,dt=context.builder,self.asset_id,context.periods,context.duration_hours
        m.add_component(f"SharedDRUB_{aid}",pyo.Constraint(T,rule=lambda model,t:model.dr[aid,t]<=self._available(context,t)))
        remaining=(context.legacy_initial_state.get("_dr_remaining_hours") or {}).get(aid)
        if remaining is None:
            cap_mwh=float(self.asset.get("hours_per_year_max",8760))*float(self.asset["pmax_curtail"])*(len(T)*dt/8760)
        else: cap_mwh=max(0,float(remaining))*float(self.asset["pmax_curtail"])
        m.add_component(f"SharedDRAnnual_{aid}",pyo.Constraint(expr=sum(m.dr[aid,t]*dt for t in T)<=cap_mwh))
        context.power_ports.append(PowerInjectionPort(aid,m.dr[aid,:]))
    def objective_terms(self,context:BuildContext)->list[CostTerm]:
        terms=[CostTerm(self.asset_id,"demand_response_cost",expression=float(self.asset.get("price_per_mwh",0))*context.builder.dr[self.asset_id,t]*context.duration_hours) for t in context.periods]; context.cost_terms.extend(terms); return terms
    def boundary_state(self,solution:Any,at:int)->BoundaryState: return BoundaryState(self.asset_id,{})
    def extract(self,solution:Any,context:BuildContext)->list[ComponentResult]:
        import pyomo.environ as pyo
        return [ComponentResult(self.asset_id,self.kind,context.period_coordinate(i),injection_mw=float(pyo.value(solution.dr[self.asset_id,t]) or 0),available_mw=self._available(context,t),curtailed_mw=float(pyo.value(solution.dr[self.asset_id,t]) or 0),curtailed_mwh=float(pyo.value(solution.dr[self.asset_id,t]) or 0)*context.duration_hours,cost_usd=float(pyo.value(float(self.asset.get("price_per_mwh",0))*solution.dr[self.asset_id,t]*context.duration_hours) or 0)) for i,t in enumerate(context.periods)]
    def qa_spec(self)->ComponentQAMetadata: return ComponentQAMetadata(self.kind,("demand_response_bounds",))
