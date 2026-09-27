"""Shared deterministic demand-response curtailment component."""
from __future__ import annotations
import math
from typing import Any
from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm
from powersim.contracts import IssueSeverity

class DemandResponseComponent:
    kind="dr"; contract_version="1.0"; supported_capabilities=frozenset({"deterministic","subhourly"})
    def __init__(self, asset:dict[str,Any]): self.asset,self.asset_id=asset,str(asset["id"])
    def validate(self, context:BuildContext)->list:
        """Validate without allowing malformed scalars to escape as float errors.

        Native-v1 treated a declared but missing availability profile as full
        availability.  The compatibility warning below preserves that legacy
        fallback explicitly; a present profile is still checked strictly.
        """
        context.require_capabilities(self.supported_capabilities,self.asset_id)
        issues=[]
        for key, default in (("pmax_curtail", None), ("price_per_mwh", 0), ("hours_per_year_max", 8760)):
            raw=self.asset.get(key, default)
            try: value=float(raw)
            except (TypeError, ValueError): value=float("nan")
            if not math.isfinite(value) or value < 0:
                issues.append(make_validation_issue(self.asset_id,"invalid_demand_response_parameter",f"{key} must be numeric, finite, and non-negative",field_name=key))
        profile_key=self.asset.get("availability_profile")
        if profile_key:
            values=context.profiles.get(profile_key)
            if values is None:
                issues.append(make_validation_issue(self.asset_id,"demand_response_availability_profile_compatibility_fallback","availability_profile is absent; native-v1 compatibility treats DR as fully available",field_name="availability_profile",severity=IssueSeverity.WARNING,compatibility=True))
            elif not isinstance(values, (list, tuple)) or len(values) < len(context.periods):
                issues.append(make_validation_issue(self.asset_id,"invalid_demand_response_availability_profile","availability_profile must cover every required period",field_name="availability_profile"))
            else:
                for index, raw in enumerate(values[:len(context.periods)]):
                    try: value=float(raw)
                    except (TypeError, ValueError): value=float("nan")
                    if not math.isfinite(value) or value < 0:
                        issues.append(make_validation_issue(self.asset_id,"invalid_demand_response_availability_value","availability factors must be numeric, finite, and non-negative",field_name="availability_profile"))
                        break
        context.validation_issues.extend(issues); return issues
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
    def qa_spec(self)->ComponentQAMetadata:
        return ComponentQAMetadata(self.kind,("demand_response_bounds","demand_response_cost","demand_response_energy_cap"),"DR injection_mw is reduced demand supplied to the balance; curtailed_mw is the same action from the demand-accounting viewpoint")
