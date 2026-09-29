"""Truthful v1 capacity-expansion *screening* wrapper.

The historical planner is retained unchanged for compatibility.  This entrypoint
accepts only the small LP it can actually evidence: hourly annual-energy proxy,
continuous new-build MW, capacity-credit and capacity-factor constraints, with
no dispatch, network, retirement, unit commitment, chronology or integer block
claim.  It reconstructs the reported LP accounting from canonical plan output.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from powersim_expansion import plan, plan_multi_year


class ExpansionValidationError(ValueError):
    def __init__(self, issues: list[dict[str, str]]):
        self.issues = issues
        super().__init__("scoped expansion input is invalid: " + "; ".join(
            f"{x['path']}: {x['message']}" for x in issues))


def _number(value: Any) -> float | None:
    try: value = float(value)
    except (TypeError, ValueError): return None
    return value if math.isfinite(value) else None


def validate_scoped_expansion_input(inp: Mapping[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]]=[]
    if not isinstance(inp, Mapping): return [{"path":"input","message":"must be an object"}]
    resolution = _number(inp.get("resolution_min", 60))
    if resolution is None or resolution != 60:
        issues.append({"path":"resolution_min","message":"scoped expansion supports hourly (60-minute) energy proxies only"})
    profiles=inp.get("profiles")
    demand=profiles.get("demand") if isinstance(profiles, Mapping) else None
    if not isinstance(demand, list) or not demand:
        issues.append({"path":"profiles.demand","message":"must be a non-empty hourly MW list"})
    elif any(_number(x) is None or _number(x) < 0 for x in demand):
        issues.append({"path":"profiles.demand","message":"must contain finite non-negative MW values"})
    ex=inp.get("expansion")
    if not isinstance(ex, Mapping): return issues+[{"path":"expansion","message":"is required"}]
    if ex.get("mode") != "scoped_screening":
        issues.append({"path":"expansion.mode","message":"must be 'scoped_screening'"})
    years=_number(ex.get("years", 1))
    if years is None or years < 1 or years != int(years): issues.append({"path":"expansion.years","message":"must be a positive integer"})
    for field, low in (("discount_rate",0.0),("reserve_margin",0.0)):
        value=_number(ex.get(field, .08 if field=="discount_rate" else .15))
        if value is None or value < low: issues.append({"path":f"expansion.{field}","message":"must be finite and non-negative"})
    candidates=ex.get("candidates")
    if not isinstance(candidates,list) or not candidates: return issues+[{"path":"expansion.candidates","message":"must be a non-empty list"}]
    seen=set()
    for index,candidate in enumerate(candidates):
        path=f"expansion.candidates[{index}]"
        if not isinstance(candidate,Mapping): issues.append({"path":path,"message":"must be an object"}); continue
        cid=candidate.get("id")
        if not isinstance(cid,str) or not cid or cid in seen: issues.append({"path":f"{path}.id","message":"must be a unique non-empty string"})
        else: seen.add(cid)
        if candidate.get("block_mw") is not None:
            issues.append({"path":f"{path}.block_mw","message":"integer/block builds are not supported by the continuous screening LP"})
        for field in ("capex_per_mw","opex_per_mw_yr","max_build_mw"):
            value=_number(candidate.get(field))
            if value is None or value < 0: issues.append({"path":f"{path}.{field}","message":"must be finite and non-negative"})
        life=_number(candidate.get("life_yrs",20))
        if life is None or life <= 0: issues.append({"path":f"{path}.life_yrs","message":"must be finite and positive"})
        for field,default in (("capacity_credit",.15),("capacity_factor",.4)):
            value=_number(candidate.get(field,default))
            if value is None or not 0 <= value <= 1: issues.append({"path":f"{path}.{field}","message":"must be finite in [0, 1]"})
    return issues


def _qa(inp: Mapping[str, Any], output: Mapping[str, Any]) -> dict[str, Any]:
    """Independent arithmetic reconstruction; it never reads Pyomo constraints."""
    issues=[]; plan_out=output.get("plan") if isinstance(output.get("plan"),Mapping) else {}
    ex=inp["expansion"]; cands={str(c["id"]):c for c in ex["candidates"]}; years=int(ex.get("years",1))
    if plan_out.get("error"):
        return {"status":"fail","issues":["planner returned an error"],"classification":"screening"}
    if years == 1:
        builds=plan_out.get("builds_mw") if isinstance(plan_out.get("builds_mw"),Mapping) else {}
        cost=firm=energy=0.0
        for cid,candidate in cands.items():
            build=_number(builds.get(cid)); rate=float(ex.get("discount_rate",.08)); life=float(candidate.get("life_yrs",20)); crf=(1.0/life if rate <= 0 else rate*(1+rate)**life/((1+rate)**life-1))
            if build is None or build < -1e-8: issues.append(f"invalid build for {cid}"); continue
            cost += (float(candidate["capex_per_mw"])*crf+float(candidate["opex_per_mw_yr"]))*build
            firm += float(candidate.get("capacity_credit",.15))*build; energy += float(candidate.get("capacity_factor",.4))*8760*build
        objective = _number(plan_out.get("objective_usd")); existing_firm = _number(plan_out.get("existing_firm_mw")); target_firm = _number(plan_out.get("adequacy_target_mw")); existing_energy = _number(plan_out.get("existing_energy_mwh")); target_energy = _number(plan_out.get("energy_target_mwh"))
        if objective is None or abs(cost-objective) > 1e-7: issues.append("annual objective does not reconstruct")
        if existing_firm is None or target_firm is None or firm+existing_firm+1e-7 < target_firm: issues.append("capacity target is not met")
        if existing_energy is None or target_energy is None or energy+existing_energy+1e-7 < target_energy: issues.append("energy target is not met")
    else:
        # Multi-year plan publishes build/cumulative maps, which are enough to verify the recursions and caps.
        build_by=plan_out.get("builds_by_year"); cumulative=plan_out.get("cumulative_by_year")
        if not isinstance(build_by,Mapping) or not isinstance(cumulative,Mapping): issues.append("missing multi-year canonical build states")
        else:
            previous={cid:0.0 for cid in cands}
            cost = 0.0; rate=float(ex.get("discount_rate",.08))
            for year in range(1,years+1):
                now=cumulative.get(year) or cumulative.get(str(year)); new=build_by.get(year) or build_by.get(str(year))
                if not isinstance(now,Mapping) or not isinstance(new,Mapping): issues.append(f"missing year {year}"); continue
                for cid,candidate in cands.items():
                    x=_number(new.get(cid)); total=_number(now.get(cid))
                    if x is None or total is None or x < -1e-8 or abs(total-(previous[cid]+x)) > .011 or total > float(candidate["max_build_mw"])+.011: issues.append(f"invalid cumulative build {cid}, year {year}")
                    if x is not None and total is not None:
                        life=float(candidate.get("life_yrs",20)); crf = (1.0/life if rate <= 0 else rate*(1+rate)**life/((1+rate)**life-1))
                        cost += (1+rate)**(-year) * (float(candidate["capex_per_mw"])*crf*x + float(candidate["opex_per_mw_yr"])*total)
                    previous[cid]=total if total is not None else previous[cid]
            objective = _number(plan_out.get("objective_usd"))
            if objective is None or abs(cost-objective) > 2.0:
                # Legacy canonical build states are rounded to 0.01 MW, so their
                # independently reconstructed NPV has a bounded extraction error.
                issues.append("multi-year objective does not reconstruct")
    return {"status":"pass" if not issues else "fail","issues":issues,"classification":"screening"}


def run_scoped_expansion(inp: Mapping[str, Any]) -> dict[str, Any]:
    issues=validate_scoped_expansion_input(inp)
    if issues: raise ExpansionValidationError(issues)
    result=plan_multi_year(dict(inp)) if int(inp["expansion"].get("years",1)) > 1 else plan(dict(inp))
    out={"workflow":"scoped_capacity_expansion_screening","plan":result,"scope":{
        "supported":"continuous new-build MW, capacity-credit and annual-energy proxy constraints, single or multi-year perfect-foresight accounting",
        "excluded":"chronological dispatch, UC, network, retirements, integer/block builds, storage chronology, endogenous capacity credit, fuel/emissions and correlated availability",
    }}
    qa=_qa(inp,out); valid=qa["status"]=="pass"; out.update({"qa":qa,"result_validity":"valid" if valid else "invalid","publication":{"publishable":valid,"classification":"screening","reasons":[] if valid else ["expansion_screening_qa_failed"]}})
    return out
