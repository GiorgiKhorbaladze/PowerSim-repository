"""Validated deterministic N-1 security-constrained UC.

This is preventive base commitment plus corrective post-contingency redispatch.
It is one Pyomo optimization built from the shared deterministic window seam.
Legacy :mod:`powersim_scuc` remains screening-only and is deliberately unused.
"""
from __future__ import annotations

from collections import deque
import math
from pathlib import Path
import sys
from typing import Any

import pyomo.environ as pyo

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from powersim.contracts import QACheckResult, QAReport, QAStatus
from powersim.results import evaluate_publication, finalized_diagnostics
from powersim.solvers import solve_model
from powersim_solver import (build_asset_map, build_gas_limits, build_result_store,
                              extract_window_solution, normalize_dc_network,
                              slice_profiles, solve_window)


SUPPORTED_KINDS = {"unit_outage", "import_outage", "line_outage"}


def validate_contingencies(inp: dict[str, Any], assets: dict[str, dict], network: dict) -> list[dict[str, str]]:
    """Validate the v1.0 one-element N-1 contract without legacy fallbacks."""
    raw = inp.get("contingencies") or []
    if not isinstance(raw, list) or not raw:
        raise ValueError("validated security UC requires a non-empty contingency list")
    seen: set[str] = set(); lines = {str(x["id"]) for x in network.get("lines") or []}; result=[]
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"contingency at index {index} must be an object")
        cid = item.get("id")
        if not isinstance(cid, str) or not cid or cid in seen:
            raise ValueError("contingency ids must be unique and non-empty")
        seen.add(cid)
        kind = item.get("kind")
        if kind not in SUPPORTED_KINDS:
            raise ValueError(f"contingency {cid} kind {kind!r} is unsupported in validated Stage 5B")
        elements = item.get("elements")
        if not isinstance(elements, list) or len(elements) != 1 or not isinstance(elements[0], str) or not elements[0]:
            raise ValueError(f"contingency {cid} must declare exactly one non-empty element")
        target = elements[0]
        if kind == "line_outage":
            if target not in lines:
                raise ValueError(f"contingency {cid} references unknown line {target}")
        else:
            asset = assets.get(target)
            if asset is None:
                raise ValueError(f"contingency {cid} references unknown asset {target}")
            if kind == "import_outage" and asset.get("type") != "import":
                raise ValueError(f"contingency {cid} import_outage target must be type=import")
            if kind == "unit_outage" and asset.get("type") not in {"thermal", "hydro_reg", "hydro_ror", "wind", "solar"}:
                raise ValueError(f"contingency {cid} unit_outage target is not a supported generation asset")
        result.append({"id": cid, "kind": kind, "target": target})
    return result


def _islanding(network: dict, line_id: str) -> bool:
    buses = [str(x["id"]) for x in network.get("buses") or []]
    if not buses: return False
    edges = [x for x in network.get("lines") or [] if str(x["id"]) != line_id]
    graph = {b: [] for b in buses}
    for line in edges:
        graph[str(line["from_bus"])].append(str(line["to_bus"])); graph[str(line["to_bus"])].append(str(line["from_bus"]))
    seen={buses[0]}; q=deque([buses[0]])
    while q:
        for nxt in graph[q.popleft()]:
            if nxt not in seen: seen.add(nxt); q.append(nxt)
    return len(seen) != len(buses)


def _reserve_expression(base, generator: str, period: int, direction: str):
    if not hasattr(base, "RIDS") or generator not in base.G:
        return 0.0
    attr = "res_up" if direction == "up" else "res_down"
    return sum(getattr(base, attr)[rid, generator, period] for rid in base.RIDS)


def build_security_model(inp: dict[str, Any]):
    assets = build_asset_map(inp); profiles, horizon = slice_profiles(inp)
    settings = dict(inp.get("solver_settings") or {})
    if str(settings.get("component_engine", "shared")).lower() != "shared":
        raise ValueError("validated security UC requires solver_settings.component_engine='shared'")
    settings["component_engine"] = "shared"
    network = normalize_dc_network(inp, assets)
    contingencies = validate_contingencies(inp, assets, network)
    if any(c["kind"] == "line_outage" for c in contingencies) and not network.get("lines"):
        raise ValueError("line_outage requires a validated DC network")
    for contingency in contingencies:
        if contingency["kind"] == "line_outage" and _islanding(network, contingency["target"]):
            raise ValueError(f"unsupported_islanding_contingency:{contingency['id']}")
    if not inp.get("reserve_products"):
        raise ValueError("validated corrective SCUC requires explicit reserve products")
    settings["_network"] = network
    settings["_co2_price_usd_per_t"] = float(inp.get("co2_price_usd_per_t", 0) or 0)
    start_h = int((inp.get("study_horizon") or {}).get("start_hour", 0)); dt = int(inp.get("resolution_min", 60)) / 60
    gas = build_gas_limits(inp, start_h, horizon)
    model = pyo.ConcreteModel(); model.base = pyo.Block()
    solve_window(assets, profiles["demand"], profiles, inp.get("reserve_products", []), gas, {}, settings,
                 offset_h=start_h, dt=dt, model=model.base, build_only=True)
    model.C = pyo.Set(initialize=[c["id"] for c in contingencies], ordered=True); model.contingency = pyo.Block(model.C)
    by_id = {c["id"]: c for c in contingencies}
    for cid in model.C:
        block = model.contingency[cid]
        solve_window(assets, profiles["demand"], profiles, inp.get("reserve_products", []), gas, {}, settings,
                     offset_h=start_h, dt=dt, model=block, build_only=True)
    model.security = pyo.ConstraintList()
    for cid in model.C:
        c = by_id[str(cid)]; block = model.contingency[cid]
        for generator in model.base.GC:
            for period in model.base.T:
                model.security.add(block.u[generator, period] == model.base.u[generator, period])
                model.security.add(block.y[generator, period] == model.base.y[generator, period])
                model.security.add(block.z[generator, period] == model.base.z[generator, period])
        # Security states are feasibility states: system-level unserved energy
        # is fixed to zero because it is not locationally meaningful in DC mode.
        for period in model.base.T:
            model.security.add(block.unserv[period] == 0)
        if c["kind"] in {"unit_outage", "import_outage"}:
            for period in model.base.T:
                model.security.add(block.p[c["target"], period] == 0)
        if c["kind"] == "line_outage":
            for period in model.base.T:
                model.security.add(block.fl[c["target"], period] == 0)
        for generator in model.base.G:
            if c["kind"] in {"unit_outage", "import_outage"} and generator == c["target"]:
                continue
            for period in model.base.T:
                model.security.add(block.p[generator, period] <= model.base.p[generator, period] + _reserve_expression(model.base, generator, period, "up"))
                model.security.add(block.p[generator, period] >= model.base.p[generator, period] - _reserve_expression(model.base, generator, period, "down"))
    model.OBJ = pyo.Objective(expr=model.base.OBJ.expr, sense=pyo.minimize)
    model._powersim_security_metadata = {"assets": assets, "profiles": profiles, "gas": gas, "settings": settings,
        "start_h": start_h, "dt": dt, "contingencies": contingencies, "input": inp}
    return model


def _qa(base_rows, contingency_rows: dict[str, Any], contingencies: list[dict[str, str]], network: dict) -> QAReport:
    checks=[]; lookup={c["id"]: c for c in contingencies}; bad_outage=[]; bad_delta=[]; bad_flow=[]; bad_limit=[]; bad_balance=[]; bad_commit=[]
    base_by_t={row["t"]: row for row in base_rows}
    lines={x["id"]: x for x in network.get("lines") or []}
    for cid, rows in contingency_rows.items():
        c=lookup[cid]
        for row in rows:
            b=base_by_t[row["t"]]
            if row.get("commitment") != b.get("commitment") or row.get("startup") != b.get("startup") or row.get("shutdown") != b.get("shutdown"):
                bad_commit.append({"contingency":cid,"period":row["t"]})
            if c["kind"] in {"unit_outage", "import_outage"} and abs(float(row["dispatch"].get(c["target"], 0))) > 1e-6:
                bad_outage.append({"contingency":cid,"period":row["t"]})
            if c["kind"] == "line_outage" and abs(float(row.get("line_flow", {}).get(c["target"], 0))) > 1e-6:
                bad_outage.append({"contingency":cid,"period":row["t"]})
            for g, dispatch in row["dispatch"].items():
                if c["kind"] in {"unit_outage", "import_outage"} and g == c["target"]: continue
                up=sum(float(x) for x in (b.get("reserve_up") or {}).values() for key,x in (x or {}).items() if key == g)
                down=sum(float(x) for x in (b.get("reserve_down") or {}).values() for key,x in (x or {}).items() if key == g)
                delta=float(dispatch)-float(b["dispatch"].get(g,0))
                if delta > up + 1e-6 or -delta > down + 1e-6: bad_delta.append({"contingency":cid,"period":row["t"],"unit":g,"delta":delta,"up":up,"down":down})
            for lid, line in lines.items():
                if c["kind"] == "line_outage" and lid == c["target"]: continue
                flow=float(row.get("line_flow",{}).get(lid,0)); angles=row.get("bus_angle_rad",{})
                expected=float(line["susceptance_mw_per_rad"])*(float(angles.get(line["from_bus"],0))-float(angles.get(line["to_bus"],0)))
                if abs(flow-expected)>1e-5: bad_flow.append({"contingency":cid,"period":row["t"],"line":lid})
                if abs(flow)>float(line["capacity_mw"])+1e-6: bad_limit.append({"contingency":cid,"period":row["t"],"line":lid})
            for bus in network.get("buses") or []:
                bid=bus["id"]; injection=float(row.get("bus_injection_mw",{}).get(bid,0))
                net=sum(float(row.get("line_flow",{}).get(x["id"],0)) for x in lines.values() if x["from_bus"]==bid)-sum(float(row.get("line_flow",{}).get(x["id"],0)) for x in lines.values() if x["to_bus"]==bid)
                demand_by_bus=network.get("demand_by_bus") or {}; share=(network.get("load_share_by_bus") or {}).get(bid,0)
                demand=(demand_by_bus.get(bid, [float(row["load_mw"])*float(share)])[0] if isinstance(demand_by_bus.get(bid),list) else demand_by_bus.get(bid,float(row["load_mw"])*float(share)))
                if abs(injection-net-float(demand))>1e-5: bad_balance.append({"contingency":cid,"period":row["t"],"bus":bid,"reason":"nodal_balance"})
            if float(row.get("unserved_mwh",0)) > 1e-8: bad_balance.append({"contingency":cid,"period":row["t"],"reason":"system_unserved"})
    def check(identifier, bad, message):
        checks.append(QACheckResult(check_id=identifier,status=QAStatus.FAIL if bad else QAStatus.PASS,message=message,witness={"violations":bad[:10]},checked_count=len(contingency_rows)))
    check("security.outage_applied",bad_outage,"outaged elements have zero canonical output/flow")
    check("security.commitment_consistency",bad_commit,"surviving first-stage commitment matches base")
    check("security.corrective_redispatch",bad_delta,"corrective redispatch stays within base reserve allocation")
    check("security.network_flow",bad_flow,"surviving branches satisfy DC flow equation")
    check("security.branch_limits",bad_limit,"surviving branches stay within capacity")
    check("security.nodal_balance",bad_balance,"contingency network balances without locational shortage")
    check("security.no_locational_shortage_claim",bad_balance,"security states contain no system-level shortage")
    checks.append(QACheckResult(check_id="security.extraction_complete",status=QAStatus.PASS if len(contingency_rows)==len(contingencies) else QAStatus.FAIL,message="every contingency extracted",witness={},checked_count=len(contingency_rows)))
    return QAReport(status=QAStatus.FAIL if any(x.status==QAStatus.FAIL for x in checks) else QAStatus.PASS,checks=checks)


def run_security_shared(inp: dict[str, Any]) -> dict[str, Any]:
    model=build_security_model(inp); meta=model._powersim_security_metadata; settings=meta["settings"]
    outcome=solve_model(model,str(settings.get("solver","auto")),{"time_limit_s":float(settings.get("time_limit_s",300)),"mip_gap":float(settings.get("mip_gap",.005)),"threads":int(settings.get("threads",0)),"log_to_console":False})
    diagnostics=outcome.diagnostics
    if not diagnostics.has_incumbent:
        return {"workflow":"security_constrained_uc","has_incumbent":False,"solver_diagnostics":diagnostics.model_dump(mode="json"),"result_validity":"invalid","publication":{"publishable":False,"reasons":["no_incumbent"]}}
    common=dict(assets=meta["assets"],demand_w=meta["profiles"]["demand"],profiles_w=meta["profiles"],reserve_prods=meta["input"].get("reserve_products",[]),gas_limits=meta["gas"],init_state={},solver_cfg=settings,offset_h=meta["start_h"],dt=meta["dt"],diagnostics=diagnostics,backend_used=diagnostics.backend)
    base_rows,*_=extract_window_solution(model.base,shared_session=model.base._powersim_window_metadata["shared_session"],**common)
    base_result=build_result_store(base_rows,meta["assets"],meta["input"],float(diagnostics.orchestration_runtime_s or 0),float(pyo.value(model.base.OBJ.expr)))
    contingency_rows={}; contingency_results=[]
    for cid in model.C:
        block=model.contingency[cid]; rows,*_=extract_window_solution(block,shared_session=block._powersim_window_metadata["shared_session"],**common)
        contingency_rows[str(cid)]=rows
        result=build_result_store(rows,meta["assets"],meta["input"],float(diagnostics.orchestration_runtime_s or 0),float(pyo.value(block.OBJ.expr)))
        result["diagnostics"]["solver_diagnostics"]["metadata"].update({"solve_scope":"security_extensive_form","contingency_id":str(cid)})
        contingency_results.append({"id":str(cid),"kind":next(x["kind"] for x in meta["contingencies"] if x["id"]==str(cid)),"canonical_rows":list(rows),"result":result})
    report=_qa(base_rows,contingency_rows,meta["contingencies"],settings["_network"])
    base_objective=float(((base_result.get("diagnostics") or {}).get("objective_breakdown") or {}).get("total_reconstructed"))
    scuc_objective=float(pyo.value(model.OBJ))
    objective_residual=abs(base_objective-scuc_objective)
    objective_tolerance=1e-6+1e-7*max(1.0,abs(base_objective),abs(scuc_objective))
    objective_check=QACheckResult(check_id="security.base_objective",status=QAStatus.PASS if objective_residual<=objective_tolerance else QAStatus.FAIL,
        message="SCUC economic objective equals base-case canonical objective",witness={"base_case_objective_usd":base_objective,"scuc_objective_usd":scuc_objective,"residual_usd":objective_residual},tolerance=objective_tolerance,max_violation=objective_residual)
    report=QAReport(status=QAStatus.FAIL if report.status==QAStatus.FAIL or objective_check.status==QAStatus.FAIL else QAStatus.PASS,checks=[*report.checks,objective_check])
    all_base_ok=(base_result.get("qa") or {}).get("status")=="pass"; all_cont_ok=all((x["result"].get("qa") or {}).get("status")=="pass" for x in contingency_results)
    required=("security.outage_applied","security.commitment_consistency","security.corrective_redispatch","security.network_flow","security.branch_limits","security.nodal_balance","security.no_locational_shortage_claim","security.extraction_complete","security.base_objective")
    decision=evaluate_publication(diagnostics,report,extraction_completed=len(contingency_results)==len(meta["contingencies"]) and all_base_ok and all_cont_ok,required_values_finite=True,required_check_ids=required)
    final=finalized_diagnostics(diagnostics,report.status,decision.publishable)
    return {"workflow":"security_constrained_uc","has_incumbent":True,"solver_diagnostics":final.model_dump(mode="json"),"base":{**base_result,"canonical_rows":list(base_rows)},"contingencies":contingency_results,"contingency_count":len(contingency_results),"secure":decision.publishable,"qa":report.model_dump(mode="json"),"result_validity":final.result_validity.value,"publication":{"publishable":decision.publishable,"reasons":list(decision.reasons)},"objective_usd":scuc_objective,"objective_scope":"base_case_only"}
