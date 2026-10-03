"""Shared-physics stochastic extensive-form construction.

This module composes deterministic window models on scenario blocks.  It does
not duplicate deterministic physical constraints: each block is built by
``powersim_solver.solve_window(..., build_only=True)`` and its local objective
expression is probability-weighted by the extensive form.
"""
from __future__ import annotations

from typing import Any
import math
from pathlib import Path
import sys
import pyomo.environ as pyo

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from powersim.solvers import solve_model
from powersim.contracts import QACheckResult, QAReport, QAStatus
from powersim.results import evaluate_publication, finalized_diagnostics
if __package__:  # Installed package path.
    from .powersim_solver import (
        build_asset_map, build_gas_limits, build_result_store, extract_window_solution,
        normalize_dc_network, slice_profiles, solve_window,
    )
    from .powersim_stochastic_shared import _scenario_input, _scenarios
else:  # Compatibility for direct historical script execution.
    from powersim_solver import (
        build_asset_map, build_gas_limits, build_result_store, extract_window_solution,
        normalize_dc_network, slice_profiles, solve_window,
    )
    from powersim_stochastic_shared import _scenario_input, _scenarios


def _reject_rolling(inp: dict[str, Any], horizon: int, duration_h: float) -> None:
    settings = inp.get("solver_settings") or {}
    window = float(settings.get("rolling_window_h", horizon * duration_h) or horizon * duration_h)
    step = float(settings.get("rolling_step_h", window) or window)
    if window < horizon * duration_h or step < horizon * duration_h:
        raise ValueError("validated stochastic UC rolling horizon is unsupported; use a full-horizon extensive form")


def _validate_projection(projection: dict[str, Any], *, scenario_id: str,
                         expected_assets: list[str] | None = None,
                         expected_horizon: int | None = None,
                         expected_resolution: int | None = None) -> tuple[dict, int]:
    """Fail closed before EF assembly - no fallback from a bad overlay."""
    assets = build_asset_map(projection)
    if expected_assets is not None and list(assets) != expected_assets:
        raise ValueError("stochastic scenario overlays may not add or remove assets")
    profiles, horizon = slice_profiles(projection)
    resolution = int(projection.get("resolution_min", 60))
    if expected_horizon is not None and horizon != expected_horizon:
        raise ValueError("stochastic scenario profile horizon differs from the base horizon")
    if expected_resolution is not None and resolution != expected_resolution:
        raise ValueError("stochastic scenario resolution differs from the base resolution")
    if not isinstance(profiles.get("demand"), list) or len(profiles["demand"]) < horizon:
        raise ValueError(f"stochastic scenario {scenario_id} demand profile does not cover the solved horizon")
    for name, values in profiles.items():
        if not isinstance(values, list) or len(values) < horizon:
            raise ValueError(f"stochastic scenario {scenario_id} profile {name} does not cover the solved horizon")
        try:
            finite = all(math.isfinite(float(value)) for value in values[:horizon])
        except (TypeError, ValueError):
            finite = False
        if not finite:
            raise ValueError(f"stochastic scenario {scenario_id} profile {name} contains non-finite data")
    for asset_id, asset in assets.items():
        for field in ("availability_profile", "inflow_profile", "pmax_profile"):
            reference = asset.get(field)
            if isinstance(reference, str) and reference not in profiles:
                raise ValueError(f"stochastic scenario {scenario_id} asset {asset_id} references unknown profile {reference}")
    return profiles, horizon


def build_extensive_form(inp: dict[str, Any]):
    """Build, but do not solve, one non-anticipative shared-physics EF."""
    scenarios = _scenarios(inp)
    projections = [_scenario_input(inp, scenario) for scenario in scenarios]
    first_assets = build_asset_map(projections[0])
    first_profiles, horizon = _validate_projection(projections[0], scenario_id=scenarios[0]["id"])
    duration_h = int(projections[0].get("resolution_min", 60)) / 60
    _reject_rolling(projections[0], horizon, duration_h)
    ef = pyo.ConcreteModel()
    ef.S = pyo.Set(initialize=range(len(scenarios)), ordered=True)
    ef.scenario = pyo.Block(ef.S)
    metadata: list[dict[str, Any]] = []
    for index, scenario in enumerate(scenarios):
        projection = projections[index]
        assets = build_asset_map(projection)
        profiles, scenario_horizon = _validate_projection(
            projection, scenario_id=scenario["id"], expected_assets=list(first_assets),
            expected_horizon=horizon, expected_resolution=int(projections[0].get("resolution_min", 60)),
        )
        gas = build_gas_limits(projection, int((projection.get("study_horizon") or {}).get("start_hour", 0)), horizon)
        solver_cfg = dict(projection["solver_settings"])
        solver_cfg["_co2_price_usd_per_t"] = float(projection.get("co2_price_usd_per_t", 0) or 0)
        solver_cfg["_network"] = normalize_dc_network(projection, assets)
        block = ef.scenario[index]
        solve_window(assets, profiles["demand"], profiles, projection.get("reserve_products", []),
                     gas, {}, solver_cfg, dt=duration_h,
                     model=block, build_only=True)
        metadata.append({"scenario": scenario, "projection": projection, "assets": assets,
                         "profiles": profiles, "gas": gas, "solver_cfg": solver_cfg})
    ef.non_anticipativity = pyo.ConstraintList()
    reference = ef.scenario[0]
    for index in list(ef.S)[1:]:
        block = ef.scenario[index]
        if list(block.GC) != list(reference.GC) or list(block.T) != list(reference.T):
            raise ValueError("stochastic scenarios have incompatible first-stage index sets")
        for generator in reference.GC:
            for period in reference.T:
                ef.non_anticipativity.add(block.u[generator, period] == reference.u[generator, period])
                ef.non_anticipativity.add(block.y[generator, period] == reference.y[generator, period])
                ef.non_anticipativity.add(block.z[generator, period] == reference.z[generator, period])
                if hasattr(reference, "y_hot") and generator in reference.MSStart:
                    ef.non_anticipativity.add(block.y_hot[generator, period] == reference.y_hot[generator, period])
    ef.OBJ = pyo.Objective(
        expr=sum(float(scenarios[index]["probability"]) * ef.scenario[index].OBJ.expr for index in ef.S),
        sense=pyo.minimize,
    )
    ef._powersim_stochastic_metadata = metadata
    return ef


def _first_stage(rows) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        for unit, commitment in (row.get("commitment") or {}).items():
            records.append({"period": row["t"], "unit": unit, "commitment": commitment,
                            "startup": (row.get("startup") or {}).get(unit, 0),
                            "shutdown": (row.get("shutdown") or {}).get(unit, 0),
                            "startup_hot": (row.get("startup_hot") or {}).get(unit, 0)})
    return records


def _aggregate_qa(scenarios: list[dict[str, Any]], diagnostics, expected_objective: float) -> QAReport:
    checks: list[QACheckResult] = []
    checks.append(QACheckResult(check_id="stochastic.probability_distribution", status=QAStatus.PASS,
        message="strict positive scenario probabilities sum to one", checked_count=len(scenarios)))
    reference = {(r["period"], r["unit"]): (r["commitment"], r["startup"], r["shutdown"], r["startup_hot"])
                 for r in scenarios[0].get("first_stage", [])} if scenarios else {}
    mismatches = []
    for scenario in scenarios[1:]:
        current = {(r["period"], r["unit"]): (r["commitment"], r["startup"], r["shutdown"], r["startup_hot"])
                   for r in scenario.get("first_stage", [])}
        for key, value in reference.items():
            if key not in current or any(abs(float(a) - float(b)) > 1e-6 for a, b in zip(value, current[key])):
                mismatches.append({"scenario": scenario["id"], "period": key[0], "unit": key[1]})
    checks.append(QACheckResult(check_id="stochastic.non_anticipativity", status=QAStatus.FAIL if mismatches else QAStatus.PASS,
        message="extracted first-stage schedules agree" if not mismatches else "extracted first-stage schedules differ",
        witness={"mismatches": mismatches[:10]}, checked_count=len(reference) * max(0, len(scenarios) - 1)))
    reconstructed = sum(float(item["probability"]) * float(item["objective_usd"]) for item in scenarios)
    residual = abs(expected_objective - reconstructed)
    tolerance = 1e-6 + 1e-7 * max(1.0, abs(expected_objective), abs(reconstructed))
    checks.append(QACheckResult(check_id="stochastic.expected_objective", status=QAStatus.PASS if residual <= tolerance else QAStatus.FAIL,
        message="expected objective closes to the EF incumbent" if residual <= tolerance else "expected objective does not close to the EF incumbent",
        witness={"expected_reconstructed_objective_usd": reconstructed, "ef_incumbent_objective_usd": expected_objective,
                 "residual_usd": residual}, tolerance=tolerance, max_violation=residual))
    bad = [item["id"] for item in scenarios if item.get("qa_status") != "pass"]
    checks.append(QACheckResult(check_id="stochastic.scenario_validity", status=QAStatus.FAIL if bad else QAStatus.PASS,
        message="all scenario physical QA reports pass" if not bad else "one or more scenario physical QA reports failed",
        witness={"failed_scenarios": bad}, checked_count=len(scenarios)))
    incomplete = [item["id"] for item in scenarios if not item.get("extraction_completed")]
    checks.append(QACheckResult(check_id="stochastic.canonical_extraction_complete", status=QAStatus.FAIL if incomplete else QAStatus.PASS,
        message="every declared scenario has canonical extracted results" if not incomplete else "one or more scenarios were not extracted",
        witness={"incomplete_scenarios": incomplete}, checked_count=len(scenarios)))
    status = QAStatus.FAIL if any(check.status == QAStatus.FAIL for check in checks) else QAStatus.PASS
    return QAReport(status=status, checks=checks)


def solve_extensive_form(inp: dict[str, Any]) -> dict[str, Any]:
    """Solve once, extract each EF block, then apply physical and aggregate QA."""
    ef = build_extensive_form(inp)
    settings = inp.get("solver_settings") or {}
    outcome = solve_model(ef, str(settings.get("solver", "auto")), {
        "time_limit_s": float(settings.get("time_limit_s", 300)),
        "mip_gap": float(settings.get("mip_gap", .005)),
        "threads": int(settings.get("threads", 0)), "log_to_console": False,
    })
    diagnostics = outcome.diagnostics
    if not diagnostics.has_incumbent:
        report = QAReport(status=QAStatus.FAIL, checks=[QACheckResult(
            check_id="stochastic.canonical_extraction_complete", status=QAStatus.FAIL,
            message="no incumbent exists to extract", witness={})])
        decision = evaluate_publication(diagnostics, report, extraction_completed=False,
                                        required_values_finite=False,
                                        required_check_ids=("stochastic.canonical_extraction_complete",))
        return {"workflow": "stochastic_uc", "has_incumbent": False,
                "solver_diagnostics": diagnostics.model_dump(mode="json"), "scenarios": [],
                "qa": report.model_dump(mode="json"), "result_validity": "invalid",
                "publication": {"publishable": False, "reasons": list(decision.reasons)}}
    scenario_outputs = []
    for index, meta in enumerate(ef._powersim_stochastic_metadata):
        block = ef.scenario[index]
        rows, _state, _wall, local_objective = extract_window_solution(
            block, assets=meta["assets"], demand_w=meta["profiles"]["demand"], profiles_w=meta["profiles"],
            reserve_prods=meta["projection"].get("reserve_products", []), gas_limits=meta["gas"], init_state={},
            solver_cfg=meta["solver_cfg"], offset_h=int((meta["projection"].get("study_horizon") or {}).get("start_hour", 0)),
            dt=int(meta["projection"].get("resolution_min", 60)) / 60, diagnostics=diagnostics,
            backend_used=diagnostics.backend, shared_session=getattr(block, "_powersim_window_metadata")["shared_session"],
        )
        result = build_result_store(rows, meta["assets"], meta["projection"],
                                    float(diagnostics.orchestration_runtime_s or 0.0), local_objective)
        inherited = ((result.get("diagnostics") or {}).get("solver_diagnostics") or {})
        inherited.setdefault("metadata", {}).update({"solve_scope": "extensive_form", "scenario_id": meta["scenario"]["id"]})
        first_stage = _first_stage(rows)
        objective = float(((result.get("diagnostics") or {}).get("objective_breakdown") or {}).get("total_reconstructed"))
        scenario_outputs.append({"id": meta["scenario"]["id"], "label": meta["scenario"].get("label", meta["scenario"]["id"]),
            "probability": float(meta["scenario"]["probability"]), "objective_usd": objective,
            "weighted_objective_contribution_usd": 0.0, "result": result, "first_stage": first_stage,
            "qa_status": (result.get("qa") or {}).get("status"), "result_validity": (result.get("diagnostics") or {}).get("result_validity"),
            "extraction_completed": bool(rows.extraction_completed)})
    for item in scenario_outputs:
        item["weighted_objective_contribution_usd"] = item["probability"] * item["objective_usd"]
    expected = float(pyo.value(ef.OBJ))
    report = _aggregate_qa(scenario_outputs, diagnostics, expected)
    finite = math.isfinite(expected) and all(math.isfinite(float(item["objective_usd"])) for item in scenario_outputs)
    required = ("stochastic.probability_distribution", "stochastic.non_anticipativity", "stochastic.expected_objective",
                "stochastic.scenario_validity", "stochastic.canonical_extraction_complete")
    decision = evaluate_publication(diagnostics, report, extraction_completed=len(scenario_outputs) == len(ef.S),
                                    required_values_finite=finite, required_check_ids=required)
    final_diagnostics = finalized_diagnostics(diagnostics, report.status, decision.publishable)
    return {"workflow": "stochastic_uc", "has_incumbent": True,
            "solver_diagnostics": final_diagnostics.model_dump(mode="json"),
            "probability_metadata": {"validated": True, "normalized": False,
                                     "sum": sum(item["probability"] for item in scenario_outputs)},
            "first_stage": scenario_outputs[0]["first_stage"], "scenarios": scenario_outputs,
            "aggregate": {"expected_objective_usd": expected, "qa": report.model_dump(mode="json"),
                          "result_validity": final_diagnostics.result_validity.value},
            "qa": report.model_dump(mode="json"), "result_validity": final_diagnostics.result_validity.value,
            "publication": {"publishable": decision.publishable, "reasons": list(decision.reasons)},
            "legacy_stochastic_paths": {"powersim_solver.run_stochastic": "legacy_compatibility_only_independent_scenario_loop",
                "powersim_stochastic.run_stochastic_2stage": "legacy_compatibility_only_consensus_heuristic",
                "powersim_stochastic_efs.solve_efs": "deprecated_duplicate_physics"}}
