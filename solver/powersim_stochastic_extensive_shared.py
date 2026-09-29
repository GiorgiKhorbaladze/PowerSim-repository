"""Shared-physics stochastic extensive-form construction.

This module composes deterministic window models on scenario blocks.  It does
not duplicate deterministic physical constraints: each block is built by
``powersim_solver.solve_window(..., build_only=True)`` and its local objective
expression is probability-weighted by the extensive form.
"""
from __future__ import annotations

from typing import Any
from pathlib import Path
import sys
import pyomo.environ as pyo

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from powersim.solvers import solve_model
from powersim_solver import build_asset_map, build_gas_limits, slice_profiles, solve_window
from powersim_stochastic_shared import _scenario_input, _scenarios


def _reject_rolling(inp: dict[str, Any], horizon: int, duration_h: float) -> None:
    settings = inp.get("solver_settings") or {}
    window = float(settings.get("rolling_window_h", horizon * duration_h) or horizon * duration_h)
    step = float(settings.get("rolling_step_h", window) or window)
    if window < horizon * duration_h or step < horizon * duration_h:
        raise ValueError("validated stochastic UC rolling horizon is unsupported; use a full-horizon extensive form")


def build_extensive_form(inp: dict[str, Any]):
    """Build, but do not solve, one non-anticipative shared-physics EF."""
    scenarios = _scenarios(inp)
    projections = [_scenario_input(inp, scenario) for scenario in scenarios]
    first_assets = build_asset_map(projections[0])
    first_profiles, horizon = slice_profiles(projections[0])
    duration_h = int(projections[0].get("resolution_min", 60)) / 60
    _reject_rolling(projections[0], horizon, duration_h)
    ef = pyo.ConcreteModel()
    ef.S = pyo.Set(initialize=range(len(scenarios)), ordered=True)
    ef.scenario = pyo.Block(ef.S)
    metadata: list[dict[str, Any]] = []
    for index, scenario in enumerate(scenarios):
        projection = projections[index]
        assets = build_asset_map(projection)
        if list(assets) != list(first_assets):
            raise ValueError("stochastic scenario overlays may not add or remove assets")
        profiles, scenario_horizon = slice_profiles(projection)
        if scenario_horizon != horizon:
            raise ValueError("stochastic scenario profile horizon differs from the base horizon")
        gas = build_gas_limits(projection, int((projection.get("study_horizon") or {}).get("start_hour", 0)), horizon)
        block = ef.scenario[index]
        solve_window(assets, profiles["demand"], profiles, projection.get("reserve_products", []),
                     gas, {}, projection["solver_settings"], dt=duration_h,
                     model=block, build_only=True)
        metadata.append({"scenario": scenario, "projection": projection, "assets": assets,
                         "profiles": profiles, "gas": gas})
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


def solve_extensive_form(inp: dict[str, Any]) -> dict[str, Any]:
    """Solve one EF and expose truthful model-level diagnostics and first stage.

    Full canonical scenario extraction is intentionally not exposed from this
    low-level seam.  The production validated entrypoint must add that
    extraction and independent aggregate QA before it can publish results.
    """
    ef = build_extensive_form(inp)
    settings = inp.get("solver_settings") or {}
    outcome = solve_model(ef, str(settings.get("solver", "auto")), {
        "time_limit_s": float(settings.get("time_limit_s", 300)),
        "mip_gap": float(settings.get("mip_gap", .005)),
        "threads": int(settings.get("threads", 0)), "log_to_console": False,
    })
    diagnostics = outcome.diagnostics
    if not diagnostics.has_incumbent:
        return {"workflow": "stochastic_uc_extensive_form", "solver_diagnostics": diagnostics.model_dump(mode="json"),
                "has_incumbent": False, "publication": {"publishable": False, "reasons": ["no_incumbent"]}}
    reference = ef.scenario[0]
    first_stage = {
        "commitment": [{str(g): float(pyo.value(reference.u[g, t]) or 0) for g in reference.GC} for t in reference.T],
        "startup": [{str(g): float(pyo.value(reference.y[g, t]) or 0) for g in reference.GC} for t in reference.T],
        "shutdown": [{str(g): float(pyo.value(reference.z[g, t]) or 0) for g in reference.GC} for t in reference.T],
    }
    return {"workflow": "stochastic_uc_extensive_form", "has_incumbent": True,
            "objective_usd": float(pyo.value(ef.OBJ)), "solver_diagnostics": diagnostics.model_dump(mode="json"),
            "first_stage": first_stage,
            "publication": {"publishable": False, "reasons": ["canonical_extraction_and_qa_not_yet_integrated"]}}
