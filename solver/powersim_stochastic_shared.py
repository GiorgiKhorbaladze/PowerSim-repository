"""Validated stochastic entrypoint built on the shared deterministic workflow.

This module intentionally validates the first Stage-5A acceptance slice only:
one probability-1 scenario.  It calls the production deterministic assembler,
canonical extraction, independent QA and publication gate directly.  Multiple
scenarios are rejected until a shared-physics extensive form with explicit
non-anticipativity is available; independent scenario loops are not labelled
stochastic UC here.
"""
from __future__ import annotations

from copy import deepcopy
import math
from typing import Any

from powersim_solver import build_asset_map, build_gas_limits, build_result_store, slice_profiles, solve_all


def _scenarios(inp: dict[str, Any]) -> list[dict[str, Any]]:
    raw = inp.get("stochastic_scenarios")
    if raw is None:
        raw = (inp.get("stochastic_tree") or {}).get("scenarios")
    if not isinstance(raw, list) or not raw:
        raise ValueError("stochastic scenarios must be a non-empty list")
    seen: set[str] = set()
    scenarios = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
            raise ValueError(f"stochastic scenario at index {index} requires a non-empty id")
        sid = item["id"]
        if sid in seen:
            raise ValueError(f"duplicate stochastic scenario id: {sid}")
        seen.add(sid)
        if "probability" in item:
            probability = item["probability"]
        elif "prob" in item:
            probability = item["prob"]
        else:
            raise ValueError(f"stochastic scenario {sid} requires an explicit probability")
        try:
            probability = float(probability)
        except (TypeError, ValueError):
            raise ValueError(f"stochastic scenario {sid} probability must be numeric") from None
        if not math.isfinite(probability) or probability < 0:
            raise ValueError(f"stochastic scenario {sid} probability must be finite and non-negative")
        scenarios.append({**item, "probability": probability})
    total = sum(item["probability"] for item in scenarios)
    if total <= 0:
        raise ValueError("stochastic scenario probabilities must have a positive total")
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"stochastic scenario probabilities must sum to 1.0, got {total:g}")
    return scenarios


def _scenario_input(base: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    resolved = deepcopy(base)
    settings = dict(resolved.get("solver_settings") or {})
    if settings.get("component_engine", "shared").lower() != "shared":
        raise ValueError("validated stochastic UC requires solver_settings.component_engine='shared'")
    settings["component_engine"] = "shared"
    resolved["solver_settings"] = settings
    resolved["scenario_metadata"] = {
        "id": scenario["id"],
        "label": scenario.get("label", scenario["id"]),
        "probability": scenario["probability"],
    }
    overrides = scenario.get("profile_overrides") or {}
    if not isinstance(overrides, dict):
        raise ValueError(f"stochastic scenario {scenario['id']} profile_overrides must be an object")
    profiles = resolved.setdefault("profiles", {})
    assets = {str(asset.get("id")): asset for asset in resolved.get("assets", []) if isinstance(asset, dict)}
    for key, value in overrides.items():
        if isinstance(value, list):
            profiles[key] = value
        elif key in assets and isinstance(value, str):
            asset = assets[key]
            if asset.get("type") in {"hydro_reg", "hydro_ror"}:
                asset["inflow_profile"] = value
            elif asset.get("type") in {"wind", "solar"}:
                asset["availability_profile"] = value
            else:
                raise ValueError(f"profile override for {key} is unsupported for asset type {asset.get('type')}")
        else:
            raise ValueError(f"stochastic scenario {scenario['id']} has unsupported profile override {key}")
    return resolved


def run_stochastic_shared(inp: dict[str, Any]) -> dict[str, Any]:
    """Run the validated singleton stochastic parity path.

    This is deliberately fail-closed for more than one scenario.  Calling
    separate deterministic solves cannot enforce first-stage non-anticipativity
    and is therefore legacy compatibility behaviour, not PowerSim 1.0 SCUC.
    """
    scenarios = _scenarios(inp)
    if len(scenarios) != 1:
        raise ValueError(
            "validated shared stochastic UC currently supports exactly one scenario; "
            "multi-scenario non-anticipative extensive form is not yet available"
        )
    scenario = scenarios[0]
    resolved = _scenario_input(inp, scenario)
    assets = build_asset_map(resolved)
    profiles, horizon = slice_profiles(resolved)
    gas_limits = build_gas_limits(resolved, int((resolved.get("study_horizon") or {}).get("start_hour", 0)), horizon)
    rows, elapsed, objective = solve_all(resolved, assets, profiles, gas_limits)
    result = build_result_store(rows, assets, resolved, elapsed, objective)
    first_stage = {
        "commitment": [dict(row.get("commitment") or {}) for row in rows],
        "startup": [dict(row.get("startup") or {}) for row in rows],
        "shutdown": [dict(row.get("shutdown") or {}) for row in rows],
    }
    objective_value = float((result.get("diagnostics") or {}).get("objective_breakdown", {}).get("pyomo_objective", objective))
    scenario_publishable = bool((result.get("publication") or {}).get("publishable"))
    return {
        "workflow": "shared_deterministic_single_scenario_parity",
        "scenario_count": 1,
        "probability_metadata": {"validated": True, "normalized": False, "sum": 1.0},
        "expected_objective_usd": objective_value,
        "first_stage": first_stage,
        "scenarios": [{
            "id": scenario["id"], "probability": scenario["probability"],
            "objective_usd": objective_value, "result": result,
            "qa_status": (result.get("qa") or {}).get("status"),
            "publishable": scenario_publishable,
        }],
        "qa_status": (result.get("qa") or {}).get("status"),
        "publication": {"publishable": scenario_publishable,
                        "reasons": [] if scenario_publishable else ["mandatory_scenario_not_publishable"]},
        "legacy_stochastic_paths": {
            "powersim_solver.run_stochastic": "legacy_compatibility_only_independent_scenario_loop",
            "powersim_stochastic.run_stochastic_2stage": "legacy_compatibility_only_consensus_heuristic",
            "powersim_stochastic_efs.solve_efs": "deprecated_duplicate_physics",
        },
    }
