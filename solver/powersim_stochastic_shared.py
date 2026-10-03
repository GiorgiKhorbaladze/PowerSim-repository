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

if __package__:  # Installed package path.
    from .powersim_solver import build_asset_map, build_gas_limits, build_result_store, slice_profiles, solve_all
else:  # Compatibility for direct historical script execution.
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
        if not math.isfinite(probability) or probability <= 0:
            raise ValueError(f"stochastic scenario {sid} probability must be finite and strictly positive")
        scenarios.append({**item, "probability": probability})
    total = sum(item["probability"] for item in scenarios)
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
    horizon_h = int((resolved.get("study_horizon") or {}).get("horizon_hours", 0) or 0)
    expected_periods = horizon_h * (60 // int(resolved.get("resolution_min", 60)))
    assets = {str(asset.get("id")): asset for asset in resolved.get("assets", []) if isinstance(asset, dict)}
    for key, value in overrides.items():
        if isinstance(value, list):
            if key not in profiles:
                raise ValueError(f"stochastic scenario {scenario['id']} references unknown profile {key}")
            try:
                valid_values = all(math.isfinite(float(item)) for item in value)
            except (TypeError, ValueError):
                valid_values = False
            if not valid_values:
                raise ValueError(f"stochastic scenario {scenario['id']} profile override {key} contains non-finite data")
            if expected_periods and len(value) < expected_periods:
                raise ValueError(f"stochastic scenario {scenario['id']} profile override {key} does not cover the solved horizon")
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
    if not isinstance(profiles.get("demand"), list):
        raise ValueError(f"stochastic scenario {scenario['id']} requires a demand profile")
    return resolved


def run_stochastic_shared(inp: dict[str, Any]) -> dict[str, Any]:
    """Run the sole validated shared-physics stochastic UC entrypoint."""
    # Import lazily because the EF builder imports this module's strict input
    # contract helpers. This is one joint solve, never a scenario loop.
    from powersim_stochastic_extensive_shared import solve_extensive_form
    return solve_extensive_form(inp)
