"""Safe PowerSim AI tool registry and implementations."""
from __future__ import annotations

import copy
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

from backend.powersim_ai_config import DEFAULT_TIMEOUT_SECONDS, REPO_ROOT, resolve_allowed_path

DANGEROUS_PATH_PARTS = {"__class__", "__dict__", "__globals__", "env", "environment", "secrets"}
MUTATING_TOOLS = {"run_solver", "run_adequacy"}


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _asset_id(asset: dict[str, Any]) -> str | None:
    return asset.get("id") or asset.get("name") or asset.get("unit_id")


def _get_path(data: dict[str, Any], paths: list[tuple[str, ...]]) -> Any:
    for path in paths:
        cur: Any = data
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                cur = None
                break
            cur = cur[part]
        if cur is not None:
            return cur
    return None


def _first_present(d: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    """Return `_num(d[k])` for the first `k` present in `d` — even if the
    value is 0. Using `d.get(a) or d.get(b)` silently drops legitimate
    zero values because 0.0 is falsy."""
    for k in keys:
        if k in d:
            return _num(d[k])
    return None


def load_input_summary(input_json: dict[str, Any]) -> dict[str, Any]:
    assets = input_json.get("assets") or []
    by_type: dict[str, int] = {}
    for asset in assets:
        typ = str(asset.get("type") or asset.get("technology") or asset.get("fuel") or "unknown")
        by_type[typ] = by_type.get(typ, 0) + 1
    demand = ((input_json.get("profiles") or {}).get("demand") or input_json.get("demand") or [])
    demand_peak = max((_num(x) or 0 for x in demand), default=None) if isinstance(demand, list) else None
    horizon = input_json.get("study_horizon") or {}
    return {
        "study_year": (input_json.get("metadata") or {}).get("study_year"),
        "horizon": horizon,
        "assets_by_type": by_type,
        "asset_count": len(assets),
        "demand_peak": demand_peak,
        "reserve_products": input_json.get("reserve_products") or [],
        "gas_mode": (input_json.get("gas_constraints") or {}).get("mode"),
        "scenario_id": (input_json.get("profile_bundle") or {}).get("scenario_id") or (input_json.get("scenario_metadata") or {}).get("scenario") or (input_json.get("metadata") or {}).get("scenario"),
    }


def summarize_results(results_json: dict[str, Any]) -> dict[str, Any]:
    sys_sum = results_json.get("system_summary") or {}
    hourly = results_json.get("hourly_system") or []
    # The solver emits `by_unit_summary` as a dict keyed by asset id, not
    # a list — normalise before iterating so the AI summariser works on
    # real solver outputs. Fallback keeps list-style inputs working too.
    units_raw = results_json.get("by_unit_summary") or []
    if isinstance(units_raw, dict):
        units = [
            {"id": gid, **(u or {})} for gid, u in units_raw.items()
            if isinstance(u, dict)
        ]
    else:
        units = [u for u in units_raw if isinstance(u, dict)]
    # Row-level marginal price key in solver output is `lambda_usd_mwh`.
    lambdas = [_num(
        (h or {}).get("lambda_usd_mwh")
        or (h or {}).get("lambda")
        or (h or {}).get("price")
        or (h or {}).get("marginal_cost")
    ) for h in hourly if isinstance(h, dict)]
    lambdas = [x for x in lambdas if x is not None]
    top_gen = sorted(units, key=lambda u: _num(
        u.get("energy_mwh") or u.get("generation_mwh") or u.get("gen_mwh")
    ) or 0, reverse=True)[:5]
    top_cost = sorted(units, key=lambda u: _num(
        u.get("gross_cost") or u.get("cost") or u.get("total_cost")
    ) or 0, reverse=True)[:5]
    bess = [u for u in units
            if str(u.get("type") or u.get("technology") or "").lower()
               in {"bess", "battery", "storage"}]
    return {
        # The solver emits *_usd suffixed keys in system_summary; keep the
        # legacy unsuffixed keys as fallbacks for older result files.
        "total_cost": _get_path(results_json, [
            ("system_summary", "total_cost_usd"),
            ("system_summary", "total_cost"),
            ("diagnostics", "total_cost"),
        ]),
        "total_objective_cost": _get_path(results_json, [
            ("system_summary", "total_objective_cost_usd"),
            ("system_summary", "objective_cost"),
            ("diagnostics", "objective_cost"),
            ("diagnostics", "objective_breakdown", "total_reconstructed"),
            ("metadata", "objective_cost"),
        ]),
        # `or` short-circuits on 0.0 so a legitimate zero-unserved run
        # would report None; walk the fallback list explicitly and stop
        # on the first key present.
        "avg_lambda": (sum(lambdas) / len(lambdas)) if lambdas else _first_present(
            sys_sum, ("avg_lambda_usd_mwh", "avg_lambda")
        ),
        "gas":         _first_present(sys_sum, ("total_gas_mm3", "gas",
                                                "gas_used", "gas_mmbtu")),
        "unserved":    _first_present(sys_sum, ("total_unserved_mwh",
                                                "unserved", "unserved_mwh")),
        "curtailment": _first_present(sys_sum, ("total_curtailed_mwh",
                                                "curtailment", "curtailment_mwh")),
        "top_units_by_generation": top_gen,
        "top_units_by_cost": top_cost,
        "bess_metrics": bess,
    }


def compare_results(base_results: dict[str, Any], candidate_results: dict[str, Any]) -> dict[str, Any]:
    b, c = summarize_results(base_results), summarize_results(candidate_results)
    keys = ["total_cost", "total_objective_cost", "gas", "unserved", "curtailment", "avg_lambda"]
    deltas = {k: ((_num(c.get(k)) or 0) - (_num(b.get(k)) or 0)) for k in keys if b.get(k) is not None or c.get(k) is not None}
    for k in ("bess_usage", "hydro_spill", "reserve_shortfall"):
        bv = _get_path(base_results, [("system_summary", k), ("diagnostics", k)])
        cv = _get_path(candidate_results, [("system_summary", k), ("diagnostics", k)])
        if bv is not None or cv is not None:
            deltas[k] = (_num(cv) or 0) - (_num(bv) or 0)
    return {"base": b, "candidate": c, "deltas": deltas}


def propose_add_bess(input_json: dict[str, Any], id: str, power_mw: float, energy_mwh: float, bus: str | None = None) -> dict[str, Any]:
    patched = copy.deepcopy(input_json)
    if any(_asset_id(a) == id for a in patched.get("assets", [])):
        raise ValueError(f"asset_id already exists: {id}")
    asset = {"id": id, "type": "bess", "power_mw": power_mw, "energy_mwh": energy_mwh}
    if bus:
        asset["bus"] = bus
    patched.setdefault("assets", []).append(asset)
    return {"patched_input": patched, "summary": f"Add BESS {id}: {power_mw} MW / {energy_mwh} MWh"}


def _validate_field_path(field_path: str) -> list[str]:
    parts = field_path.split(".")
    if not parts or any(not p or p in DANGEROUS_PATH_PARTS or p.startswith("_") for p in parts):
        raise ValueError("dangerous field_path rejected")
    if parts[0] in {"metadata", "profiles", "profile_bundle", "time_index"}:
        raise ValueError("field_path is not editable by chat tools")
    return parts


def propose_edit_asset(input_json: dict[str, Any], asset_id: str, field_path: str, value: Any) -> dict[str, Any]:
    parts = _validate_field_path(field_path)
    patched = copy.deepcopy(input_json)
    assets = patched.get("assets") or []
    target = next((a for a in assets if _asset_id(a) == asset_id), None)
    if target is None:
        raise ValueError(f"asset_id not found: {asset_id}")
    cur = target
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
        if not isinstance(cur, dict):
            raise ValueError("field_path traverses a non-object value")
    old = cur.get(parts[-1])
    cur[parts[-1]] = value
    return {"patched_input": patched, "summary": f"Edit {asset_id}.{field_path}: {old!r} -> {value!r}", "changes": [{"asset_id": asset_id, "field_path": field_path, "old": old, "new": value}]}


def propose_set_horizon(input_json: dict[str, Any], hours: int, start_hour: int = 0) -> dict[str, Any]:
    if hours <= 0 or start_hour < 0:
        raise ValueError("hours must be positive and start_hour non-negative")
    patched = copy.deepcopy(input_json)
    patched["study_horizon"] = {**(patched.get("study_horizon") or {}), "horizon_hours": hours, "start_hour": start_hour}
    return {"patched_input": patched, "summary": f"Set horizon to {hours} hours starting at {start_hour}"}


def _completed(command: list[str], timeout: int) -> dict[str, Any]:
    if not command or command[0] != sys.executable:
        raise ValueError("arbitrary command execution rejected")
    proc = subprocess.run(command, cwd=REPO_ROOT, shell=False, text=True, capture_output=True, timeout=timeout, check=False)
    return {"command": command, "returncode": proc.returncode, "stdout": proc.stdout[-8000:], "stderr": proc.stderr[-8000:]}


def run_solver(input_path: str, out_dir: str, hours: int | None = None, scenario: str | None = None, *, confirmed: bool = False, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    if not confirmed:
        return {"requires_confirmation": True, "message": "Solver runs require explicit confirmation."}
    inp = resolve_allowed_path(input_path)
    out = resolve_allowed_path(out_dir, must_be_under=(REPO_ROOT / ".runtime", REPO_ROOT / "out"))
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "scripts/run_horizon.py", "--project-dir", "project_data", "--config", str(inp), "--out-dir", str(out)]
    if hours is not None:
        cmd += ["--hours", str(hours)]
    if scenario:
        cmd += ["--scenario", scenario]
    return _completed(cmd, timeout)


def run_adequacy(input_path: str, out_dir: str, mode: str = "deterministic_derated", *, confirmed: bool = False, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    if not confirmed:
        return {"requires_confirmation": True, "message": "Adequacy runs require explicit confirmation."}
    inp = resolve_allowed_path(input_path)
    out = resolve_allowed_path(out_dir, must_be_under=(REPO_ROOT / ".runtime", REPO_ROOT / "out"))
    out.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "scripts/run_adequacy.py", "--input", str(inp), "--out-dir", str(out), "--mode", mode]
    return _completed(cmd, timeout)


def export_report_summary(results_json: dict[str, Any]) -> str:
    s = summarize_results(results_json)
    return "\n".join(["# PowerSim Results Summary", f"- Total cost: {s.get('total_cost')}", f"- Objective cost: {s.get('total_objective_cost')}", f"- Avg lambda: {s.get('avg_lambda')}", f"- Unserved energy: {s.get('unserved')}", f"- Curtailment: {s.get('curtailment')}"])

TOOL_REGISTRY = {
    "load_input_summary": load_input_summary,
    "summarize_results": summarize_results,
    "compare_results": compare_results,
    "propose_add_bess": propose_add_bess,
    "propose_edit_asset": propose_edit_asset,
    "propose_set_horizon": propose_set_horizon,
    "run_solver": run_solver,
    "run_adequacy": run_adequacy,
    "export_report_summary": export_report_summary,
}

def make_action(tool_name: str, title: str, description: str, tool_args: dict[str, Any], risk_level: str = "medium") -> dict[str, Any]:
    return {"action_id": str(uuid.uuid4()), "title": title, "description": description, "changes": [], "risk_level": risk_level, "requires_confirmation": True, "tool_name": tool_name, "tool_args": tool_args}
