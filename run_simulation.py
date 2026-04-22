#!/usr/bin/env python3
"""
PowerSim canonical runner
=========================
Build input from project data, validate against schema, run solver, and
validate outputs. Designed for local runs, Colab, and CI jobs.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
for p in (HERE, HERE / "schema", HERE / "solver"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

build_input_from_project = None
summary_report = None
validate_input = None
validate_output = None


def _missing_runtime_deps() -> list[str]:
    required = ["pyomo", "highspy", "pandas", "numpy", "openpyxl", "xlsxwriter"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))


def _solver_path() -> Path:
    candidate = HERE / "powersim_solver.py"
    if candidate.exists():
        return candidate
    return HERE / "solver" / "powersim_solver.py"


def _config_path(path_arg: str) -> Path:
    p = Path(path_arg)
    if p.exists():
        return p
    alt = HERE / "tests" / p.name
    if alt.exists():
        return alt
    return p


def run_pipeline(project_dir: Path, config_path: Path, out_dir: Path,
                 horizon_hours: int | None = None, start_hour: int | None = None) -> int:
    cfg = _load_json(config_path)

    sh = dict(cfg.get("study_horizon", {"start_hour": 0, "horizon_hours": 168, "mode": "full"}))
    if horizon_hours is not None:
        sh["horizon_hours"] = int(horizon_hours)
    if start_hour is not None:
        sh["start_hour"] = int(start_hour)

    input_json = out_dir / "powersim_input.json"
    results_json = out_dir / "powersim_results.json"
    results_xlsx = out_dir / "powersim_results.xlsx"

    t0 = time.time()
    inp = build_input_from_project(
        project_dir,
        assets=cfg["assets"],
        hydro_zone_map=cfg.get("hydro_zone_map", {}),
        re_site_map=cfg.get("re_site_map", {}),
        reserve_products=cfg.get("reserve_products", []),
        gas_constraints=cfg.get("gas_constraints", {}),
        study_horizon=sh,
        scenario=cfg.get("scenario", "A_mean"),
        annual_twh=cfg.get("annual_twh", 15.621),
        demand_mode=cfg.get("demand_mode", "shape_times_annual"),
        solver_settings=cfg.get("solver_settings"),
        hydro_inflow_unit=cfg.get("hydro_inflow_unit", "raw"),
        scenario_metadata=cfg.get("scenario_metadata"),
        study_year=cfg.get("study_year", 2026),
    )
    _save_json(input_json, inp)

    ok_in, errs_in, warns_in = validate_input(inp)
    if not ok_in:
        print("[run] validate_input failed")
        for e in errs_in:
            print(f"  ERROR: {e}")
        return 2
    if warns_in:
        print(f"[run] validate_input warnings: {len(warns_in)}")

    print(summary_report(inp))

    solver = _solver_path()
    cmd = [
        sys.executable, str(solver),
        "--input", str(input_json),
        "--output", str(results_json),
        "--excel", str(results_xlsx),
    ]
    print("[run] " + " ".join(cmd))
    proc = subprocess.run(cmd, text=True)
    if proc.returncode != 0:
        print(f"[run] solver failed with code {proc.returncode}")
        return proc.returncode

    out = _load_json(results_json)
    ok_out, errs_out, warns_out = validate_output(out)
    if not ok_out:
        print("[run] validate_output failed")
        for e in errs_out:
            print(f"  ERROR: {e}")
        return 3

    print(f"[run] done in {time.time() - t0:.1f}s")
    print(f"[run] results: {results_json}")
    print(f"[run] excel:   {results_xlsx}")
    if warns_out:
        print(f"[run] validate_output warnings: {len(warns_out)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run a PowerSim simulation pipeline")
    ap.add_argument("--project-dir", default=os.environ.get("POWERSIM_PROJECT_DIR", "/mnt/project"),
                    help="Data directory containing hydro/renewables/xlsx source files")
    ap.add_argument("--config", default="stage1_smoke_fleet.json",
                    help="Path to run config JSON")
    ap.add_argument("--out-dir", default="out/run",
                    help="Output directory (input/results json + excel)")
    ap.add_argument("--horizon-hours", type=int, default=None,
                    help="Override study_horizon.horizon_hours from config")
    ap.add_argument("--start-hour", type=int, default=None,
                    help="Override study_horizon.start_hour from config")
    args = ap.parse_args(argv)
    missing = _missing_runtime_deps()
    if missing:
        print("[run] missing python dependencies:", ", ".join(missing))
        print("[run] install with: python3 -m pip install -r requirements.txt")
        return 4

    global build_input_from_project, summary_report, validate_input, validate_output
    from powersim_dataio import build_input_from_project as _build_input_from_project
    from powersim_dataio import summary_report as _summary_report
    from powersim_schema import validate_input as _validate_input
    from powersim_schema import validate_output as _validate_output
    build_input_from_project = _build_input_from_project
    summary_report = _summary_report
    validate_input = _validate_input
    validate_output = _validate_output

    project_dir = Path(args.project_dir)
    config_path = _config_path(args.config)
    out_dir = Path(args.out_dir)

    if not project_dir.is_dir():
        print("[run] project dir not found:", project_dir)
        print("      Use --project-dir /path/to/project_data or POWERSIM_PROJECT_DIR.")
        return 1
    if not config_path.is_file():
        print("[run] config not found:", config_path)
        return 1

    return run_pipeline(
        project_dir=project_dir,
        config_path=config_path,
        out_dir=out_dir,
        horizon_hours=args.horizon_hours,
        start_hour=args.start_hour,
    )


if __name__ == "__main__":
    raise SystemExit(main())
