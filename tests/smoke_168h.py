"""
PowerSim v4.0 — Stage 1 Smoke Test
===================================
End-to-end verification that the full Stage 1 pipeline runs:

    dataio.build_input_from_project
       └→ INPUT JSON validates against schema v1.1
    solver.main (UC/ED, 168h, 8 assets)
       └→ RESULTS JSON validates against OUTPUT_SCHEMA v1.1
    simulated HTML importResults() structural check
       └→ pass

Usage:
    python smoke_168h.py
        --project-dir /mnt/project
        --config      stage1_smoke_fleet.json
        [--keep-outputs  out/smoke_168h/]

Exits 0 on success, non-zero on any failure.  Suitable for CI.
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


# ── Path setup — make schema/ and solver/ importable whether we run from
# the repo root or from tests/. ───────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _missing_runtime_deps() -> list[str]:
    required = ["pyomo", "highspy", "pandas", "numpy", "openpyxl", "xlsxwriter"]
    return [m for m in required if importlib.util.find_spec(m) is None]


_missing = _missing_runtime_deps()
if _missing:
    print("❌ SMOKE FAILED — missing python dependencies: " + ", ".join(_missing))
    print("   Install with: python3 -m pip install -r requirements.txt")
    sys.exit(1)

from powersim_schema import validate_input, validate_output            # noqa: E402
from powersim_dataio import (                                           # noqa: E402
    build_input_from_project, summary_report, LOADER_VERSION,
)


def _resolve_repo_file(*parts: str) -> Path:
    """
    Resolve file paths for both historical tree layout and flat repo layout.
    Tries:
      1) <repo_root>/<parts...>            (historical: solver/, tests/, ...)
      2) <script_dir>/<parts...> basename  (flat: files at repository root)
    """
    preferred = ROOT.joinpath(*parts)
    if preferred.exists():
        return preferred
    fallback = HERE / Path(parts[-1])
    if fallback.exists():
        return fallback
    return preferred


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def hr(title: str) -> None:
    print("\n" + "═" * 72)
    print(f"   {title}")
    print("═" * 72)


def fail(msg: str) -> None:
    print(f"\n❌ SMOKE FAILED — {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"✅ {msg}")


# ──────────────────────────────────────────────────────────────────────
# Individual stages
# ──────────────────────────────────────────────────────────────────────
def stage_dataio(project_dir: Path, config_path: Path, out_path: Path) -> dict:
    hr("STAGE 1 / 4  —  dataio.build_input_from_project")
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    inp = build_input_from_project(
        project_dir,
        assets            = cfg["assets"],
        hydro_zone_map    = cfg.get("hydro_zone_map", {}),
        re_site_map       = cfg.get("re_site_map", {}),
        reserve_products  = cfg.get("reserve_products", []),
        gas_constraints   = cfg.get("gas_constraints", {}),
        study_horizon     = cfg.get("study_horizon",
                                    {"start_hour":0,"horizon_hours":168,"mode":"full"}),
        scenario          = cfg.get("scenario", "A_mean"),
        annual_twh        = cfg.get("annual_twh", 15.621),
        demand_mode       = cfg.get("demand_mode", "shape_times_annual"),
        solver_settings   = cfg.get("solver_settings"),
        hydro_inflow_unit = cfg.get("hydro_inflow_unit", "raw"),
        scenario_metadata = cfg.get("scenario_metadata"),
        study_year        = cfg.get("study_year", 2026),
    )
    out_path.write_text(json.dumps(inp, ensure_ascii=False, separators=(",", ":")),
                        encoding="utf-8")
    print(summary_report(inp))
    ok(f"wrote input JSON → {out_path}  ({out_path.stat().st_size/1024:.1f} KiB)")
    return inp


def stage_validate_input(inp: dict) -> None:
    hr("STAGE 2 / 4  —  validate_input (schema v1.2)")
    ok_flag, errs, warns = validate_input(inp)
    print(f"  result: ok={ok_flag}  errors={len(errs)}  warnings={len(warns)}")
    for e in errs:  print(f"    ERROR: {e}")
    for w in warns: print(f"    WARN:  {w}")
    if not ok_flag:
        fail(f"validate_input returned {len(errs)} error(s)")
    ok("validate_input passed")


def stage_solver(input_path: Path, results_path: Path, excel_path: Path) -> None:
    hr("STAGE 3 / 4  —  solver run  (MIP UC/ED, HiGHS)")
    cmd = [
        sys.executable,
        str(_resolve_repo_file("solver", "powersim_solver.py")),
        "--input",  str(input_path),
        "--output", str(results_path),
        "--excel",  str(excel_path),
    ]
    print("  command: " + " ".join(cmd))
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.time() - t0
    # Echo solver output so smoke_168h.py is a black-box runner users trust.
    if proc.stdout:
        print("  ── solver stdout ──")
        print("\n".join("    " + ln for ln in proc.stdout.rstrip().splitlines()))
    if proc.returncode != 0:
        if proc.stderr:
            print("  ── solver stderr ──")
            print("\n".join("    " + ln for ln in proc.stderr.rstrip().splitlines()))
        fail(f"solver exited with code {proc.returncode}")
    if not results_path.exists():
        fail(f"solver did not produce {results_path}")
    ok(f"solver completed in {dt:.1f}s → {results_path}  "
       f"({results_path.stat().st_size/1024:.1f} KiB)")


def stage_validate_output(results_path: Path) -> dict:
    hr("STAGE 4 / 4  —  validate_output (schema v1.2) + HTML import shape check")
    res = json.loads(results_path.read_text(encoding="utf-8"))
    ok_flag, errs, warns = validate_output(res)
    print(f"  validate_output: ok={ok_flag}  errors={len(errs)}  warnings={len(warns)}")
    for e in errs:  print(f"    ERROR: {e}")
    for w in warns: print(f"    WARN:  {w}")
    if not ok_flag:
        fail(f"validate_output returned {len(errs)} error(s)")

    # ── HTML-side importResults() compatibility check ─────────────────
    # Mirrors psValidateOutput() in PowerSim_v4.html — if this passes here
    # it will pass there, guaranteeing the HTML can render the results.
    required = ("metadata","diagnostics","system_summary","hourly_system",
                "hourly_by_unit","by_unit_summary")
    missing = [k for k in required if k not in res]
    if missing:
        fail(f"HTML importResults would reject: missing {missing}")

    H   = res["metadata"].get("horizon_hours")
    hs  = res.get("hourly_system") or []
    if H is not None and len(hs) != H:
        fail(f"HTML importResults would reject: hourly_system length {len(hs)} ≠ {H}")
    for gid, rows in (res.get("hourly_by_unit") or {}).items():
        if H is not None and len(rows) != H:
            fail(f"HTML importResults would reject: hourly_by_unit['{gid}'] length {len(rows)} ≠ {H}")
    ok("validate_output passed")
    ok("HTML importResults() structural check passed")
    return res


def stage_report(res: dict) -> None:
    hr("RESULT SUMMARY")
    meta = res["metadata"]
    sm   = res["system_summary"]
    diag = res["diagnostics"]
    fpr  = meta.get("data_source_fingerprint", {}) or {}

    print(f"  Scenario:         {meta.get('scenario')}")
    print(f"  Schema:           {meta.get('schema_version')}   "
          f"Horizon: {meta.get('horizon_hours')}h")
    print(f"  Loader version:   {fpr.get('loader_version')}")
    print(f"  Solver version:   {fpr.get('solver_version')}")
    print(f"  Solver status:    {diag.get('solver_status')}   "
          f"time: {diag.get('solve_time_s')}s")
    print(f"  Total cost:       ${sm.get('total_cost_usd'):,.0f}")
    print(f"  Total energy:     {sm.get('total_energy_mwh'):,.0f} MWh")
    print(f"  Avg λ:            ${sm.get('avg_lambda_usd_mwh'):.3f} /MWh")
    print(f"  Peak load:        {sm.get('peak_load_mw'):.1f} MW")
    print(f"  Gas used:         {sm.get('total_gas_mm3'):.4f} Mm³")
    print(f"  Unserved energy:  {sm.get('total_unserved_mwh'):.1f} MWh")
    print(f"  Closure:          ok={meta.get('closure_ok')}  "
          f"gap={meta.get('closure_gap')}")
    if meta.get("closure_note"):
        print(f"                    {meta['closure_note']}")
    print(f"  Gas cap binding:  {diag.get('gas_cap_binding')}  "
          f"(utilization {diag.get('gas_utilization_pct')}%)")
    print(f"  Hydro end-stor warnings: {len(diag.get('hydro_end_storage_warnings') or [])}")
    print(f"  Output schema:    ok={diag.get('output_schema_ok')}  "
          f"errors={len(diag.get('output_schema_errors') or [])}  "
          f"warnings={len(diag.get('output_schema_warnings') or [])}")


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="PowerSim v4.0 Stage 1 smoke test")
    ap.add_argument("--project-dir", default=os.environ.get("POWERSIM_PROJECT_DIR", "/mnt/project"),
                    help="Directory containing the uploaded project files.")
    ap.add_argument("--config", default=str(_resolve_repo_file("tests", "stage1_smoke_fleet.json")),
                    help="Fleet + mapping config JSON.")
    ap.add_argument("--keep-outputs", default=None,
                    help="Directory to write intermediate artifacts (default: temp).")
    args = ap.parse_args(argv)

    project_dir = Path(args.project_dir)
    config_path = Path(args.config)
    if not project_dir.is_dir():
        fail(
            f"project dir not found: {project_dir}. "
            "Provide --project-dir /path/to/data or set POWERSIM_PROJECT_DIR."
        )
    if not config_path.is_file(): fail(f"config not found: {config_path}")

    out_dir = Path(args.keep_outputs) if args.keep_outputs else Path("/tmp/powersim_smoke")
    out_dir.mkdir(parents=True, exist_ok=True)
    input_json   = out_dir / "powersim_input.json"
    results_json = out_dir / "powersim_results.json"
    excel_path   = out_dir / "powersim_results.xlsx"

    print("┌" + "─" * 70 + "┐")
    print("│  PowerSim v4.0 — Stage 1 end-to-end smoke test" + " " * 24 + "│")
    print(f"│  project_dir : {str(project_dir):<55}│")
    print(f"│  config      : {config_path.name:<55}│")
    print(f"│  out_dir     : {str(out_dir):<55}│")
    print(f"│  loader      : {LOADER_VERSION:<55}│")
    print("└" + "─" * 70 + "┘")

    t_total = time.time()
    inp = stage_dataio(project_dir, config_path, input_json)
    stage_validate_input(inp)
    stage_solver(input_json, results_json, excel_path)
    res = stage_validate_output(results_json)
    stage_report(res)

    hr("PIPELINE COMPLETE")
    print(f"  Total wallclock: {time.time()-t_total:.1f}s")
    print(f"  Artifacts in:    {out_dir}")
    print()
    print("  🎉  Stage 1 smoke test PASSED — HTML → JSON → solver → JSON → HTML")
    print("      round-trip is intact.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
