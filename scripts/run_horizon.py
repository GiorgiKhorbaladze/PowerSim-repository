"""
PowerSim v4.0 — Single-scenario horizon run
===========================================

Convenience wrapper around the canonical 3-step pipeline:

    1.  dataio.build_input_from_project   →  input JSON
    2.  schema.validate_input             →  must pass (exit 1 on error)
    3.  solver.powersim_solver            →  results JSON + Excel
        + schema.validate_output          →  must pass

This script writes everything into one ``--out-dir`` so artifacts of a run
are self-contained and easy to diff against another run.

Examples
--------

    # 168-hour smoke run (fast)
    python scripts/run_horizon.py \\
        --project-dir project_data \\
        --config      tests/stage1_smoke_fleet.json \\
        --hours       168 \\
        --out-dir     out/run_168h

    # Full-year decision-grade run
    python scripts/run_horizon.py \\
        --project-dir project_data \\
        --config      tests/stage1_smoke_fleet.json \\
        --hours       8760 \\
        --mip-gap     0.02 \\
        --out-dir     out/run_8760h
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_schema import validate_input, validate_output, SCENARIOS  # noqa: E402
from powersim_dataio import build_input_from_project, summary_report   # noqa: E402


def _hr(title: str) -> None:
    print("\n" + "═" * 72)
    print(f"   {title}")
    print("═" * 72)


def _fail(msg: str) -> int:
    print(f"\n❌ {msg}")
    return 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project-dir", required=True,
                    help="Folder with hydro/renewable CSVs and demand XLSX.")
    ap.add_argument("--config",      required=True,
                    help="Fleet+mapping config JSON (e.g. tests/stage1_smoke_fleet.json).")
    ap.add_argument("--out-dir",     required=True,
                    help="Output directory — created if missing.")
    ap.add_argument("--scenario",    default=None, choices=SCENARIOS,
                    help="Override config['scenario'].")
    ap.add_argument("--hours",       type=int, default=None,
                    help="Override study_horizon.horizon_hours.")
    ap.add_argument("--start-hour",  type=int, default=None,
                    help="Override study_horizon.start_hour.")
    ap.add_argument("--mip-gap",     type=float, default=None,
                    help="Override solver_settings.mip_gap.")
    ap.add_argument("--time-limit",  type=int, default=None,
                    help="Override solver_settings.time_limit_s (per window).")
    ap.add_argument("--rolling-window", type=int, default=None)
    ap.add_argument("--rolling-step",   type=int, default=None)
    ap.add_argument("--ed-resolve", action="store_true",
                    help="Run an LP ED resolve for accurate marginal prices.")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = Path(args.config).resolve()
    if not cfg_path.is_file():
        return _fail(f"config not found: {cfg_path}")

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    # ── overrides
    scenario   = args.scenario   or cfg.get("scenario", "A_mean")
    sh         = dict(cfg.get("study_horizon",
                              {"start_hour": 0, "horizon_hours": 168, "mode": "full"}))
    if args.hours       is not None: sh["horizon_hours"] = args.hours
    if args.start_hour  is not None: sh["start_hour"]    = args.start_hour
    sh["mode"] = "rolling" if sh["horizon_hours"] > 168 else sh.get("mode", "full")

    s_cfg = dict(cfg.get("solver_settings") or {})
    if args.mip_gap        is not None: s_cfg["mip_gap"]        = args.mip_gap
    if args.time_limit     is not None: s_cfg["time_limit_s"]   = args.time_limit
    if args.rolling_window is not None: s_cfg["rolling_window_h"] = args.rolling_window
    if args.rolling_step   is not None: s_cfg["rolling_step_h"]   = args.rolling_step

    input_json   = out_dir / "powersim_input.json"
    results_json = out_dir / "powersim_results.json"
    excel_path   = out_dir / "powersim_results.xlsx"

    # ── 1. build input ───────────────────────────────────────────────
    _hr("STEP 1 / 3 — build input from project_data")
    print(f"  scenario={scenario}  horizon={sh['horizon_hours']}h  "
          f"start_hour={sh['start_hour']}h")
    inp = build_input_from_project(
        args.project_dir,
        assets            = cfg["assets"],
        hydro_zone_map    = cfg.get("hydro_zone_map", {}),
        re_site_map       = cfg.get("re_site_map", {}),
        reserve_products  = cfg.get("reserve_products", []),
        gas_constraints   = cfg.get("gas_constraints", {}),
        study_horizon     = sh,
        scenario          = scenario,
        annual_twh        = cfg.get("annual_twh", 15.621),
        demand_mode       = cfg.get("demand_mode", "shape_times_annual"),
        solver_settings   = s_cfg or None,
        hydro_inflow_unit = cfg.get("hydro_inflow_unit", "raw"),
        scenario_metadata = cfg.get("scenario_metadata"),
        study_year        = cfg.get("study_year", 2026),
    )
    input_json.write_text(json.dumps(inp, ensure_ascii=False, separators=(",", ":")),
                          encoding="utf-8")
    print(summary_report(inp))
    print(f"  → wrote {input_json}  ({input_json.stat().st_size/1024:.1f} KiB)")

    # ── 2. validate input ────────────────────────────────────────────
    _hr("STEP 2 / 3 — validate_input")
    ok, errs, warns = validate_input(inp)
    print(f"  ok={ok}  errors={len(errs)}  warnings={len(warns)}")
    for e in errs:  print(f"   ERROR: {e}")
    for w in warns: print(f"   WARN:  {w}")
    if not ok:
        return _fail("validate_input failed")

    # ── 3. run solver ────────────────────────────────────────────────
    _hr("STEP 3 / 3 — solve")
    cmd = [sys.executable, str(ROOT / "solver" / "powersim_solver.py"),
           "--input", str(input_json), "--output", str(results_json),
           "--excel", str(excel_path)]
    if args.ed_resolve:
        cmd.append("--ed-resolve")
    print("  command: " + " ".join(cmd))
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.time() - t0
    if proc.stdout:
        print("\n".join("  " + ln for ln in proc.stdout.rstrip().splitlines()))
    if proc.returncode != 0:
        if proc.stderr:
            print("  STDERR:")
            print("\n".join("  " + ln for ln in proc.stderr.rstrip().splitlines()))
        return _fail(f"solver failed (rc={proc.returncode})")

    # ── output validation ───────────────────────────────────────────
    res = json.loads(results_json.read_text(encoding="utf-8"))
    ok, errs, warns = validate_output(res)
    if not ok:
        for e in errs: print(f"   ERROR: {e}")
        return _fail("validate_output failed")
    for w in warns: print(f"   WARN: {w}")
    sm = res["system_summary"]
    print(f"\n✅ DONE in {dt:.1f}s — {results_json}")
    print(f"   total_cost = ${sm['total_cost_usd']:,.0f}   "
          f"avg_lambda = ${sm['avg_lambda_usd_mwh']:.2f}/MWh   "
          f"unserved = {sm['total_unserved_mwh']:.1f} MWh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
