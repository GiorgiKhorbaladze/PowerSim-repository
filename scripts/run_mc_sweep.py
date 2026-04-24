"""
PowerSim v4.0 — Monte-Carlo scenario sweep
==========================================

Runs the solver across a configurable list of MC scenarios (default
P10 / P50 / P90 plus the A_mean baseline) using IDENTICAL solver
calibration so only the inflow CSV changes between runs.

Outputs one subdirectory per scenario (input JSON, results JSON, Excel)
and a top-level ``mc_summary.json`` that aggregates total_cost,
avg_lambda, total_unserved_mwh, total_gas_mm3 across the sweep.

Usage
-----

    python scripts/run_mc_sweep.py \\
        --project-dir project_data \\
        --config      tests/stage1_smoke_fleet.json \\
        --hours       720 \\
        --out-dir     out/mc_sweep_720h \\
        --scenarios   A_mean MC_P10 MC_P50 MC_P90

The aggregator records P10/P50/P90 risk metrics over the requested
scenarios. P10 / P50 / P90 here refer to *inflow scenarios*, NOT to
percentiles of the cost distribution — the cost distribution is reported
separately in ``mc_summary.json -> percentiles``.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RUN  = HERE / "run_horizon.py"


def _hr(title: str) -> None:
    print("\n" + "═" * 72)
    print(f"   {title}")
    print("═" * 72)


def _percentiles(vals: list[float]) -> dict:
    if not vals:
        return {}
    s = sorted(vals)
    n = len(s)
    def pct(p: float) -> float:
        if n == 1: return s[0]
        idx = max(0, min(n - 1, int(round(p / 100.0 * (n - 1)))))
        return s[idx]
    return {"p10": pct(10), "p50": pct(50), "p90": pct(90),
            "min": s[0], "max": s[-1], "mean": statistics.fmean(s)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--config",      required=True)
    ap.add_argument("--out-dir",     required=True)
    ap.add_argument("--scenarios", nargs="+",
                    default=["A_mean", "MC_P10", "MC_P50", "MC_P90"])
    ap.add_argument("--hours",   type=int, default=168)
    ap.add_argument("--mip-gap", type=float, default=None)
    ap.add_argument("--time-limit", type=int, default=None)
    ap.add_argument("--rolling-window", type=int, default=None)
    ap.add_argument("--rolling-step",   type=int, default=None)
    ap.add_argument("--ed-resolve", action="store_true")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summaries: dict[str, dict] = {}
    t_start = time.time()
    for sc in args.scenarios:
        _hr(f"SCENARIO {sc}")
        sc_out = out_dir / sc
        cmd = [sys.executable, str(RUN),
               "--project-dir", args.project_dir,
               "--config",      args.config,
               "--scenario",    sc,
               "--hours",       str(args.hours),
               "--out-dir",     str(sc_out)]
        if args.mip_gap        is not None: cmd += ["--mip-gap",        str(args.mip_gap)]
        if args.time_limit     is not None: cmd += ["--time-limit",     str(args.time_limit)]
        if args.rolling_window is not None: cmd += ["--rolling-window", str(args.rolling_window)]
        if args.rolling_step   is not None: cmd += ["--rolling-step",   str(args.rolling_step)]
        if args.ed_resolve:                 cmd += ["--ed-resolve"]
        print(" ".join(cmd))
        t0 = time.time()
        rc = subprocess.run(cmd).returncode
        dt = time.time() - t0
        if rc != 0:
            print(f"❌ {sc} failed (rc={rc}); continuing")
            summaries[sc] = {"error": f"rc={rc}", "wallclock_s": round(dt, 2)}
            continue
        rj = sc_out / "powersim_results.json"
        if not rj.exists():
            summaries[sc] = {"error": "no results json", "wallclock_s": round(dt, 2)}
            continue
        res  = json.loads(rj.read_text(encoding="utf-8"))
        sm   = res.get("system_summary", {})
        diag = res.get("diagnostics", {})
        meta = res.get("metadata", {})
        summaries[sc] = {
            "wallclock_s":         round(dt, 2),
            "total_cost_usd":      sm.get("total_cost_usd"),
            "avg_lambda_usd_mwh":  sm.get("avg_lambda_usd_mwh"),
            "total_energy_mwh":    sm.get("total_energy_mwh"),
            "peak_load_mw":        sm.get("peak_load_mw"),
            "total_gas_mm3":       sm.get("total_gas_mm3"),
            "total_unserved_mwh":  sm.get("total_unserved_mwh"),
            "total_curtailed_mwh": sm.get("total_curtailed_mwh"),
            "closure_ok":          meta.get("closure_ok"),
            "closure_gap":         meta.get("closure_gap"),
            "solver_status":       diag.get("solver_status"),
        }

    # ── aggregation ──────────────────────────────────────────────────
    successful = [v for v in summaries.values() if "error" not in v]
    aggregate = {
        "scenarios_run":   len(summaries),
        "scenarios_ok":    len(successful),
        "horizon_hours":   args.hours,
        "wallclock_total_s": round(time.time() - t_start, 2),
        "percentiles": {
            metric: _percentiles([v[metric] for v in successful
                                  if v.get(metric) is not None])
            for metric in ("total_cost_usd", "avg_lambda_usd_mwh",
                           "total_unserved_mwh", "total_gas_mm3")
        },
        "by_scenario": summaries,
    }
    summary_path = out_dir / "mc_summary.json"
    summary_path.write_text(json.dumps(aggregate, ensure_ascii=False, indent=2),
                            encoding="utf-8")

    _hr("MC SWEEP SUMMARY")
    print(json.dumps(aggregate["percentiles"], indent=2))
    print(f"\n  per-scenario costs:")
    for sc, v in summaries.items():
        if "error" in v:
            print(f"    {sc:<10}  ❌  {v['error']}")
        else:
            print(f"    {sc:<10}  ${v['total_cost_usd']:>14,.0f}   "
                  f"unserved={v['total_unserved_mwh']:>9.1f} MWh   "
                  f"λ̄=${v['avg_lambda_usd_mwh']:>8.2f}/MWh")
    print(f"\n💾 wrote {summary_path}")
    return 0 if all("error" not in v for v in summaries.values()) else 2


if __name__ == "__main__":
    sys.exit(main())
