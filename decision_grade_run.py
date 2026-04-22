#!/usr/bin/env python3
"""Run Monte Carlo and annual PowerSim studies with standardized output layout."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCENARIOS = ["MC_P10", "MC_P50", "MC_P90"]


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _run(cmd: list[str]) -> int:
    print("[exec]", " ".join(cmd))
    return subprocess.run(cmd).returncode


def _make_config(base: dict, scenario: str, horizon: int, rolling_window: int, rolling_step: int) -> dict:
    cfg = json.loads(json.dumps(base))
    cfg["scenario"] = scenario
    sh = dict(cfg.get("study_horizon", {}))
    sh["start_hour"] = int(sh.get("start_hour", 0) or 0)
    sh["horizon_hours"] = int(horizon)
    sh["mode"] = sh.get("mode", "full")
    cfg["study_horizon"] = sh

    ss = dict(cfg.get("solver_settings", {}))
    ss["rolling_window_h"] = int(rolling_window)
    ss["rolling_step_h"] = int(rolling_step)
    cfg["solver_settings"] = ss
    return cfg


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Decision-grade Monte Carlo + annual run driver")
    ap.add_argument("--project-dir", required=True, help="Input project data directory")
    ap.add_argument("--base-config", default="stage1_smoke_fleet.json", help="Base config JSON")
    ap.add_argument("--output-root", default="outputs", help="Root directory for outputs")
    ap.add_argument("--mc-horizon", type=int, default=720, help="MC horizon (default 720)")
    ap.add_argument("--annual-horizon", type=int, default=8760, help="Annual horizon (default 8760)")
    ap.add_argument("--annual-rolling-window", type=int, default=168)
    ap.add_argument("--annual-rolling-step", type=int, default=24)
    ap.add_argument("--skip-annual", action="store_true", help="Only run MC scenarios")
    args = ap.parse_args(argv)

    base_cfg_path = Path(args.base_config)
    if not base_cfg_path.is_file():
        print(f"[error] base config not found: {base_cfg_path}")
        return 1

    base = _load_json(base_cfg_path)
    root = Path(args.output_root)
    mc_root = root / "mc"
    annual_root = root / "annual"

    run_sim = [sys.executable, "run_simulation.py", "--project-dir", args.project_dir]

    mc_results = []
    for scenario in SCENARIOS:
        cfg = _make_config(base, scenario, args.mc_horizon, 168, 24)
        scen_dir = mc_root / f"{scenario.lower()}_{args.mc_horizon}h"
        cfg_path = scen_dir / "config.json"
        _save_json(cfg_path, cfg)

        code = _run(run_sim + [
            "--config", str(cfg_path),
            "--out-dir", str(scen_dir),
            "--horizon-hours", str(args.mc_horizon),
        ])
        if code != 0:
            print(f"[warn] scenario {scenario} failed with code {code}; continuing")
        else:
            mc_results.append((scenario, scen_dir / "powersim_results.json", scen_dir / "powersim_input.json"))

    if not args.skip_annual:
        annual_cfg = _make_config(base, "MC_P50", args.annual_horizon,
                                  args.annual_rolling_window, args.annual_rolling_step)
        annual_dir = annual_root / f"annual_{args.annual_horizon}h"
        annual_cfg_path = annual_dir / "config.json"
        _save_json(annual_cfg_path, annual_cfg)

        code = _run(run_sim + [
            "--config", str(annual_cfg_path),
            "--out-dir", str(annual_dir),
            "--horizon-hours", str(args.annual_horizon),
        ])
        if code != 0:
            print(f"[warn] annual run failed with code {code}")
        else:
            mc_results.append(("ANNUAL", annual_dir / "powersim_results.json", annual_dir / "powersim_input.json"))

    report_args = [sys.executable, "powersim_report.py", "--out-dir", str(root / "reports")]
    if mc_results:
        report_args += ["--results"] + [str(r[1]) for r in mc_results] + ["--inputs"] + [str(r[2]) for r in mc_results]
        _run(report_args)
    else:
        print("[warn] no successful runs to report")

    print(f"[done] outputs at {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
