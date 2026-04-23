"""
PowerSim v4.0 — Batch orchestrator  (#18)
==========================================

Runs a queue of PowerSim jobs described in a single YAML-like JSON
file, in sequence, honouring a per-job resource budget.  Every job's
results are auto-ingested into the SQL store (`scripts/db_ingest.py`)
and receive a consistent tag so downstream SQL queries can filter by
batch.

Queue file format:

    {
      "batch_id":      "monthly_mc_sensitivity_2026_04",
      "tag":           "monthly_mc",
      "default": {
        "project_dir": "project_data",
        "config":      "tests/gse_2026_baseline.json",
        "time_limit":  300,
        "mip_gap":     0.02,
        "rolling_window": 168,
        "rolling_step":   168,
        "warm_start":   true,
        "solver":       "auto"
      },
      "jobs": [
        {"name":"baseline",   "scenario":"A_mean",  "hours": 720},
        {"name":"wet",        "scenario":"MC_P10",  "hours": 720},
        {"name":"dry",        "scenario":"MC_P90",  "hours": 720},
        {"name":"annual",     "scenario":"A_mean",  "hours": 8760}
      ],
      "out_root": "out/batch_monthly_mc"
    }

Usage:

    python scripts/batch.py --queue batches/monthly_mc.json
    python scripts/batch.py --queue batches/monthly_mc.json --dry-run
    python scripts/batch.py --queue batches/monthly_mc.json --resume

Each job runs `scripts/run_horizon.py` with the merged default+override
flags; on success the results JSON is ingested into powersim_store.sqlite
with the batch tag.  A `batch_manifest.json` is written alongside the
outputs so runs can be reproduced exactly.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RUN  = HERE / "run_horizon.py"
DB   = HERE / "db_ingest.py"


def _load_queue(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _merge(default: dict, job: dict) -> dict:
    m = dict(default); m.update({k: v for k, v in job.items() if v is not None})
    return m


def _cmd_for_job(job: dict, out_dir: Path, project_dir: str, config: str) -> list:
    cmd = [sys.executable, str(RUN),
           "--project-dir", project_dir,
           "--config",      config,
           "--out-dir",     str(out_dir)]
    if "scenario"       in job: cmd += ["--scenario",       str(job["scenario"])]
    if "hours"          in job: cmd += ["--hours",          str(job["hours"])]
    if "start_hour"     in job: cmd += ["--start-hour",     str(job["start_hour"])]
    if "mip_gap"        in job: cmd += ["--mip-gap",        str(job["mip_gap"])]
    if "time_limit"     in job: cmd += ["--time-limit",     str(job["time_limit"])]
    if "rolling_window" in job: cmd += ["--rolling-window", str(job["rolling_window"])]
    if "rolling_step"   in job: cmd += ["--rolling-step",   str(job["rolling_step"])]
    if job.get("ed_resolve"): cmd += ["--ed-resolve"]
    return cmd


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--queue", required=True)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print the commands that would run; do not execute.")
    ap.add_argument("--resume", action="store_true",
                    help="Skip jobs whose output directory already has powersim_results.json.")
    ap.add_argument("--no-db",  action="store_true",
                    help="Do not auto-ingest results into the SQL store.")
    args = ap.parse_args(argv)

    queue = _load_queue(Path(args.queue))
    defaults = queue.get("default") or {}
    tag      = queue.get("tag", queue.get("batch_id", "batch"))
    out_root = Path(queue.get("out_root", f"out/batch_{tag}"))
    out_root.mkdir(parents=True, exist_ok=True)

    manifest = {
        "batch_id":   queue.get("batch_id", tag),
        "tag":        tag,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "queue":      queue,
        "results":    [],
    }

    t_start = time.time()
    for i, job in enumerate(queue.get("jobs") or [], start=1):
        m = _merge(defaults, job)
        name = m.get("name") or f"job_{i}"
        out_dir = out_root / name
        rj = out_dir / "powersim_results.json"

        if args.resume and rj.exists():
            print(f"\n── [{i}/{len(queue['jobs'])}] SKIP {name}  (already done)")
            manifest["results"].append({"name": name, "status": "skipped",
                                        "out_dir": str(out_dir)})
            continue

        print(f"\n── [{i}/{len(queue['jobs'])}] {name}  scenario={m.get('scenario','?')}  "
              f"hours={m.get('hours','?')} ──")
        cmd = _cmd_for_job(m, out_dir, m["project_dir"], m["config"])
        print(" ".join(cmd))
        if args.dry_run:
            manifest["results"].append({"name": name, "status": "dry-run",
                                        "cmd": cmd})
            continue

        t0 = time.time()
        rc = subprocess.run(cmd).returncode
        elapsed = time.time() - t0

        entry = {"name": name, "status": "ok" if rc == 0 else "fail",
                 "rc": rc, "elapsed_s": round(elapsed, 2),
                 "out_dir": str(out_dir)}

        if rc == 0 and rj.exists() and not args.no_db:
            subprocess.run([sys.executable, str(DB),
                            "--results", str(rj),
                            "--tag",     f"{tag}:{name}"],
                           check=False)

        manifest["results"].append(entry)

    manifest["ended_at"]    = time.strftime("%Y-%m-%d %H:%M:%S")
    manifest["wallclock_s"] = round(time.time() - t_start, 2)
    (out_root / "batch_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n══════════════════════════════════════════════════════════════════════")
    print(f"  BATCH {tag} — {len(manifest['results'])} job(s)  "
          f"wallclock {manifest['wallclock_s']}s")
    for r in manifest["results"]:
        print(f"    {r['status']:<8} {r['name']}")
    print(f"💾 manifest → {out_root/'batch_manifest.json'}")
    return 0 if all(r["status"] in ("ok", "skipped", "dry-run")
                    for r in manifest["results"]) else 2


if __name__ == "__main__":
    sys.exit(main())
