# PowerSim v1.0 Release Candidate Checklist

This checklist prepares PowerSim for a v1.0 Release Candidate demo. The current
repository components keep their internal version labels, but this document
tracks the external v1.0 RC readiness gate: reproducible setup, documented run
commands, HTML import/export readiness, honest limitations, and no private GSE
data in git.

## Current RC status

**Status: RC documentation/demo-ready with validation blockers.**

PowerSim can be demonstrated from synthetic `project_data/` and checked with the
included 168-hour smoke workflow. Longer 720-hour, 8760-hour, and MC sweep
commands are documented and supported by the runner scripts, but final v1.0 RC
sign-off should wait for the remaining blocker list below, especially real GSE
data validation.

## Release checklist

| Area | Status | Evidence / required action |
|------|--------|----------------------------|
| Clean clone setup | Ready | `README.md` and `docs/HAPPY_PATH.md` include clone, virtualenv, dependency, demo-data, smoke-run, and HTML import steps. |
| Private data safety | Ready for docs | Required files and folder layout are documented below. Do not commit `project_data/`, `out/`, or private GSE files. |
| 168h smoke run | Ready | `tests/smoke_168h.py` supports a quick end-to-end run using synthetic project data and the smoke fleet. |
| 720h run | Ready | `scripts/run_horizon.py` supports `--hours 720` with rolling-window settings. |
| 8760h run | Supported, time-box before demo | `scripts/run_horizon.py` supports `--hours 8760`; run ahead of live demo because runtime depends on machine, fleet, and gap settings. |
| MC sweep | Supported | `scripts/run_mc_sweep.py` supports `A_mean MC_P10 MC_P50 MC_P90` and writes `mc_summary.json`. |
| HTML result import | Ready | The UI imports `powersim_results.json` and renders Results / Compare views. |
| Excel report export | Ready in HTML and Python output | Python writes `powersim_results.xlsx`; HTML Results tab can export a workbook from imported results. |
| Scenario comparison | Ready | Compare tab supports adding one or more results JSON files and running comparison. |
| Known limitations | Ready | Limitations are listed in this checklist and should be repeated verbally in the demo. |
| Real GSE validation | Blocker for final RC sign-off | Synthetic demo data is not a substitute for private GSE source validation and stakeholder acceptance. |

## Required local input files

PowerSim runs need a local `project_data/` directory. For demo and CI, build it
synthetically. For private production validation, replace it with real GSE input
files using the same naming pattern.

### Synthetic demo data

```bash
python scripts/build_demo_project.py --out project_data
```

The generator creates 8760-hour synthetic profiles for hydro zones, renewable
sites, and demand. It is safe for public demos and CI because it does not contain
private GSE source data.

### Expected private data folder structure

Use this local-only layout when running against real GSE files:

```text
project_data/
├── 2026_A_historical_mean.csv
├── 2026_B_montecarlo_P10.csv
├── 2026_B_montecarlo_P50.csv
├── 2026_B_montecarlo_P90.csv
├── Solar_<Site>_2026_<Scenario>.csv
├── Wind_<Site>_2026_<Scenario>.csv
└── GSE_CharYear_Normalized_1.xlsx
```

Notes:

* `project_data/` is a working folder and should remain untracked.
* Hydro files are expected for the baseline and MC scenarios used in the run.
* Renewable files are expected per configured site and scenario.
* The demand workbook is required for the demand-shape workflow.
* The installed-capacity workbook used by the asset mapper is private and should
  be referenced from a local path, not copied into the repository.
* `tests/gse_2026_installed_capacity_registry_summary.json` is a sanitized
  aggregate-only fixture for category totals and mapper regression tests; do not
  replace it with the private workbook or row-level registry unless sharing has
  been explicitly approved.

Optional full-fleet config build from a private installed-capacity workbook:

```bash
python solver/powersim_asset_mapper.py \
    --excel /private/path/to/დადგმული_სიმძლავრე__2026.xlsx \
    --hydro-overrides tests/reservoir_overrides_template.csv \
    --out /private/path/full_fleet_2026.json
```

If you intentionally want to use that generated fleet config in this repo, first
confirm it contains no private source values that cannot be shared. Otherwise,
keep it in a private path and pass it to `--config` from there.

## Run commands for RC validation

All commands assume a clean clone with dependencies installed and synthetic or
private `project_data/` already available.

### 168-hour smoke run

```bash
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h
```

Expected artifacts:

* `out/smoke_168h/powersim_input.json`
* `out/smoke_168h/powersim_results.json`
* `out/smoke_168h/powersim_results.xlsx`

### 720-hour run

```bash
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/gse_720h
```

Use the smoke fleet config (`tests/stage1_smoke_fleet.json`) if you want the
fastest possible local check. Use the GSE baseline config for the release demo.

### 8760-hour annual run

```bash
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       8760 \
    --mip-gap     0.03 \
    --rolling-window 168 --rolling-step 168 \
    --time-limit  600 \
    --out-dir     out/gse_8760h
```

8760-hour runs are supported by the horizon runner and schema. For a live demo,
run this before the call and import the completed JSON; do not rely on a live
annual solve unless the machine, fleet, solver gap, and time limit have already
been tested.

### MC sweep

```bash
python scripts/run_mc_sweep.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --scenarios   A_mean MC_P10 MC_P50 MC_P90 \
    --out-dir     out/gse_mc_720h
```

Expected artifacts:

* `out/gse_mc_720h/A_mean/powersim_results.json`
* `out/gse_mc_720h/MC_P10/powersim_results.json`
* `out/gse_mc_720h/MC_P50/powersim_results.json`
* `out/gse_mc_720h/MC_P90/powersim_results.json`
* `out/gse_mc_720h/mc_summary.json`

## HTML import workflow

1. Open `html/PowerSim_v4.html` in a modern browser.
2. Optional: click **🏭 GSE 2026 Demo ჩატვირთვა** to load the demo fleet in the UI.
3. Click **📥 Import Results JSON**.
4. Select a generated `powersim_results.json`, for example
   `out/gse_720h/powersim_results.json`.
5. Review KPI cards, dispatch stack, lambda chart, monthly summary,
   diagnostics, and per-asset summary.
6. Open **🔀 Compare**.
7. Click **➕ Add Scenario(s)** and add two or more result JSON files, for
   example `A_mean` and `MC_P90` from the MC sweep output folder.
8. Click **▶ Compare**.
9. Return to **Results** and click **⬇ Excel** to export a report workbook from
   the currently imported result.

## Known limitations to disclose

* **Not a full PLEXOS clone.** PowerSim is a transparent UC/ED workflow and demo
  platform, not a complete replacement for commercial production-cost software.
* **Limited network constraints.** Current demo readiness emphasizes system-level
  dispatch and scenario comparison; full nodal or zonal transmission constraints
  are not a v1.0 RC claim.
* **Hydro simplifications.** Hydro is represented with practical reservoir,
  cascade, water-value, and target-end-level assumptions, not a complete water
  management or hydrological operations model.
* **BESS and reservoir carry-over validation.** Rolling-horizon carry-over is a
  priority item from Issue #3. Treat BESS/reservoir carry-over as pending final
  validation unless the latest test/audit run for the candidate build explicitly
  signs it off.
* **Real GSE data validation status.** Synthetic demo data is validated for the
  public demo path. Real private GSE source files still require a controlled
  validation pass and stakeholder review before final v1.0 RC sign-off.
* **Market assumptions.** Fuel price, gas caps, imports, water values, reserve
  penalties, unserved-energy penalty, and other market assumptions are model
  inputs/calibration choices, not audited market forecasts.
* **Runtime sensitivity.** Annual and MC runs can vary significantly by machine,
  solver version, fleet size, MIP gap, rolling-window size, and time limits.

## Remaining blockers before final v1.0 RC

1. Run and archive the 168-hour smoke result from a clean clone.
2. Run and archive the 720-hour GSE-baseline-style result from clean synthetic
   `project_data/`.
3. Time-box or precompute the 8760-hour annual result for demo use.
4. Run the MC sweep and confirm all requested scenarios write valid result JSON.
5. Reconfirm HTML import, scenario comparison, and Excel export in the target
   demo browser.
6. Complete private real-GSE data validation without committing private data.
7. Record validation results, environment, solver version, and known deviations
   in release notes.
8. Confirm rolling-horizon carry-over behavior for reservoir, BESS, gas cap, and
   unit commitment on the final candidate build.

## RC go / no-go decision

**Go for public demo:** Yes, if using synthetic `project_data/` and precomputed
long-horizon artifacts.

**Go for v1.0 RC tag:** Not until the remaining blockers above are either closed
or explicitly accepted as documented limitations by the release owner.
