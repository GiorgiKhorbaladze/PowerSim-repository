# PowerSim v1.0 Release Candidate Checklist

This checklist tracks PowerSim v1.0 repository-side readiness after the BESS sizing / Georgia 2030 research module was removed from project scope.

## Current RC status

**Status: repo-side v1.0 RC ready for the synthetic/demo workflow.**

The repository now has no open PR or open issue blockers. The separate BESS sizing research module was removed from scope and merged through PR #50. Core PowerSim storage/BESS dispatch functionality remains in the main solver and continues to be covered by deterministic storage, SOC, reserve, and carry-over tests.

Private real-GSE data validation remains an operational/stakeholder activity outside the repository. It should be performed before using results for official GSE conclusions, but it is no longer tracked as a code-readiness blocker in this repository unless a specific code defect is found.

## Scope after BESS research removal

Removed from active project scope:

- `solver/bess_sizing.py`
- `solver/bess_scenario_runner.py`
- `tests/test_bess_sizing.py`
- BESS sizing research CI references
- Georgia 2030 BESS sizing research issues and PRs

Preserved in active PowerSim scope:

- BESS/storage asset dispatch inside the UC/ED solver
- BESS SOC logic and rolling carry-over
- BESS PLEXOS-parity storage tests
- BESS and pumped-hydro reserve provision tests
- Core dispatch, adequacy screening, scenario comparison, reporting, and HTML import/export workflows

## Release checklist

| Area | Status | Evidence / required action |
|------|--------|----------------------------|
| Open PR blockers | Complete | No open PR blockers after closing stale PR #29 and merging PR #50. |
| Open issue blockers | Complete | The temporary v1.0 agent queue was closed as completed; BESS sizing issues were closed as not planned after scope removal. |
| Clean clone setup | Ready | `README.md` and `docs/HAPPY_PATH.md` document clone, virtualenv, dependency, demo-data, smoke-run, and HTML import steps. |
| Private data safety | Ready | Private GSE files, `project_data/`, and `out/` remain local-only and must not be committed. |
| Synthetic demo data | Ready | `scripts/build_demo_project.py` creates safe public demo input data. |
| 2026 demo asset registry | Complete | The public 2026 demo registry and validator remain in scope. |
| 168h smoke run | Ready / CI-covered | `tests/smoke_168h.py` supports the quick end-to-end smoke workflow. |
| 720h run | Ready | `scripts/run_horizon.py` supports `--hours 720` with rolling-window settings. |
| 8760h run | Supported | Annual runs are supported; for live demos, precompute rather than solve live. |
| MC sweep | Supported | `scripts/run_mc_sweep.py` supports `A_mean MC_P10 MC_P50 MC_P90` and writes `mc_summary.json`. |
| HTML result import | Ready | The UI imports `powersim_results.json` and renders Results / Compare views. |
| Excel report export | Ready | Python writes `powersim_results.xlsx`; HTML Results tab can export a workbook from imported results. |
| Scenario comparison | Ready | Compare tab supports adding one or more result JSON files and running comparison. |
| JSON contract | Complete / CI-covered | JSON contract checks remain in CI. |
| Rolling carry-over | Complete / CI-covered | BESS SOC, reservoir/hydro, gas/DR/UC carry-over tests remain in CI. |
| Ramp-rate regression | Complete / CI-covered | Sub-hourly and rolling-boundary ramp tests remain in CI. |
| Reserve/storage provision | Complete / CI-covered | BESS and pumped-hydro reserve provision tests remain in CI. |
| Hydro / thermal / solar / wind stages | Complete / CI-covered | Deterministic tests remain in CI. |
| Adequacy screening | Ready / CI-covered | Adequacy metrics and expansion workflow tests remain in CI. |
| BESS sizing research | Removed from scope | Removed by PR #50; not part of v1.0 RC. |
| Real private GSE validation | External / operational | Required before official external conclusions, but not a repository code blocker. |

## Required local input files

PowerSim runs need a local `project_data/` directory. For demo and CI, build it synthetically. For private production validation, replace it with real GSE input files using the same naming pattern.

### Synthetic demo data

```bash
python scripts/build_demo_project.py --out project_data
```

The generator creates 8760-hour synthetic profiles for hydro zones, renewable sites, and demand. It is safe for public demos and CI because it does not contain private GSE source data.

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

- `project_data/` is a working folder and should remain untracked.
- Hydro files are expected for the baseline and MC scenarios used in the run.
- Renewable files are expected per configured site and scenario.
- The demand workbook is required for the demand-shape workflow.
- The installed-capacity workbook used by the asset mapper is private and should be referenced from a local path, not copied into the repository.
- `tests/gse_2026_installed_capacity_registry_summary.json` is a sanitized aggregate-only fixture for category totals and mapper regression tests.

Optional full-fleet config build from a private installed-capacity workbook:

```bash
python solver/powersim_asset_mapper.py \
    --excel /private/path/to/დადგმული_სიმძლავრე__2026.xlsx \
    --hydro-overrides tests/reservoir_overrides_template.csv \
    --out /private/path/full_fleet_2026.json
```

## Run commands for RC validation

All commands assume a clean clone with dependencies installed and synthetic or private `project_data/` already available.

### 168-hour smoke run

```bash
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h
```

Expected artifacts:

- `out/smoke_168h/powersim_input.json`
- `out/smoke_168h/powersim_results.json`
- `out/smoke_168h/powersim_results.xlsx`

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

Annual runs are supported by the horizon runner and schema. For a live demo, run this before the call and import the completed JSON.

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

- `out/gse_mc_720h/A_mean/powersim_results.json`
- `out/gse_mc_720h/MC_P10/powersim_results.json`
- `out/gse_mc_720h/MC_P50/powersim_results.json`
- `out/gse_mc_720h/MC_P90/powersim_results.json`
- `out/gse_mc_720h/mc_summary.json`

## HTML import workflow

1. Open `html/PowerSim_v4.html` in a modern browser.
2. Optional: click **🏭 GSE 2026 Demo ჩატვირთვა** to load the demo fleet in the UI.
3. Click **📥 Import Results JSON**.
4. Select a generated `powersim_results.json`, for example `out/gse_720h/powersim_results.json`.
5. Review KPI cards, dispatch stack, lambda chart, monthly summary, diagnostics, and per-asset summary.
6. Open **🔀 Compare**.
7. Click **➕ Add Scenario(s)** and add two or more result JSON files.
8. Click **▶ Compare**.
9. Return to **Results** and click **⬇ Excel** to export a report workbook from the currently imported result.

## Known limitations to disclose

- **Not a full PLEXOS clone.** PowerSim is a transparent UC/ED workflow and demo platform, not a complete replacement for commercial production-cost software.
- **Limited network constraints.** Current demo readiness emphasizes system-level dispatch and scenario comparison; full nodal or zonal transmission constraints are not a v1.0 RC claim.
- **Hydro simplifications.** Hydro includes practical reservoir, cascade, inflow-unit, rule-curve, minimum-release, water-value, and head-efficiency metadata features, but not a complete hydrological operations model.
- **BESS sizing research removed.** PowerSim still supports BESS/storage dispatch inside the solver, but the separate Georgia 2030 BESS sizing research module is no longer part of the project scope.
- **Synthetic/demo registry readiness.** Public demo data and sanitized fixtures are suitable for demos and CI. Private GSE source files require separate controlled validation before official conclusions.
- **Market assumptions.** Fuel price, gas caps, imports, water values, reserve penalties, unserved-energy penalty, and other market assumptions are model inputs/calibration choices, not audited market forecasts.
- **Runtime sensitivity.** Annual and MC runs can vary by machine, solver version, fleet size, MIP gap, rolling-window size, and time limits.

## Final repo-side go / no-go decision

**Go for v1.0 repo-side RC:** Yes.

**Go for public demo:** Yes, using synthetic `project_data/` and/or precomputed long-horizon artifacts.

**Go for official GSE analytical conclusions:** Only after private real-GSE data validation and stakeholder acceptance.

## Solver hardening notes

- BESS can provide reserve up/down subject to discharge/charge headroom and SOC or empty-SOC over `reserve_duration_h`.
- Eligible reserve assets with unsupported provider types are surfaced in diagnostics (`reserve_eligible_filtered`) rather than silently ignored.
- Rolling-horizon first-period ramp constraints use previous-window dispatch carry-over where available.
- `system_summary.total_cost_usd` remains production/gross cost for compatibility; `system_summary.total_objective_cost_usd` and `diagnostics.objective_breakdown` report the full objective and closure gap.
- Stochastic summaries use full scenario result stores/objective costs and warn with `stochastic_profiles_not_switched` when no scenario-specific profiles or overrides are applied.
- Limitations remain: not a full PLEXOS clone, simplified DC-OPF, no AC voltage/reactive power, and no reserve market settlement.

## Adequacy Stage 1 release checks

- Run `python -m py_compile solver/powersim_adequacy.py scripts/run_adequacy.py`.
- Run `python tests/test_adequacy_metrics.py` and `python tests/test_adequacy_expansion.py` with synthetic data only.
- Run `python tests/check_json_contract.py` to confirm backward-compatible sample JSON contracts.
- For demo handoff, run `python scripts/run_adequacy.py --input samples/sample_input_168h.json --out-dir out/adequacy_demo --mode deterministic_derated --write-expanded-input` and then test any recommended builds through UC/ED before describing investment decisions.
- Confirm documentation states the module is screening-level and not full PLEXOS PASA.
