# PowerSim v1.0-rc1 — Release Notes

**Tag:** `v1.0-rc1`
**Status:** Release Candidate — demo-ready on synthetic / public-demo data.
**Sign-off pending:** controlled validation pass on real private GSE input files.

---

## 1. Component versions

| Component | Version | Source of truth |
|-----------|---------|-----------------|
| Schema (input + output) | `1.5` | `schema/powersim_schema.py` (`SCHEMA_VERSION`) |
| HTML schema constants | `1.5`, accepted prior `{1.0, 1.1, 1.2, 1.3, 1.4}` | `html/PowerSim_v4.html` (`SCHEMA_VERSION`, `ACCEPTED_OUTPUT_SCHEMAS`) |
| Solver | `powersim_solver 1.6.0` | `solver/powersim_solver.py` (`SOLVER_VERSION`) |
| Data loader | `powersim_dataio 1.0.1` | `solver/powersim_dataio.py` (`LOADER_VERSION`) |

HTML and Python schema constants are kept in lockstep by
`tests/check_json_contract.py`. CI fails if either drifts.

## 2. Runtime environment used for the candidate build

| Item | Value |
|------|-------|
| Python | 3.10 / 3.11 / 3.12 (matrix) |
| Solver backend | HiGHS via `highspy` |
| Modeling layer | Pyomo |
| Spreadsheet I/O | `openpyxl`, `xlsxwriter` |
| Data | `pandas`, `numpy` |

Exact pins live in `requirements.txt`. `pytest` is installed in CI for the
two pytest-style suites (`test_bess_sizing.py`,
`test_gse_2026_registry_mapping.py`); it is not a runtime dependency.

## 3. What is included in v1.0-rc1

### Solver hardening (Stage 1)
* Reserve enforcement made symmetric across BESS / pumped-hydro.
* Ramp-down constraint scaled by period `dt` (sub-hourly correctness).
* Objective closure reconstruction matches the reported value within
  numerical tolerance.
* Stochastic scenario weights and summary aggregation deterministic.

### Asset feature parity (Stages 2–5)
* **Hydro Stage 1:** m³/s → Mm³/h inflow conversion, spill-cost penalty,
  `cascade_flow_mode = release_plus_spill` downstream delivery.
* **Hydro Stage 2:** environmental `min_release_mm3h`, monthly
  `storage_targets` soft penalty, head-dependent efficiency curve.
* **Thermal Stage 3:** CO₂ factor + price, multi-stage hot/cold startup,
  ambient-temperature capacity derating.
* **Solar / VRE Stage 4:** DC/AC ratio inverter clipping, inverter
  efficiency, multi-year degradation, temperature derating.
* **Wind Stage 5:** wake-loss fraction, monthly availability mask, air
  density correction (∝ 1/T<sub>kelvin</sub>).
* **BESS PLEXOS-parity:** self-discharge, asymmetric charge/discharge
  power, c-rate cap, aux load, ramp delta cap, end-SOC soft target,
  cycle counter + aging metrics.

### Rolling-horizon correctness
Cross-window invariants covered by deterministic tests:
gas annual / monthly cap, DR annual hours, UC `min_up` / `min_down`
boundary state, BESS SOC, reservoir-hydro storage, ramp limits across
the window boundary.

### Adequacy + expansion
Stage 1 LOLE / EENS / reserve-margin screening and expansion
candidate workflow, with BESS firm-capacity duration limitation noted
as an approximation, not chronological storage adequacy.

### HTML UI
* Schema-aware import/export with strict version gate.
* Compare tab for N-way scenario comparison.
* Excel export from imported results.
* Selectable themes (`gse-light`, `industrial-dark`, `executive-red`)
  with `localStorage` persistence.
* Language selector (`ka` / `en`).
* Responsive nav with anti-overlap rules at 1100 px and 760 px breakpoints.
* Empty-state visuals for profile list, reserves, results, compare,
  system status.

### Public demo data
Synthetic 8760-hour `project_data/` generator and the
`samples/demo_asset_registry_2026.json` 131-asset registry let the
public demo path run end-to-end with no private inputs in the repo.

## 4. CI gates that protect v1.0-rc1

`python-checks.yml` runs the following on each push and pull request,
on Python 3.10 / 3.11 / 3.12:

1. `py_compile` sweep across schema + solver + scripts + 17 tests.
2. Runtime dependency sanity probe.
3. `schema/powersim_schema.py` self-test.
4. Maintenance enforcement (5 checks).
5. BESS PLEXOS-parity (9 checks).
6. Hydro Stage 1 (5 checks).
7. Hydro Stage 2 (4 checks).
8. Thermal Stage 3 (4 checks).
9. Solar / VRE Stage 4 (5 checks).
10. Wind Stage 5 (4 checks).
11. JSON contract regression (HTML↔Python schema lockstep).
12. Demo asset registry validator.
13. Rolling-horizon carry-over deterministic suite (7 invariants).
14. Sub-hourly ramp deterministic test.
15. Rolling boundary ramp deterministic test.
16. Reserve storage provision deterministic test.
17. Objective closure deterministic test.
18. Stochastic summary deterministic test.
19. Adequacy metrics deterministic test.
20. Adequacy + expansion deterministic test.
21. Pytest-style suites: BESS sizing (16 tests) + GSE 2026 registry
    mapping (2 tests).
22. 168-hour smoke run on synthetic `project_data/`, full HTML → JSON →
    solver → JSON → HTML round-trip.

## 5. Known limitations (disclose verbally in the demo)

* **Not a full PLEXOS clone.** Transparent UC/ED workflow and demo
  platform; not a complete replacement for commercial production-cost
  software.
* **Limited network constraints.** v1.0-rc1 emphasizes system-level
  dispatch and scenario comparison. Full nodal/zonal transmission
  constraints are not a v1.0-rc1 claim.
* **Hydro simplifications.** Practical reservoir, cascade, water-value,
  and target-end-level assumptions; not a complete water-management or
  hydrological-operations model.
* **Adequacy is Stage 1 screening.** BESS firm capacity is duration-
  limited, not full chronological storage adequacy.
* **Market assumptions are inputs, not forecasts.** Fuel price, gas
  caps, imports, water values, reserve penalties, unserved-energy
  penalty, and similar values are calibration inputs, not audited
  market forecasts.
* **Runtime sensitivity.** Annual and MC runs vary by machine, solver
  version, fleet size, MIP gap, rolling-window size, and time limits.
  Pre-compute long-horizon artifacts before a live demo.
* **Real GSE data validation status.** Synthetic demo data is validated
  for the public demo path. Real private GSE source files still require
  a controlled validation pass and stakeholder review before final
  v1.0 sign-off.

## 6. Things intentionally not in the repository

* `project_data/` — working folder, untracked.
* `out/` — solver/run outputs, untracked.
* Original real GSE Excel workbook (installed-capacity registry).
* Any private GSE source CSVs / workbooks / OneDrive paths.
* Private hydro / demand workbooks referenced by the asset mapper.

`tests/gse_2026_installed_capacity_registry_summary.json` is a
sanitized aggregate-only fixture for category totals and mapper
regression tests; it is **not** the private workbook or row-level
registry.

## 7. Remaining items before tagging v1.0 (non-RC)

1. Controlled validation pass on real private GSE inputs without
   committing any of them.
2. 8760-hour annual run pre-computed for the live demo on the target
   machine, solver gap, time limit, and fleet config that will be used
   on stage.
3. MC sweep (`A_mean`, `MC_P10`, `MC_P50`, `MC_P90`) archived for the
   demo.
4. HTML import + Compare + Excel export reconfirmed in the target demo
   browser on the pre-computed artifacts.
5. Stakeholder review and acceptance.

## 8. Go / no-go for the public demo

* **Public demo on synthetic `project_data/`:** **Go.**
* **v1.0 final tag (post-rc1):** waits on the items in section 7.

## 9. Reproduce this build

```bash
git clone https://github.com/GiorgiKhorbaladze/PowerSim-repository.git
cd PowerSim-repository
git checkout v1.0-rc1                     # once the tag is pushed
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install pytest                        # CI-only, for two pytest suites

# Synthetic demo data
python scripts/build_demo_project.py --out project_data

# 168h smoke (fast end-to-end check)
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h

# 720h baseline
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/gse_720h
```

Open `html/PowerSim_v4.html`, click **📥 Import Results JSON**, select
`out/smoke_168h/powersim_results.json` (or the 720h artifact), and
verify KPI cards, dispatch stack, lambda chart, monthly summary,
diagnostics, and per-asset summary. Use the theme picker to switch
between `gse-light`, `industrial-dark`, and `executive-red`.
