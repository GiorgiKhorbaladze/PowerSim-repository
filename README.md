# PowerSim v4 — UC/ED Research Baseline

PowerSim is an hourly unit commitment / economic dispatch (UC/ED) simulation stack for the Georgian power system.
It includes:
- thermal commitment and dispatch,
- reservoir + run-of-river hydro,
- hydro cascade links and travel delay,
- gas constraints (annual/monthly),
- renewable profiles,
- schema-validated JSON I/O,
- HTML results import compatibility.

This repository is now organized for a **single canonical Python workflow** usable in local environments, Google Colab, and future CI.

---

## 1) Repository structure

Current repository layout is a flat root:

```
.
├── powersim_solver.py          # Main UC/ED solver CLI (Pyomo + HiGHS)
├── powersim_dataio.py          # Project data loaders -> solver input JSON
├── powersim_asset_mapper.py    # Installed-capacity workbook -> asset config
├── powersim_schema.py          # Input/output schema + validators
├── smoke_168h.py               # End-to-end smoke test (168h pipeline)
├── run_simulation.py           # Canonical single-run pipeline (dataio->solver->validate)
├── decision_grade_run.py       # Batch runner: MC (P10/P50/P90) + annual
├── powersim_report.py          # Comparative reporting + cascade/reservoir checks
├── stage1_smoke_fleet.json     # Baseline 8-asset config
├── PowerSim_v4.html            # UI for import/export and visualization
├── requirements.txt            # Python dependencies
└── README.md
```

Notes:
- Historical docs/scripts may refer to `solver/`, `schema/`, and `tests/` folders.
- Runtime scripts in this repo support both historical and flat layouts for key paths.

---

## 2) Setup

### Python requirements
- Python 3.10+
- HiGHS via `highspy`

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

If you prefer explicit packages:

```bash
python3 -m pip install pyomo highspy pandas numpy openpyxl xlsxwriter
```

---

## 3) Canonical project data layout

Set one project data directory (recommended: `./project_data`) and place source files under it.

Minimal expected inputs:

```
project_data/
├── 2026_A_historical_mean.csv          # hydro scenario A_mean
├── 2026_MC_P10.csv                     # optional
├── 2026_MC_P50.csv                     # optional
├── 2026_MC_P90.csv                     # optional
├── Demand_CharYear.xlsx                # demand shape workbook
├── generation-2026-forecast-v13.xlsx   # demand absolute workbook
├── wind_tbilisi_A_mean.csv             # renewable site examples
├── solar_tbilisi_A_mean.csv
└── ... other supported source files
```

The loader in `powersim_dataio.py` already probes multiple common subdirectories (flat + nested data folders), but keeping everything under a single root avoids ambiguity.

You can point all commands to this directory with either:
- `--project-dir /path/to/project_data`, or
- `POWERSIM_PROJECT_DIR=/path/to/project_data`.

---

## 4) Canonical execution workflow

## A) 168h smoke / baseline run

```bash
python3 run_simulation.py \
  --project-dir /path/to/project_data \
  --config stage1_smoke_fleet.json \
  --out-dir outputs/smoke_168h \
  --horizon-hours 168
```

This pipeline performs:
1. `build_input_from_project(...)`
2. `validate_input(...)`
3. solver run (`powersim_solver.py`)
4. `validate_output(...)`

Expected outputs:
- `outputs/smoke_168h/powersim_input.json`
- `outputs/smoke_168h/powersim_results.json`
- `outputs/smoke_168h/powersim_results.xlsx`

## B) 720h run

```bash
python3 run_simulation.py \
  --project-dir /path/to/project_data \
  --config stage1_smoke_fleet.json \
  --out-dir outputs/run_720h \
  --horizon-hours 720
```

## C) 8760h run path

```bash
python3 run_simulation.py \
  --project-dir /path/to/project_data \
  --config stage1_smoke_fleet.json \
  --out-dir outputs/run_8760h \
  --horizon-hours 8760
```

For long runs, tune `solver_settings` in config (e.g., `mip_gap`, `time_limit_s`, rolling-window parameters).

---

## 5) Alternative entry points

### Decision-grade batch run (Monte Carlo + annual)

```bash
python3 decision_grade_run.py \
  --project-dir /path/to/project_data \
  --base-config stage1_smoke_fleet.json \
  --output-root outputs \
  --mc-horizon 720 \
  --annual-horizon 8760 \
  --annual-rolling-window 168 \
  --annual-rolling-step 24
```

This writes:
- `outputs/mc/*` for scenario runs,
- `outputs/annual/*` for the annual run,
- `outputs/reports/scenario_comparison.csv` and per-scenario diagnostics.

### Smoke test script

```bash
python3 smoke_168h.py \
  --project-dir /path/to/project_data \
  --config stage1_smoke_fleet.json \
  --keep-outputs outputs/smoke_test
```

### DataIO only (build input JSON)

```bash
python3 powersim_dataio.py \
  --project-dir /path/to/project_data \
  --config stage1_smoke_fleet.json \
  --out outputs/input_only/powersim_input.json
```

### Solver only

```bash
python3 powersim_solver.py \
  --input outputs/input_only/powersim_input.json \
  --output outputs/solver_only/powersim_results.json \
  --excel outputs/solver_only/powersim_results.xlsx
```

---

## 6) Validation story

- Input validation: `powersim_schema.validate_input`
- Output validation: `powersim_schema.validate_output`
- `run_simulation.py` enforces both for single runs.
- `decision_grade_run.py` orchestrates scenario runs and then calls `powersim_report.py`.
- `smoke_168h.py` also validates output shape compatibility expected by HTML import.

If validation fails, scripts print explicit errors and return non-zero exit codes.

---

## 7) Google Colab quickstart

```python
!pip install pyomo highspy pandas numpy openpyxl xlsxwriter
```

Then upload repository files and data, and run:

```python
!python run_simulation.py --project-dir /content/project_data --config stage1_smoke_fleet.json --out-dir /content/outputs_168h --horizon-hours 168
```

---

## 8) CI readiness suggestions

Typical CI checks:

```bash
python3 -m py_compile smoke_168h.py run_simulation.py decision_grade_run.py powersim_report.py powersim_dataio.py powersim_solver.py powersim_schema.py powersim_asset_mapper.py
python3 smoke_168h.py --project-dir /mnt/project --config stage1_smoke_fleet.json --keep-outputs /tmp/powersim_smoke
```

Second command requires runtime dependencies and data files to be present.

---

## 9) Troubleshooting

### `ModuleNotFoundError` (e.g. pandas/pyomo/highspy)
Install dependencies from `requirements.txt` in an activated virtualenv.

### `project dir not found`
Pass `--project-dir` explicitly or set `POWERSIM_PROJECT_DIR`.

### Missing hydro / renewable / xlsx files
Check your project data root and filenames. The loader error message lists accepted candidate names and folders.

### Solver fails to converge or is too slow
Adjust config `solver_settings` (`mip_gap`, `time_limit_s`, rolling horizon settings, threads).

---

## 10) Non-goals of this hardening pass

This baseline intentionally does **not** alter calibration economics (water values, fuel prices, hydro derating, cascade gains, penalties) except where needed for pure runtime robustness.
