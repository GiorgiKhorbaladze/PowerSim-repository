# PowerSim v1.0 RC Demo Script

This is the suggested final demo flow for Giorgi. It is designed to show the
complete PowerSim loop while avoiding live-demo risk from long optimization
runs.

## Demo objective

Show that PowerSim can start from a clean repository, prepare `project_data/`,
run a solver workflow, open the browser UI, import JSON results, compare
scenarios, and export a report — while clearly stating limitations and remaining
RC blockers.

## Pre-demo preparation

Run these before the live demo and keep the output folders available:

```bash
python scripts/build_demo_project.py --out project_data

python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h

python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/gse_720h

python scripts/run_mc_sweep.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --scenarios   A_mean MC_P10 MC_P50 MC_P90 \
    --out-dir     out/gse_mc_720h
```

Optional annual precompute:

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

## Live demo flow

### 1. Introduce PowerSim

Suggested narration:

> PowerSim is a Georgian power-system decision-support workflow. The browser UI
> prepares and reviews scenarios; the Python solver runs unit commitment and
> dispatch; JSON files are the contract between them.

Open `docs/WHAT_IS_POWERSIM.md` or summarize it verbally.

### 2. Show the repository structure

Point out:

* `html/PowerSim_v4.html` — browser UI, no build step.
* `scripts/build_demo_project.py` — synthetic safe demo data.
* `scripts/run_horizon.py` — one scenario / one horizon.
* `scripts/run_mc_sweep.py` — multi-scenario sweep.
* `docs/RELEASE_CANDIDATE_CHECKLIST.md` — RC checklist and limitations.
* `project_data/` and `out/` — local working folders, not committed.

### 3. Build `project_data/`

Run live if time allows:

```bash
python scripts/build_demo_project.py --out project_data
```

Explain that this creates synthetic 8760-hour profiles and avoids private GSE
data in the demo.

### 4. Run the fast solver smoke test

Run live:

```bash
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h
```

Show the output files:

* `out/smoke_168h/powersim_input.json`
* `out/smoke_168h/powersim_results.json`
* `out/smoke_168h/powersim_results.xlsx`

### 5. Show the 720-hour run command

Either run live if already benchmarked, or show the precomputed output:

```bash
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/gse_720h
```

Narration: rolling horizon makes month-length and annual runs practical while
keeping the JSON artifacts easy to inspect.

### 6. Open the HTML UI

Open the UI:

```bash
xdg-open html/PowerSim_v4.html
```

If `xdg-open` is unavailable, double-click `html/PowerSim_v4.html` or serve the
folder locally:

```bash
python -m http.server --directory html 8000
```

Then browse to `http://localhost:8000/PowerSim_v4.html`.

### 7. Import JSON results

In the HTML UI:

1. Click **📥 Import Results JSON**.
2. Select `out/gse_720h/powersim_results.json`.
3. Show KPI cards.
4. Show dispatch stack and lambda chart.
5. Show monthly summary and diagnostics.

### 8. Compare scenarios

Use the MC sweep outputs:

1. Open **🔀 Compare**.
2. Click **➕ Add Scenario(s)**.
3. Select at least two files, for example:
   * `out/gse_mc_720h/A_mean/powersim_results.json`
   * `out/gse_mc_720h/MC_P90/powersim_results.json`
4. Click **▶ Compare**.
5. Discuss cost, gas, unserved energy, and average lambda deltas.

### 9. Export report

In the Results tab:

1. Click **⬇ Excel** in the Diagnostics card.
2. Save the generated `PowerSim_Results_<date>.xlsx` workbook.
3. Mention that the Python run also writes `powersim_results.xlsx` directly in
   each output folder.

### 10. Close with limitations and blockers

State clearly:

* PowerSim is not a full PLEXOS clone.
* Network constraints are limited.
* Hydro is simplified.
* BESS/reservoir carry-over needs final sign-off unless already audited on the
  final candidate.
* Real GSE validation is the main blocker before final RC tag.
* Market assumptions are model inputs, not audited forecasts.

## Recommended final demo path

For the smoothest demo, run only the synthetic data build and 168-hour smoke
case live. Use precomputed 720-hour, MC sweep, and optional 8760-hour result JSON
files for the browser import, comparison, and report export sections.
