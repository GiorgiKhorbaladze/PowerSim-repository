# PowerSim — Happy Path (5 minutes from clone to first results)

This is the shortest journey from `git clone` to a rendered results dashboard.

```
┌──────────┐    ┌──────────┐    ┌─────────────┐    ┌────────────┐    ┌──────────┐
│ HTML UI  │ ─▶ │ input.json│ ─▶ │ Python      │ ─▶ │ results.   │ ─▶ │ HTML UI  │
│ (browser)│    │ (export)  │    │ solver      │    │ json       │    │ (results)│
└──────────┘    └──────────┘    └─────────────┘    └────────────┘    └──────────┘
```

You only do steps 1 and 5. Everything in the middle is automated.

---

## 0. Install (~30 s)

```bash
git clone <this repo> powersim && cd powersim
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 1. Try the demo fleet (no GSE files needed)

If you don't yet have access to the proprietary GSE installed-capacity
workbook and inflow CSVs, run the synthetic project_data generator first:

```bash
python scripts/build_demo_project.py --out project_data
```

This writes 16 hydro zones × 4 scenarios + 10 RE sites × 4 scenarios + a
demand shape XLSX into `project_data/`. The values are realistic but
synthetic — replace this folder with the real GSE files for production.

## 2. Run a 168-hour smoke test (~5 s)

```bash
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h
```

Expected last line:

```
🎉  Stage 1 smoke test PASSED — HTML → JSON → solver → JSON → HTML round-trip is intact.
```

You now have:
* `out/smoke_168h/powersim_input.json`   — schema v1.2 input
* `out/smoke_168h/powersim_results.json` — schema v1.2 results
* `out/smoke_168h/powersim_results.xlsx` — multi-sheet Excel report

## 3. Run a longer horizon (~10 s for 720h, ~60 s for 8760h)

```bash
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/run_720h
```

Switch `--hours 8760` for the full year and bump `--mip-gap 0.03` to keep
runtime well under a minute on a single core.

## 4. Run a P10 / P50 / P90 Monte-Carlo sweep (~30 s for 720h × 4 scenarios)

```bash
python scripts/run_mc_sweep.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --hours       720 \
    --scenarios   A_mean MC_P10 MC_P50 MC_P90 \
    --out-dir     out/mc_sweep_720h
```

Each scenario writes its own subdirectory; `mc_sweep_720h/mc_summary.json`
aggregates total_cost / avg_lambda / unserved / gas with P10/P50/P90
percentiles.

## 5. View results in the HTML UI

1. Open `html/PowerSim_v4.html` in any modern browser (just double-click,
   or `python -m http.server` from `html/` if you prefer).
2. Click the **📥 Import Results JSON** button (Workflow tab, Validate
   tab, or Results tab — they all wire to the same handler).
3. Pick `out/run_720h/powersim_results.json`.
4. The Results tab opens with KPIs, hourly stack, lambda chart, monthly
   bar, and a per-asset summary table.

For scenario comparison (e.g. P10 vs P90), use the **🔀 Compare** tab and
load two results JSONs.

---

## Switching to your own data

Replace the demo `project_data/` with the real files (same names):

```
project_data/
├── 2026_A_historical_mean.csv      ← 16 hydro zones, ;-sep, comma-decimal
├── 2026_B_montecarlo_P10.csv
├── 2026_B_montecarlo_P50.csv
├── 2026_B_montecarlo_P90.csv
├── Solar_<Site>_2026_<Scenario>.csv  (10 sites × 4 scenarios = 40 files)
├── Wind_<Site>_2026_<Scenario>.csv
└── GSE_CharYear_Normalized_1.xlsx    ← demand shape, sheet 'CharYear_Normalized_8760'
```

…and (optionally) point the asset mapper at the GSE installed-capacity
workbook to auto-build the 131-plant config:

```bash
python solver/powersim_asset_mapper.py \
    --excel /path/to/დადგმული_სიმძლავრე__2026.xlsx \
    --hydro-overrides tests/reservoir_overrides_template.csv \
    --out tests/full_fleet_2026.json
```

then point `--config tests/full_fleet_2026.json` at every script above.

---

## What the user does vs. what's automated

| You | Automated |
|-----|-----------|
| Click "Export Input JSON" in HTML | Schema v1.2 validation, hashing, time index, derived profile keys |
| Run one Python script | Asset preprocessing, gas constraint pro-rating, rolling-horizon decomposition, MIP UC + LP ED resolve, closure check, output validation, Excel export |
| Click "Import Results JSON" in HTML | Output validation, KPI cards, dispatch chart, lambda chart, monthly bar, per-asset table |

Nothing else needs hand-tuning to get a first result.
