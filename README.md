# PowerSim v4.0 — Georgian Power System Simulation Platform

Decision-support platform for the Georgian electricity system. End-to-end
unit-commitment + economic-dispatch (UC/ED), 1-hour resolution, 8760h
non-leap year, Asia/Tbilisi calendar.

```
                      ┌───────────────┐                     ┌──────────────────┐
                      │  HTML UI      │                     │  Python solver   │
   user input      → │  Export Input │── powersim_input ──▶│  pyomo + highs   │
   (browser)         │   JSON        │     (schema v1.2)   │  rolling horizon │
                      │               │◀── powersim_results │                  │
   user views      ←  │  Import Results JSON                │                  │
                      └───────────────┘                     └──────────────────┘
```

The HTML manages **inputs and visualisation**. The Python side runs the
**MIP solver**. The two never share a process — they only share two JSON
files. See [`docs/JSON_HANDOFF.md`](docs/JSON_HANDOFF.md) for the contract.

---

## What's in this repo

```
.
├── html/
│   └── PowerSim_v4.html          ← open in any browser (no build step)
├── solver/
│   ├── powersim_solver.py        ← MIP UC/ED, rolling horizon (Pyomo + HiGHS)
│   ├── powersim_dataio.py        ← raw GSE files → input JSON
│   └── powersim_asset_mapper.py  ← installed-capacity workbook → asset list
├── schema/
│   └── powersim_schema.py        ← v1.2 input + output validators
├── tests/
│   ├── smoke_168h.py             ← end-to-end CI smoke test
│   ├── stage1_smoke_fleet.json   ← 8-asset minimal fleet (CI only)
│   ├── gse_2026_baseline.json    ← 27-asset GSE 2026 fleet, v10 LOCKED
│   └── reservoir_overrides_template.csv
├── scripts/
│   ├── build_demo_project.py     ← synthesize project_data/ for trials
│   ├── run_horizon.py            ← single-scenario horizon run
│   └── run_mc_sweep.py           ← P10/P50/P90 MC sweep
├── samples/
│   ├── sample_input_168h.json                  ← smoke fleet input
│   ├── sample_input_gse_2026_168h.json         ← GSE 2026 baseline input
│   ├── sample_results_168h.json                ← smoke fleet results
│   ├── sample_results_720h.json
│   ├── sample_results_gse_2026_720h.json       ← GSE baseline 720h results
│   ├── sample_mc_summary_720h.json
│   └── sample_mc_summary_gse_720h.json         ← GSE 4-scenario MC summary
├── docs/
│   ├── HAPPY_PATH.md             ← clone → result in 5 minutes
│   ├── JSON_HANDOFF.md           ← input/output schema reference
│   ├── COLAB.md                  ← run on Google Colab
│   └── TROUBLESHOOTING.md
├── requirements.txt
└── README.md
```

`project_data/` and `out/` are local working folders — kept out of git.

---

## Quick start (5 minutes)

```bash
git clone <this repo> powersim && cd powersim
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. synthetic data so you can try without GSE files
python scripts/build_demo_project.py --out project_data

# 2. 168-hour CI smoke test (≈5 s, 8-asset minimal fleet)
python tests/smoke_168h.py \
    --project-dir project_data \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs out/smoke_168h

# 3. or — full GSE 2026 baseline 168h (≈35 s, 27 assets, v10 LOCKED)
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       168 \
    --out-dir     out/gse_168h

# 4. open the HTML
xdg-open html/PowerSim_v4.html      # or just double-click
# → click "🏭 GSE 2026 Demo ჩატვირთვა"  (loads the same 27-asset fleet)
# → click "📥 Import Results JSON" → pick out/gse_168h/powersim_results.json
```

Full walkthrough: [`docs/HAPPY_PATH.md`](docs/HAPPY_PATH.md).
Colab walkthrough: [`docs/COLAB.md`](docs/COLAB.md).

---

## Running real horizons

### Reference numbers (GSE 2026 baseline, demo project_data)

These should be reproducible verbatim from a clean clone — they are the
acceptance check for the v10 LOCKED calibration on the demo project_data.

| Horizon | Fleet | Wallclock | Total cost | Avg λ | Unserved | Gas |
|---------|-------|-----------|------------|-------|----------|-----|
| 168h    | 27 assets | 35 s  | $9.9 M    | $37.12/MWh | 0 MWh | 20.8 Mm³ |
| 720h    | 27 assets | 44 s  | $66.2 M   | $49.04/MWh | 0 MWh | 87.3 Mm³ |
| 8760h   | 27 assets | 217 s | $780.9 M  | $52.27/MWh | 0 MWh | 1069.6 Mm³ |
| MC sweep 720h × 4 | 27 assets | 180 s | P10/P50/P90 ≈ $66.14M / $66.20M / $66.22M | — | — | gas-cap binding |

Annual gas usage (1069.6 Mm³) is within the 1170 Mm³ cap; closure_gap ≈ 0%
on every run; all four MC scenarios converge to optimal.

### 720-hour run (1 month)

```bash
python scripts/run_horizon.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --mip-gap     0.02 \
    --rolling-window 168 --rolling-step 168 \
    --out-dir     out/gse_720h
```

Rolling horizon: 720 h ÷ 168 h windows × 168 h step = 5 windows. Each
window's terminal storage / commitment carries over to the next.

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

### Monte-Carlo P10 / P50 / P90 sweep

```bash
python scripts/run_mc_sweep.py \
    --project-dir project_data \
    --config      tests/gse_2026_baseline.json \
    --hours       720 \
    --scenarios   A_mean MC_P10 MC_P50 MC_P90 \
    --out-dir     out/gse_mc_720h
```

Each scenario writes its own subdirectory; `mc_sweep_720h/mc_summary.json`
aggregates `total_cost_usd`, `avg_lambda_usd_mwh`, `total_unserved_mwh`,
`total_gas_mm3` with **P10 / P50 / P90 percentiles** of the cost
distribution across the requested scenarios.

### Full-fleet (131-plant) build from the GSE workbook

```bash
python solver/powersim_asset_mapper.py \
    --excel /path/to/დადგმული_სიმძლავრე__2026.xlsx \
    --hydro-overrides tests/reservoir_overrides_template.csv \
    --out tests/full_fleet_2026.json
```

Then point any of the scripts above at `--config tests/full_fleet_2026.json`.

---

## What the user does vs. what's automated

| You | Automated |
|-----|-----------|
| Open HTML, enter / import inputs, click **Export Input JSON** | Schema v1.2 validation, time index generation, derived profile keys, SHA-256 fingerprints |
| Run one Python script (locally or in Colab) | Asset preprocessing, gas-constraint pro-rating, rolling-horizon decomposition, MIP UC + LP ED resolve, closure check, output validation, Excel export |
| Open HTML, click **Import Results JSON** | Output validation, KPI cards, dispatch chart, lambda chart, monthly bar, per-asset summary |

That's the whole product loop.

---

## Calibration baseline (v10 LOCKED — do not change without justification)

| Parameter | Value |
|-----------|-------|
| Hydro pmax derate | 0.65 (inflow-driven only; Section I excluded) |
| Gas fuel price | $5.50/MMBtu |
| Gas startup / no-load | $1000 / $50 |
| Coal startup / no-load | $4000 / $100 |
| Thermal pmin | 25% of pmax |
| Section-I water values | Enguri/Vardnili $35, Zhinvali/Khrami $42, Shaori/Dzevrula $37 |
| Section-I end-level target | 85% of `reservoir_max` |
| Section-I end-level penalty | $20/Mm³ |
| Cascade Enguri → Vardnili | delay = 2 h, gain = 0.97 |
| Cascade Khrami_1 → Khrami_2 | delay = 1 h, gain = 0.95 |
| Monthly gas caps | annual 1170 Mm³ (winter ~180, summer ~40) |

These are encoded in the asset mapper and reservoir override template.

---

## Versioning

| Component | Version | Source of truth |
|-----------|---------|-----------------|
| Schema    | 1.2     | `schema/powersim_schema.py::SCHEMA_VERSION` (1.0 / 1.1 still load with a warning) |
| Loader    | 1.0.1   | `solver/powersim_dataio.py::LOADER_VERSION` |
| Solver    | 1.1.0   | `solver/powersim_solver.py::SOLVER_VERSION` |
| HTML UI   | 1.2     | `html/PowerSim_v4.html::SCHEMA_VERSION` |

---

## Troubleshooting

See [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) for installation,
schema, solver, and HTML issues.
