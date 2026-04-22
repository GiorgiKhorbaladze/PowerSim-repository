# PowerSim v4.0 — Georgian Power System Simulation

End-to-end UC/ED platform for the Georgian electricity system. Hourly resolution,
8760h annual horizon, Asia/Tbilisi calendar. Data flow:

    HTML UI  →  config JSON  →  Python solver (Pyomo + HiGHS)  →  results JSON / Excel  →  HTML UI

Stage 2 (current): hydro cascade with travel delay, strategic reservoir end-level
penalties, real per-calendar-month gas caps, pmax-weighted hydro inflow shares,
and dispatch-side hydro derating.

---

## 1. Project structure

```
powersim_v4/
├── solver/
│   ├── powersim_solver.py         (1500+ lines — UC/ED MIP, rolling horizon)
│   ├── powersim_dataio.py         (1000+ lines — Excel / CSV → input JSON)
│   └── powersim_asset_mapper.py   (1400+ lines — installed-capacity → assets)
├── schema/
│   └── powersim_schema.py         (schema v1.2, validate_input/_output)
├── tests/
│   ├── smoke_168h.py              (end-to-end: dataio→validate→solver→validate)
│   ├── stage1_smoke_fleet.json    (8-asset minimal fleet for CI)
│   └── reservoir_overrides_template.csv
├── html/
│   └── PowerSim_v4.html           (browser UI; round-trips with the JSON files)
├── README.md                      (this file)
└── README_stage1.md               (historical Stage 1 narrative — kept for context)
```

---

## 2. Setup

### Requirements

- Python 3.10 or newer (3.12 tested)
- HiGHS solver via Pyomo's appsi backend (bundled when you `pip install` Pyomo)

### Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyomo highspy pandas openpyxl numpy
```

### Project data

The dataio module reads from a project directory containing the GSE/ESCO source
files. Pass it via `--project-dir`. Expected files (any of the supported layouts):

- `2026_A_historical_mean.csv` (or `MC_P10/P50/P90.csv`) — hourly hydro inflow per zone
- Renewable site CSVs (Tbilisi, Kutaisi, Mta_Sabueti, …)
- Demand CharYear / S-curve workbook
- `დადგმული_სიმძლავრე__2026.xlsx` — installed capacity by plant

If the project directory is not at `/mnt/project`, pass `--project-dir /path/to/your/data`
to every CLI in this README.

---

## 3. Running simulations

### 168h smoke test (end-to-end CI)

```bash
cd powersim_v4
python tests/smoke_168h.py \
    --project-dir /mnt/project \
    --config      tests/stage1_smoke_fleet.json \
    --keep-outputs /tmp/smoke_out
```

This runs the full pipeline against an 8-asset minimal fleet:
1. `dataio.build_input_from_project` → produces `powersim_input.json`
2. `schema.validate_input` → must return `ok=True`
3. `solver.powersim_solver.py` → produces `powersim_results.json` and `.xlsx`
4. `schema.validate_output` → must return `ok=True`
5. HTML `importResults()` shape check

Exit code 0 on full success.

### 168h full-fleet run (131 plants)

Build a config from the asset mapper and run the solver directly:

```bash
# Step 1 — Build the asset list from the installed-capacity workbook
python solver/powersim_asset_mapper.py \
    --excel /mnt/project/დადგმული_სიმძლავრე__2026.xlsx \
    --hydro-overrides tests/reservoir_overrides_template.csv \
    --emit-config /tmp/run/config_168h.json

# Step 2 — Build solver input JSON
python solver/powersim_dataio.py \
    --project-dir /mnt/project \
    --config      /tmp/run/config_168h.json \
    --out         /tmp/run/input_168h.json

# Step 3 — Solve
python solver/powersim_solver.py \
    --input  /tmp/run/input_168h.json \
    --output /tmp/run/results_168h.json \
    --excel  /tmp/run/results_168h.xlsx
```

Typical wallclock at `mip_gap=0.01`: 50–80 s on a single core.

### 720h run (1 month)

Same as 168h, but in the config set:

```json
"study_horizon":   { "start_hour": 0, "horizon_hours": 720, "mode": "full" },
"solver_settings": {
    "mip_gap": 0.02,
    "time_limit_s": 600,
    "rolling_window_h": 168,
    "rolling_step_h":   168,
    "unserved_penalty": 3000,
    "curtailment_penalty": 0,
    "threads": 0
}
```

The solver automatically uses rolling-horizon decomposition: 720 h ÷ 168 h window
× 168 h step = 5 windows. Each window's terminal state (storage, BESS SoC,
commitment) is carried forward as initial state for the next window.

Typical wallclock for 720h: 2–4 minutes per scenario.

### MC scenario sweep

Change the `scenario` field to `MC_P10`, `MC_P50`, or `MC_P90` and re-run dataio
+ solver. Hydro inflow profiles will be loaded from the matching CSV.
Calibration (water values, derate factor, etc.) is unchanged across scenarios.

### Annual 8760h run

Set `horizon_hours: 8760` and `rolling_step_h: 168`. The solver will execute
~52 windows. Budget: 30–60 minutes at `mip_gap=0.02`.

---

## 4. Stage 2 features and how to use them

### Hydro cascade with travel delay

Set on a downstream reservoir's `hydro` block:

```json
"hydro": {
    "cascade_upstream":       "engurhesi",
    "cascade_travel_delay_h": 2,
    "cascade_gain":           0.97,
    "..."
}
```

The solver enforces:

    cascade_in[t]  =  cascade_gain × release_upstream[t − travel_delay_h]
    release_upstream[τ]  =  p_upstream[τ] / efficiency_upstream

For `t ≤ travel_delay_h` the upstream contribution is 0 (bootstrap — no
pre-horizon water is modeled). Only turbined water flows downstream — upstream
*spill* is excluded by design to avoid recovering wasted water as energy.

Defaults applied automatically by the asset mapper:

| Downstream | Upstream | Delay (h) | Gain |
|---|---|---:|---:|
| `vardnilhesi` | `engurhesi` | 2 | 0.97 |
| `khramhesi_2` | `khramhesi_1` | 1 | 0.95 |

### Strategic reservoir end-level penalty

Discourages myopic depletion in short-horizon runs by adding a soft penalty
when end storage falls below a target fraction of `reservoir_max`:

```json
"hydro": {
    "target_end_level_frac": 0.85,
    "end_level_penalty":     20.0,
    "..."
}
```

Objective adds `end_level_penalty × max(0, target − stor[T_last])` for each
plant that declares both fields. Section-I reservoirs receive these defaults
automatically.

### Water value (`water_value`)

For reservoir hydro (Section I), the marginal cost is floored by the
`water_value` field ($/MWh). This treats stored water as a priced resource:
the solver will not turbine reservoir water below this opportunity cost.
Set per plant in the override CSV.

### Real per-calendar-month gas caps

```json
"gas_constraints": {
    "mode":   "annual+monthly",
    "unit":   "Mm3",
    "annual": { "cap": 1170 },
    "monthly": {
        "Jan": 180, "Feb": 160, "Mar": 100, "Apr":  60,
        "May":  40, "Jun":  40, "Jul":  60, "Aug":  60,
        "Sep":  60, "Oct":  90, "Nov": 140, "Dec": 180
    },
    "applies_to": ["gardabani_1", "gardabani_2", "..."]
}
```

The solver maps each window hour to its calendar month using the global
`offset_h` and emits one constraint per month touched by the window. For
horizons shorter than a full month, the cap is pro-rated as
`cap_window = cap_full × (hours_in_window / hours_in_month)`.

Month keys may be `"Jan".."Dec"`, `"jan".."dec"`, integers `1..12`, or
strings `"1".."12"`. Annual + monthly caps may be combined.

### Hydro inflow allocation (Layer 1 — pmax-weighted share)

Each plant in a hydrological zone receives `share = pmax / Σ pmax_zone` of
the zone inflow profile. Σ(share) = 1.0 per zone is enforced; the asset mapper
computes this automatically and emits a warning if any zone deviates by more
than 1e-6.

This eliminates the original 7.88× water double-counting that resulted from
every plant claiming `share = 1.0`.

### Hydro pmax derating

Inflow-driven hydro (109 plants) has `pmax_effective = pmax × 0.65` applied
in the asset mapper to reflect maintenance, head loss, intake silting, low-flow
restrictions, and ice. Section-I reservoirs (7 plants) are excluded.

### Calibration baseline (v10 LOCKED)

| Parameter | Value |
|---|---|
| Hydro pmax derate | 0.65 (inflow-driven only) |
| Gas fuel price | $5.50/MMBtu |
| Gas startup / no-load | $1000 / $50 |
| Coal startup / no-load | $4000 / $100 |
| Thermal pmin | 25% of pmax |
| Section-I water values | Enguri/Vardnili $35, Zhinvali/Khrami $42, Shaori/Dzevrula $37 |
| Section-I end-level target | 85% of reservoir_max |
| Section-I end-level penalty | $20/Mm³ |
| Cascade Enguri→Vardnili | delay=2h, gain=0.97 |
| Cascade Khrami_1→Khrami_2 | delay=1h, gain=0.95 |
| Monthly gas caps | annual 1170 Mm³ (winter ~180, summer ~40) |

---

## 5. Output schema highlights

`results.json` produced by the solver:

```
metadata:
    schema_version, scenario, horizon_hours, closure_ok, closure_gap, ...
    data_source_fingerprint: { profile_bundle, input_file_hashes, loader_version }
diagnostics:
    solver_status, solve_time_s, gas_cap_binding, hydro_end_storage_warnings, ...
system_summary:
    total_cost_usd, total_energy_mwh, avg_lambda_usd_mwh, peak_load_mw,
    total_gas_mm3, total_unserved_mwh, total_curtailed_mwh
hourly_system: [ { t, load_mw, lambda_usd_mwh, ... } × H ]
hourly_by_unit: { asset_id: [ { t, dispatch_mw, hydro: {...}, ... } × H ] }
by_unit_summary: { asset_id: { name, type, energy_mwh, capacity_factor, ... } }
```

The HTML UI reads this format directly via its `importResults()` function.

---

## 6. Reference runs (in `tests/`)

| Path | Description |
|---|---|
| `reality_check_v8/` | v8 calibration baseline (Layer 1 + derate 0.65 + thermal v5 + gas $5.50) |
| `stage2_v10_720h/` | 720h Stage 2 with cascade fully active (Enguri+Vardnili wv=$35) |
| `mc_sweep_720h/` | MC_P10 / P50 / P90 robustness sweep at v10 calibration |

Each contains `config_*.json`, `results_*.json`, `results_*.xlsx`.

---

## 7. Troubleshooting

- **HiGHS import error**: `pip install --upgrade pyomo highspy`
- **`raw` hydro unit unverified warning**: expected — the solver treats raw values as Mm³/h
- **Slow solves on dry MC scenarios**: tighten initial state or loosen `mip_gap` to 0.02
- **`closure_gap` > 1%**: re-run with `mip_gap` ≤ 0.005 — closure tracks LP optimality
- **Georgian filenames not loading**: ensure your filesystem and Python locale support UTF-8

---

## 8. Versioning

- Schema: `1.2` (Stage 2 — current). Accepts `1.0` and `1.1` configs with a warning.
- Loader: see `LOADER_VERSION` in `solver/powersim_dataio.py`
- Solver: see `MODEL_VERSION` in `schema/powersim_schema.py`
