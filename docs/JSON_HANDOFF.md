# PowerSim — JSON Handoff Contract (HTML ⇄ Python)

PowerSim is intentionally split in two halves:

* **HTML UI** (`html/PowerSim_v4.html`) — runs in any modern browser. It only
  manages **inputs** (assets, profiles, reserves, gas, horizon) and renders
  **results**. It does NOT optimize.
* **Python solver** (`solver/`) — UC/ED MIP using Pyomo + HiGHS. Reads an
  input JSON, writes a results JSON (and Excel).

The two sides exchange exactly two artifacts:

```
                ┌───────────────┐                     ┌──────────────────┐
                │  HTML UI      │                     │  Python solver   │
   user input  →│  Export Input │── powersim_input ──▶│  pyomo + highs   │
                │   JSON        │                     │  rolling horizon │
                │               │◀── powersim_results │                  │
   user views  ←│  Import Results JSON                │                  │
                └───────────────┘                     └──────────────────┘
```

Both files use schema **v1.2**. Older versions are accepted with a warning
(see `schema/powersim_schema.py::ACCEPTED_SCHEMA_PRIOR`).

---

## Input JSON — `powersim_input.json`

Top-level keys (required unless noted):

| Key                | Type   | Notes |
|--------------------|--------|-------|
| `metadata`         | dict   | `model_version`, `schema_version`, `timezone`, `study_year` |
| `time_index`       | list   | 8760 strings `"YYYY-MM-DD HH:MM"` (Asia/Tbilisi) |
| `study_horizon`    | dict   | `start_hour`, `horizon_hours`, `mode` (`"full"` \| `"rolling"` \| `"auto"`) |
| `assets`           | list   | Each asset has `id`, `type` ∈ `thermal`, `hydro_reg`, `hydro_ror`, `wind`, `solar`, `import`, `bess`, plus type-specific fields |
| `profiles`         | dict   | `demand` (length 8760, MW) plus optional derived keys (`_hydro_*`, `_wind_*`, `_solar_*`) |
| `gas_constraints`  | dict   | `mode` ∈ `none`/`annual`/`monthly`/`annual+monthly`, `annual.cap`, `monthly[Jan..Dec]`, `applies_to: [asset_id]` |
| `reserve_products` | list   | `id`, `direction`, `requirement`, `shortfall_penalty`, `eligible_units` |
| `solver_settings`  | dict   | `mip_gap`, `time_limit_s`, `rolling_window_h`, `rolling_step_h`, `unserved_penalty`, `curtailment_penalty`, `threads` |
| `scenario_metadata`| dict   | `id` ∈ `A_mean`/`MC_P10`/`MC_P50`/`MC_P90`, `label`, `probability` |
| `profile_bundle`   | dict   | (v1.1+) provenance: `scenario_id`, `hydro_inflow_unit`, file SHA-256 fingerprints |
| `hydro_zone_map`   | dict   | (v1.1+) `{asset_id: {zone, share, scaling_mw}}` — auto-binds zone inflow CSVs |
| `re_site_map`      | dict   | (v1.1+) `{asset_id: {source: "wind"\|"solar", site}}` — auto-binds RE CSVs |
| `demand_spec`      | dict   | (v1.1+) `mode: "shape_times_annual"` + `shape_profile_key` + `annual_twh`, OR `mode: "absolute"` + `absolute_profile_key` |

A complete worked example: `samples/sample_input_168h.json`.

### v1.2 hydro additions (Stage 2)

On any `hydro_reg`/`hydro_ror` asset, the `hydro` sub-dict may carry:

```json
"hydro": {
  "reservoir_init":        700.0,
  "reservoir_min":         100.0,
  "reservoir_max":        1100.0,
  "reservoir_end_min":     500.0,
  "efficiency":            350.0,
  "water_value":            35.0,
  "cascade_upstream":   "engurhesi",
  "cascade_travel_delay_h":     2,
  "cascade_gain":            0.97,
  "target_end_level_frac":   0.85,
  "end_level_penalty":      20.0
}
```

Validation (`schema.validate_input`) enforces:

* `cascade_upstream` references an existing asset; not self.
* `cascade_travel_delay_h ∈ [0, 168]` integer.
* `target_end_level_frac ∈ [0, 1]`; `end_level_penalty ≥ 0`.

### v1.5 hydro Stage 1 additions

The reservoir balance is always written in **Mm³/h**. Source profiles
can arrive in any of the units listed in
`profile_bundle.hydro_inflow_unit`; the solver normalizes once at load
time via `normalize_hydro_inflow_rate()`.

| Unit          | Treatment                                                 |
| ------------- | --------------------------------------------------------- |
| `Mm3_per_h`   | unchanged                                                 |
| `m3_per_s`    | `value * 3600 / 1_000_000`                                |
| `raw`         | unchanged (legacy assumption; emits `hydro_raw_unit_warning`) |
| `normalized`  | scaled by `hydro.inflow_scale_mm3h` or `hydro.annual_inflow_mm3 / 8760` (whichever is present) |

The `raw` default exists only for backward compatibility. Final GSE-style
validation runs should declare the actual source unit.

Additional optional `hydro` sub-fields (all default-safe, additive):

```json
"hydro": {
  "spill_cost_usd_per_mm3":  100.0,
  "cascade_flow_mode":   "release_plus_spill",
  "annual_inflow_mm3":       3200.0,
  "inflow_scale_mm3h":          1.5
}
```

* **`spill_cost_usd_per_mm3`** (or legacy `spill_cost`) — `$/Mm³` penalty
  added to the objective per unit of `spill[h,t] * dt`. Default `0.0`.
* **`cascade_flow_mode`** — for downstream reservoirs with
  `cascade_upstream` set:
    * `"turbined_only"` (default) — `cascade_in = gain * upstream_release`.
    * `"release_plus_spill"` — `cascade_in = gain * (upstream_release + upstream_spill)`.
* **`annual_inflow_mm3`** / **`inflow_scale_mm3h`** — required when
  `hydro_inflow_unit = "normalized"` (else the conversion is a no-op and
  the diagnostic block lists the asset under `missing_scale_for`).

Results-side diagnostics added under `diagnostics`:

* `hydro_inflow_unit_used` — the declared source unit.
* `hydro_inflow_conversion_applied` — `true` when at least one profile was rescaled.
* `hydro_raw_unit_warning` — `true` iff `unit == "raw"`.

Per-reservoir aggregates added to `by_unit_summary[hydro_reg_id]`:

* `total_spill_mm3` — sum of `spill_mm3h * dt` over the horizon.
* `total_hydro_release_mm3` — sum of `release_mm3h * dt` over the horizon.

### GSE 2026-2030 hourly load projection

A committed JSON snapshot of the GSE PLEXOS sc2 P50-central hourly
load projections lives at ``data/gse_load_2026_2030.json`` (~340 KiB,
5 years × 8760 h). The upstream Excel workbooks are in ``load
2026-2030.rar`` at the repo root; re-run ``python
scripts/load_gse_projections.py`` to rebuild the JSON after replacing
the .rar (needs ``unrar-free`` / ``unrar`` / ``7z`` on PATH, or the
``rarfile`` PyPI package).

```python
from solver.load_dataio import (
    snapshot_years, load_snapshot_year, load_snapshot,
)

years = snapshot_years()               # [2026, 2027, 2028, 2029, 2030]
inp["profiles"]["demand"] = load_snapshot_year(2030)
```

Annual targets from the snapshot:

| Year | Target GWh | Peak MW |
|------|-----------:|--------:|
| 2026 | 15,621     | 2,645   |
| 2027 | 16,128     | 2,731   |
| 2028 | 16,694     | 2,827   |
| 2029 | 17,406     | 2,947   |
| 2030 | 18,060     | 3,058   |

The HTML UI has an equivalent uploader in the Profiles tab
("📂 GSE Load Excel ატვირთვა") that:

* accepts one or many .xlsx files,
* auto-detects the first sheet/column that looks like an 8760-h MW
  series (positive, ≤ 100 000 MW, non-flat),
* extracts a 4-digit year from the filename and stores as
  ``STATE.profiles['demand_YYYY']`` when multiple files are dropped,
* aliases the newest year to ``STATE.profiles['demand']`` so the
  solver picks it up unchanged.

Solver-side, ``solver.load_dataio.autodetect_hourly_demand(path)`` runs
the same heuristic for arbitrary GSE-style workbooks (not just the
committed sc2 set).

### PyPSA-Eur cost / efficiency database

A curated snapshot of validated technology costs from
[PyPSA technology-data](https://github.com/PyPSA/technology-data) is
committed at `data/cost_database_pypsa_eur.json` (~120 KiB, CC-BY 4.0,
4 snapshot years × 41 technologies × {investment / FOM / VOM /
efficiency / lifetime / fuel / CO₂ intensity / electricity-input}).

This is reference data, not optimizer input — use it to fill the
HTML asset-editor or solver JSON with realistic numbers instead of
guessing.

```python
from solver.cost_database import (
    available_technologies, get, annualized_capex_usd_per_mw_yr,
)

# Browse what's in the snapshot for 2030
techs = available_technologies(2030)

# Per-parameter lookup
ccgt_inv_eur_per_kw = get("CCGT", 2030, "investment")
ccgt_efficiency    = get("CCGT", 2030, "efficiency")
ccgt_lifetime_yrs  = get("CCGT", 2030, "lifetime")

# Annualized CAPEX for BESS sizing inputs
bess_usd_per_mw_yr = annualized_capex_usd_per_mw_yr(
    "battery storage", 2030, eur_to_usd=1.08, discount_rate=0.07,
)
```

CLI:

```bash
python scripts/cost_lookup.py --list
python scripts/cost_lookup.py --tech CCGT --year 2030
python scripts/cost_lookup.py --tech "battery storage" --annualize
```

Refresh from upstream (fetches all four CSVs, ~5 s):

```bash
python scripts/refresh_cost_database.py
python scripts/refresh_cost_database.py --years 2025 2030 2035 2040 2045
```

### v1.5 wind Stage 5 additions

Three optional wind-only fields layered on top of the generic VRE
stack from Stage 4. All default-safe — absence ⇒ unchanged behavior.

```json
"wind_unit": {
  "type":              "wind",
  "pmax_installed":     200.0,
  "availability_profile": "wind_cf",
  "wake_loss_frac":      0.08,
  "monthly_availability_factor":
    [0.92, 0.95, 1.00, 1.00, 0.97, 0.95,
     0.92, 0.93, 0.97, 1.00, 0.98, 0.93],
  "air_density_correction": true,
  "density_ref_temp_c":   15.0,
  "temp_profile_key":     "tbilisi_amb_temp"
}
```

Composed inside `_wind_extras_factor` and folded into `get_pmax_t` for
wind units only:

```
factor = (1 − wake_loss_frac)
       × monthly_availability_factor[month_of(t)]
       × (273.15 + density_ref_temp_c) / (273.15 + amb_temp[t])
```

* **`wake_loss_frac`** — constant farm-level loss (typical 0.05–0.15
  for arrayed farms). Default `0` ⇒ no derate.
* **`monthly_availability_factor`** — 12 entries (Jan…Dec) captured
  via the existing `_MONTH_END_HOURS` table. Useful for icing,
  seasonal maintenance, or planned-outage patterns binned by month.
  Any entry may be `null` to skip.
* **`air_density_correction`** — boolean. When true, the existing
  `temp_profile_key` is interpreted as ambient °C and wind power is
  scaled by `(T_ref_K) / (T_amb_K)` (physically: cold air is denser,
  so a turbine delivers more power; default reference 15 °C / 288.15 K
  per IEC standard).

Note: the full-horizon solver call now passes `study_horizon.start_hour`
into `solve_window.offset_h` so the monthly mask, maintenance windows,
and storage targets all use the correct global hour-of-year alignment
when the study does not start at midnight on Jan 1.

### v1.5 solar / VRE Stage 4 additions

Three optional `solar`/`wind` fields close the highest-impact PLEXOS PV
gaps. All default-safe — absence ⇒ unchanged behavior.

```json
"solar_unit": {
  "type":               "solar",
  "pmax_installed":      100.0,
  "availability_profile": "solar_cf",
  "dc_ac_ratio":           1.35,
  "inverter_efficiency":   0.97,
  "degradation_rate_per_year": 0.005,
  "commissioning_year":   2026,
  "temp_derating_curve":  [[25, 1.0], [45, 0.90]],
  "temp_profile_key":     "tbilisi_amb_temp"
}
```

The LP upper bound on `m.p[g,t]` for a wind/solar unit becomes:

```
pmax_installed
  · min(cf[t] · dc_ac_ratio, 1.0)             ← clipping at AC rating
  · inverter_efficiency                        ← AC-side losses
  · (1 − degradation_rate)^(study_year − commissioning_year)  ← multi-year aging
  · temp_derating(amb_temp[t])                 ← ambient °C derating
  · maint_factor(t)                            ← maintenance windows
```

* **`dc_ac_ratio`** — DC array MW divided by AC inverter MW (typical
  modern PV 1.2–1.4). The availability profile is interpreted as DC
  capacity factor and the product is clipped at the AC rating; default
  `1.0` ⇒ no clipping (legacy semantics).
* **`inverter_efficiency`** — AC-side multiplier; default `1.0`.
* **`degradation_rate_per_year`** — annual capacity loss (typical
  0.004–0.008 for crystalline Si). Compounded once in `build_asset_map`
  using `metadata.study_year − commissioning_year` and stored on the
  asset as `_degradation_factor` so `get_pmax_t` stays O(1).
* **`commissioning_year`** — integer year for the degradation start;
  defaults to `metadata.study_year` (no degradation applied).
* **`temp_derating_curve` + `temp_profile_key`** — same shape as the
  thermal Stage 3 fields. The solver helper `_temp_factor` already
  worked for any asset type; schema validation is now extended to
  wind/solar too.

### v1.5 thermal Stage 3 additions

Three optional `thermal.*` fields close the highest-impact PLEXOS gaps
for thermal modeling. All default-safe — absence ⇒ unchanged behavior.

```json
"thermal_unit": {
  "co2_factor_t_per_mwh":     0.381,
  "startup_cost_hot":         500.0,
  "startup_cost_cold":       8000.0,
  "hot_start_threshold_h":       3,
  "temp_derating_curve":  [[15, 1.00], [25, 1.00], [35, 0.85], [45, 0.72]],
  "temp_profile_key":     "tbilisi_amb_temp"
}
```

Plus the top-level `co2_price_usd_per_t` (default `0`) which charges
each thermal unit's emissions in the LP objective.

* **`co2_factor_t_per_mwh`** — per-MWh CO₂ emissions of the unit
  (typical CCGT on natural gas ≈ 0.38 t/MWh). Combined with the
  system-wide `co2_price_usd_per_t` the objective adds
  `co2_factor · p[g,t] · dt · price`. Output: `by_unit_summary[gid].total_co2_t`
  / `co2_cost_usd` and `system_summary.total_co2_t` / `total_co2_cost_usd`.
* **`startup_cost_hot` + `startup_cost_cold` + `hot_start_threshold_h`** —
  two-bucket startup pricing. A new binary `y_hot[g,t]` is eligible only
  if the unit was committed within the last `hot_start_threshold_h`
  periods (window history plus init-state `periods_on` for rolling
  boundaries). When all three fields are set the LP picks `hot` whenever
  feasible; the residual `y - y_hot` pays `cold`. The legacy
  `startup_cost` field still applies to units without the multi-stage
  fields. `by_unit_summary[gid].startup_cost` reconstructs the cost
  using the per-period `hourly_by_unit[gid].startup_hot` indicator.
* **`temp_derating_curve` + `temp_profile_key`** — piecewise-linear
  capacity multiplier vs ambient °C. The profile referenced by
  `temp_profile_key` (an 8760-h profile in `profiles`) is interpolated
  per period and the result multiplies `pmax` inside `get_pmax_t` —
  alongside the maintenance derating — so it applies cleanly to the
  LP upper bound on `m.p[g,t]`.

### v1.5 hydro Stage 2 additions

Three optional fields close the most-impactful gaps versus PLEXOS-style
hydro modeling. All default-safe — absence ⇒ unchanged behavior.

```json
"hydro": {
  "min_release_mm3h":           0.090,
  "storage_targets": {
    "month_end":  [600,550,500,550,700,850,1000,1100,1050,950,850,750],
    "penalty_usd_per_mm3":      50.0
  },
  "head_efficiency_curve": [
    [reservoir_min,        260.0],
    [reservoir_max * 0.4,  320.0],
    [reservoir_max * 0.8,  370.0],
    [reservoir_max,        400.0]
  ]
}
```

* **`min_release_mm3h`** — mandatory continuous flow (release + spill) per
  hour for environmental / sanitary / downstream-water-rights compliance.
  Default `0`. The constraint is `release_mm3h + spill_mm3h ≥ min_release_mm3h`
  for every period. Both turbined release and bypass count toward the
  ecological requirement.
* **`storage_targets.month_end`** — list of 12 entries (Jan…Dec) giving
  the desired reservoir level at the last hour of each calendar month
  (`Mm³`). Any entry may be `null` to skip that month. The penalty
  (`storage_targets.penalty_usd_per_mm3`) is applied per `Mm³` of
  shortfall in the objective. Only month-ends that fall inside the
  current window are enforced — full-year horizons see all 12, rolling
  windows see whatever boundaries they cover.
* **`head_efficiency_curve`** — list of `[stor_mm3, eff_mwh_per_mm3]`
  breakpoints (strictly increasing in `stor_mm3`). The solver picks the
  effective efficiency by piecewise-linear interpolation at the
  reservoir's storage at the start of each window, then holds it
  constant inside the window for LP-friendliness. Rolling-horizon runs
  naturally refresh it every step, so seasonal head variation flows
  through. When the curve is absent, the solver falls back to the
  constant `hydro.efficiency`.

---

## Results JSON — `powersim_results.json`

Required top-level keys (`schema.validate_output`):

| Key               | Type | What it contains |
|-------------------|------|------------------|
| `metadata`        | dict | `schema_version`, `scenario`, `horizon_hours`, `solved_at`, `closure_ok`, `closure_gap`, `data_source_fingerprint` |
| `diagnostics`     | dict | `solver_status`, `solve_time_s`, `gas_cap_binding`, `gas_utilization_pct`, `hydro_end_storage_warnings`, `output_schema_ok` |
| `system_summary`  | dict | `total_cost_usd`, `total_energy_mwh`, `avg_lambda_usd_mwh`, `peak_load_mw`, `total_gas_mm3`, `total_unserved_mwh`, `total_curtailed_mwh` |
| `hourly_system`   | list | Length = `horizon_hours`. Each row: `t`, `load_mw`, `lambda_usd_mwh`, `unserved_mwh`, `curtailed_mwh`, `gas_mm3h` |
| `hourly_by_unit`  | dict | `{asset_id: [...rows]}`. Each row: `t`, `dispatch_mw`, `commitment`, `startup`, `shutdown`, `reserve_up`, `reserve_down`, `hydro`, `bess` |
| `by_unit_summary` | dict | Per-asset rollup: `name`, `type`, `energy_mwh`, `capacity_factor`, `oper_hours`, `starts`, `gross_cost`, `gas_mm3`, `SRMC` |
| `monthly_summary` | list | Per-calendar-month rollups (only months touched by the horizon) |
| `stochastic_summary` | dict\|null | populated when the solver runs `--stochastic`; otherwise `null` |

A complete worked example: `samples/sample_results_168h.json`.

### Validation

* **Python:** `from powersim_schema import validate_output; ok, errs, warns = validate_output(res)`.
  Exits non-zero in scripts if `ok` is False.
* **HTML:** `psValidateOutput(res)` mirrors the Python check; the Import
  Results dialog rejects malformed files and surfaces the first error.

---

## Round-trip rules

1. **Lengths must match.** `hourly_system` and every `hourly_by_unit[g]` array
   must have exactly `metadata.horizon_hours` rows. Both validators enforce
   this.
2. **Asset id stability.** Once an asset is in `assets`, its `id` is the
   primary key everywhere downstream (results, charts, Excel column names).
   The HTML and asset mapper guarantee unique ASCII-safe ids.
3. **Profile keys are namespaced.** Derived keys produced by the dataio
   loader are underscore-prefixed (`_hydro_engurhesi`, `_solar_Tbilisi`).
   User-uploaded keys (`demand`, `wind_cf`, etc.) carry no prefix.
4. **Schema version coexistence.** Files with `schema_version` `1.0` or
   `1.1` are accepted with a warning; the engine emits `1.2`.
5. **Provenance.** Every results JSON carries
   `metadata.data_source_fingerprint` so a downstream consumer can detect
   that two results came from different inputs even when the headline KPIs
   look identical.

---

## Common round-trip mistakes (and what the validators say)

| Symptom on Import (HTML) | Likely cause |
|--------------------------|--------------|
| `output validation: missing top-level key 'hourly_by_unit'` | Solver crashed mid-run; rerun with the same input |
| `output schema_version 'X' unsupported` | Mixing files from different PowerSim versions |
| `hourly_system length N ≠ metadata.horizon_hours M` | Window size mismatch in rolling horizon — re-run with `rolling_step_h ≤ rolling_window_h` |
| Excel KPIs look right but Compare tab shows zeros | One of the loaded scenarios doesn't have `system_summary` — confirm it's a results JSON, not an input JSON |

## Solver hardening v1.6.0 notes

- BESS can provide reserve up/down subject to discharge/charge headroom and SOC or empty-SOC over `reserve_duration_h` (default 1 hour).
- Eligible reserve assets with unsupported provider types are surfaced in diagnostics (`reserve_eligible_filtered`) rather than silently ignored.
- Rolling-horizon first-period ramp constraints use previous-window dispatch carryover where available.
- `system_summary.total_cost_usd` remains production/gross cost for compatibility; `system_summary.total_objective_cost_usd` and `diagnostics.objective_breakdown` report the full objective and closure gap.
- Stochastic summaries use full scenario result stores/objective costs and warn with `stochastic_profiles_not_switched` when no scenario-specific profiles or overrides are applied.
- Limitations remain: not a full PLEXOS clone, simplified DC-OPF, no AC voltage/reactive power, and no reserve market settlement.

## Adequacy screening handoff

Inputs may optionally include an `adequacy` block with `mode`, `samples`, `seed`, `lole_target_h`, `eens_target_mwh`, `reserve_margin_target_pct`, and `required_storage_duration_h`. Asset fields such as `for_rate`, `capacity_credit`, `firm_capacity_mw`, `outage_model`, and `adequacy_eligible` are optional and backward-compatible.

Run adequacy from the command line:

```bash
python scripts/run_adequacy.py --input powersim_input.json --out-dir out/adequacy --mode deterministic_derated --write-expanded-input
```

The CLI writes `adequacy_summary.json`, optionally `adequacy_expansion_plan.json`, and, when requested, `expanded_input.json`. Expansion candidates under `expansion.mode = "adequacy_screening"` are added greedily by least annualized cost per firm MW until LOLE, EENS, and reserve margin targets pass or candidate limits are exhausted. The expanded input is a handoff artifact for later UC/ED testing; the adequacy workflow does not overwrite the source JSON.
