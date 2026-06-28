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
