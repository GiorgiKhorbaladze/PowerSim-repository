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
