# PowerSim v4.0 — Phase 4 features (schema v1.4)

This phase adds four features from the gap list I flagged earlier:

| # | Gap | What shipped |
|---|-----|--------------|
| 7 | IIS infeasibility diagnostics | Gurobi-native `computeIIS` when available; HiGHS fallback is an elastic heuristic that never-the-less prints the likely culprit (peak adequacy / reserves / gas / hydro end-state). Enabled via `solver_settings.iis_on_infeasible = true` (or the HTML Horizon-tab checkbox). The result JSON gains a `diagnostics.iis` list; the HTML Results tab now shows a red "Infeasibility Report" card above the diagnostics. |
| 10 | BESS cycle-depth degradation | New optional BESS fields: `cycle_cost_per_mwh` (charge+discharge throughput penalty), `depth_multiplier` (extra cost when SoC below threshold), `soc_deep_threshold`. Implemented as a linearised depth split using a big-M binary linked to SoC. |
| 11 | DR / interruptible loads | New asset type `"dr"`. Fields: `pmax_curtail` MW, `price_per_mwh` $/MWh, `hours_per_year_max` (optional, auto-prorates to window). Appears as supply-side in the balance with its own price; enters `by_unit_summary` with its own gross_cost. |
| 13 | Pumped-storage with head-dependent efficiency | New asset type `"pumped_hydro"`. 2-bin efficiency model: SoC-linked big-M splits gen and pump dispatch into "high head" and "deep head" segments, each with its own `efficiency_*` pair. Mode binary forbids simultaneous generation and pumping. |

Schema version bumps **1.3 → 1.4**; v1.0 – v1.3 inputs still load with a warning.

## One-liners

```bash
# DR example — annual cap 200 hours at 50 MW
{
  "id":"dr_industry","name":"DR — Industrial","type":"dr",
  "pmax_curtail":50,"price_per_mwh":180,"hours_per_year_max":200
}

# Pumped hydro — 150 MW gen, 140 MW pump, 900 MWh, head-dependent η
{
  "id":"ph_site_a","name":"Pumped Hydro A","type":"pumped_hydro",
  "pmax":150,"pump_mw":140,"energy_mwh":900,
  "soc_init":0.5,"soc_min":0.05,"soc_max":0.95,
  "efficiency_pump":0.85,"efficiency_gen":0.90,
  "efficiency_pump_deep":0.72,"efficiency_gen_deep":0.78,
  "soc_deep_threshold":0.3,"vom":0.5
}

# BESS with cycle-depth cost
{
  "id":"bess_cycle","type":"bess","power_mw":100,"energy_mwh":400,
  "soc_init":0.5,"soc_min":0.05,"soc_max":0.95,
  "eta_charge":0.92,"eta_discharge":0.92,"vom_discharge":1.0,
  "cycle_cost_per_mwh":6.0,"depth_multiplier":1.8,"soc_deep_threshold":0.2
}

# IIS on infeasibility
"solver_settings": { "iis_on_infeasible": true, ... }
```

## Per-row output additions

Each `hourly_system` row gains two dictionaries:

```json
"dr":            {"dr_industry": 35.2},            // MW curtailed per DR asset
"pumped_hydro": {
  "ph_site_a": {
    "gen_mw":  110.0,
    "pump_mw":   0.0,
    "net_mw":  110.0,
    "soc_mwh": 420.5,
    "head_segment": "high",
    "mode":    "gen"
  }
}
```

`by_unit_summary` enumerates DR and pumped_hydro assets alongside the
other types, so Excel export / the HTML table pick them up automatically.

## Validation results (this environment, HiGHS backend)

| Run | Wallclock | Total cost | Avg λ | Unserved |
|-----|-----------|------------|-------|----------|
| smoke 168h (v1.2 input, v1.4 solver) | 8.7 s  | $17,666,394 (identical) | $2755/MWh | 90,532 MWh (by design) |
| GSE 168h baseline        | 32 s | $9,915,621 | $37.12 | 0 |
| GSE 168h + DR + PH + cycle BESS | 44 s | $9,896,891 | $37.03 | 0 |
| Infeasible fleet + IIS on | 1 s  | (nan)   | — | 52,308 MWh → IIS entry attached |

The DR + pumped-hydro + cycle-BESS run saved **$18,730 (1.2%) in 168 h**
on the demo fleet, with pumped-hydro contributing 337 MWh and DR / BESS
remaining off (out-of-the-money at $37/MWh clearing price).

## HTML UI additions

* Assets tab — type selector now includes **DR** and **Pumped Hydro**;
  defaults auto-fill `pmax_curtail / price_per_mwh / hours_per_year_max`
  for DR and `pmax / pump_mw / energy_mwh / efficiency_*` for PH.
* Horizon tab — new **IIS on infeasibility** checkbox propagates into
  `solver_settings.iis_on_infeasible` on Export.
* Results tab — new **⚠ Infeasibility Report (IIS)** red card appears
  automatically when `diagnostics.iis` is non-empty; lists each
  constraint / variable-bound that caused infeasibility.

## Known limitations

* Pumped-hydro head-efficiency is a 2-bin linearisation (high-head vs
  deep).  For finer nonlinearity, add more segments by extending
  `ph_zhi` into an SOS1 set — already straightforward.
* Elastic-heuristic IIS (HiGHS) is a diagnosis, not a mathematical IIS.
  When a Gurobi license is present, the `gurobi` backend path calls the
  native `computeIIS()` and returns the true irreducible core.
* BESS depth multiplier applies only to discharge.  For a full
  throughput-weighted degradation model, extend `_bess_pen` with a
  charge-throughput term (scaffolding already in place).
