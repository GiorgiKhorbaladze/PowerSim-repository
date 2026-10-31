# Georgia 2030 model reconstructed from PLEXOS

`samples/projects/georgia_2030_plexos.json` is a typed PowerSim project rebuilt from the user-approved PLEXOS 11 model **"BESS Research 800-1400"** (`plexos model.zip`, SHA-256 `0e55ed41…e53d`). `scripts/import_plexos_model.py` regenerates it, together with the benchmark reference `data/benchmarks/plexos_bess_research_800_1400.json`. Every asset and profile records the archive hash, the PLEXOS object and the source file.

The model is an aggregated zonal/regional study model, as in PLEXOS. It is not a unit-level model and it is not official GSE operational data.

## Mapping

| PLEXOS | PowerSim | Notes |
|---|---|---|
| 16 generators `01`…`16` with rating factor `RoR <zone> Medium/Dry/Flood` | `hydro_ror` `ror-<zone>`, profile = rating factor / 100 | Medium in the base case; Dry/Flood in scenarios |
| 5 wind generators, `Wind <region> Average/Calm/Storm` | `wind` `wind-<region>` | Average in base |
| 9 PV generators, `PV <region> Average/Low/High` | `solar` `pv-<region>` | Average in base |
| `Generation = Rated Capacity` constraints (renewables must-take, surplus dumped) | renewables curtailable at zero cost | see "Surplus energy" below |
| `Regulating HPP`: monthly Max Capacity, monthly Min Stable Level, Must-Run, monthly energy equality constraint (GWh) | `hydro_ror` `regulating-hpp`, flat availability, `monthly_pmax_mw`, `monthly_pmin_mw`, `monthly_energy_mwh` (×1000) with `monthly_energy_mode = exact` | energy-limited aggregate, no reservoir model |
| `TPP/Import`: Rating = `Tpp-Imp.csv`, equality constraint to the same file, VO&M 3 | `import` `tpp-import` with `pmin_profile = pmax_profile = tpp-import-mw`, `vom = 3` | fixed hourly output |
| Battery `Ksani`: 800 MW, 1,400 MWh, efficiency 100 %, initial SoC 50 %, VO&M Charge 1.5 | `bess` `bess-ksani`, `vom_discharge = 1.5` | equal energy for a lossless cyclic battery |
| Region `1_Georgia` load (`Load.csv`) + region `2_Export` load (`Export.csv`, max 800 MW = Geo-Exp line limit) | one `demand` profile = load + export | the line limit is never binding in the data |

The solve is a single full-year optimisation (`rolling_window_h = 8760`). A monthly energy budget can only be enforced exactly when the whole month is visible, so PowerSim refuses `monthly_energy_mwh` in a rolling solve rather than approximating it.

## New solver inputs used by this model

All are optional and default to the previous behaviour:

- `monthly_pmax_mw` (12 values): caps the asset's available capacity in each calendar month.
- `monthly_pmin_mw` (12 values) and `pmin_profile` (profile id, MW per period): forced minimum output, capped by available capacity.
- `monthly_energy_mwh` (12 values) with `monthly_energy_mode` `max` (default) or `exact`: total dispatched energy per calendar month.

They are verified independently after every solve by the QA check `operating_envelope`; a violation fails QA and blocks publication. Hand-checked regression cases are in `tests/test_operating_envelope.py`.

## Surplus energy

The fixed supply in the PLEXOS inputs exceeds demand: renewables, TPP and regulating hydro total 25,072 GWh against 22,554 GWh of load plus export. PLEXOS forces renewables to their rating and dumps the surplus (2,720 GWh derived from its hourly results). PowerSim keeps renewables curtailable, so the same surplus appears as wind/solar curtailment (reported) and run-of-river spill (available minus dispatched). The energy balance is the same; only the label differs.

## Benchmark against the PLEXOS solution

The PLEXOS archive contains the hourly results of the "BEST" model. Unserved and dump energy are not exported, so they are derived hour by hour from PLEXOS's own results: generation + battery discharge − battery charge, compared with the same demand.

| Quantity (2030, base case) | PLEXOS | PowerSim |
|---|---|---|
| Regulating HPP energy per month | 296 / 276 / 491 / 429 / 830 / 890 / 1,050 / 766 / 498 / 367 / 319 / 367 GWh | identical (exact budget, QA-verified) |
| Unserved energy | 202.2 GWh in 774 hours (every month) | 117.6 GWh in 199 hours (Jan, Feb, Sep–Dec) |
| Surplus (dump / curtailment + spill) | 2,720 GWh dump | wind/solar curtailment 1,712 GWh, plus run-of-river spill |
| Solve | PLEXOS ST schedule | one 8,760-hour MIP, about 60 s, optimal, QA pass |

Interpretation:
- Both models agree that, **with these inputs, the system is short of energy in winter**. In January, for example, demand not covered by renewables and the fixed TPP/import needs 341.5 GWh, but the regulating HPP's January energy is fixed at 296 GWh. A lossless battery can only move surplus hours into deficit hours.
- PowerSim's solution satisfies all of PLEXOS's constraints, and it optimises the battery and the regulating HPP over the whole year at once. Under the same constraints it can therefore only match or improve on PLEXOS's unserved energy. `tests/test_georgia_plexos_model.py` asserts exactly that, plus the monthly HPP energy and the energy balance, in the release gate.
- Hourly shapes of the regulating HPP and battery are not compared. With zero-cost hydro and renewables, many hourly schedules are equally optimal, so hour-by-hour differences say nothing about accuracy.

## Scenarios

| Scenario | Profiles |
|---|---|
| `hydro-dry`, `hydro-flood` | RoR Dry / Flood |
| `wind-calm`, `wind-storm` | Wind Calm / Storm |
| `solar-low`, `solar-high` | PV Low / High |
| `stress-dry-calm-low` | RoR Dry + Wind Calm + PV Low |

## Limitations

- The regulating HPP is an energy-limited aggregate. Reservoir levels, inflows and cascades are not modelled (same as the PLEXOS model).
- TPP/import is fixed to its hourly file, so it cannot respond to shortages. The unserved energy above therefore indicates how much additional firm supply or import the 2030 system would need under these inputs.
- Costs are only the PLEXOS VO&M values. With zero-cost hydro and renewables, the objective mainly reflects unserved energy at the 3,000 USD/MWh penalty.
- The data are PLEXOS study inputs approved by the user. They are not verified against measured operation; use the calibration workflow for that.
