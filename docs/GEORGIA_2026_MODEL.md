# Georgia 2026 and 2027 models (unit-level, 2024–2025 balance hydrology)

`samples/projects/georgia_2026_balance.json` is a single-node, 8,760-hour
dispatch model of the Georgian power system for 2026. It is built by
`scripts/build_georgia_2026_model.py` from committed inputs only, and
`tests/test_georgia_2026_model.py` checks that the committed file is exactly
what the builder produces.

It is a **planning study model**. It is not an official GSE forecast,
operational dispatch or policy-grade adequacy result. Thermal cost inputs are
illustrative placeholders that require review (see below).

## Inputs

| Input | File | Use |
|---|---|---|
| Hourly load 2026 | `data/gse_load_2026_2030.json` (GSE P50, 15,621 GWh, peak 2,645 MW) | demand |
| Installed capacity | `samples/demo_asset_registry_2026.json` | capacity of every asset |
| Monthly balance 2024 | `data/actuals/georgia_balance_2024.json` (from `2024_Cor_Geo.pdf`) | hydrology, wind |
| Monthly balance 2025 | `data/actuals/georgia_balance_2025.json` (from `2025_GEO.pdf`) | hydrology, wind, net flow |
| Generic thermal data | `data/cost_database_pypsa_eur.json` (PyPSA technology-data, 2025) | efficiency, VOM |
| Wind / solar shape | `data/renewables/Wind_Gori_2026_A_mean.csv`, `Solar_Gori_2026_A_mean.csv` | hourly shape |

The balance PDFs were supplied by the user. `scripts/parse_georgia_balance.py`
turns them into JSON, records the PDF SHA-256 and checks every row against its
own annual total. The tests also check, for both years, that:

- generation plus net flow equals consumption including losses;
- regulating plus seasonal HPPs equals total HPP output;
- the seasonal-HPP row equals its plant rows plus the small-HPP row.

The seasonal row **includes** the small HPPs, so the two rows must not be added
together.

## Mapping

| Balance item | PowerSim | Rule |
|---|---|---|
| 7 regulating HPPs (Enguri, Vardnili, Khrami 1/2, Shaori, Dzevrula, Zhinvali) | 7 `hydro_ror` assets, flat availability | **exact monthly energy** = mean of 2024 and 2025; dispatchable within the month up to installed capacity; no reservoir model |
| 20 seasonal-HPP rows (Vartsikhe 1–4 and Gumati 1–2 are one row each) | 20 `hydro_ror` assets | monthly mean energy, **flat within the month**, spill allowed |
| Small HPPs (+ Chiora HPP, which is missing from the registry) | one `hydro_ror` aggregate, 331.26 MW | as above |
| Kartli wind | `wind`, 20.7 MW | Gori 2026 hourly shape, scaled per month to mean Kartli energy 2024–2025 |
| Zemo Wind (new in 2026) | `wind`, 18.75 MW | **assumption:** Kartli capacity factor (no data) |
| 6 solar plants | `solar` aggregate, 10.75 MW | Gori 2026 profile (2025 solar output covers only part of the year) |
| Net flow ("გადადინება სალდო") | `import` fixed to its profile; export added to load | **2025 monthly net balance**, flat within the month |
| 6 thermal plants (7 registry units) | `thermal`, dispatched by the optimiser | capacity from registry; costs below |

Plants that produced nothing in 2024 (Mtkvari HPP) use 2025 only. Khobi-2 HPP
started part-way through 2024, so its mean underestimates a full year (about
-20 % against 2025).

### Thermal costs: review required

There is no approved unit-level thermal data in the repository. Each thermal
asset carries `review_required: true`.

- **Efficiency and VOM:** PyPSA generic values. CCGT 0.57 is used for
  Gardabani 1/2. OCGT 0.405 is used for G-Power. The coal steam value 0.356 is
  a proxy for the Mtkvari IX and Tbilsresi steam units and for Tkibuli. EUR
  values are used as USD without conversion.
- **Fuel prices:** gas 5.5 USD/MMBtu and coal 4.0 USD/MMBtu. These are the
  illustrative values already used by `samples/sample_input_gse_2026_168h.json`.
- **Availability:** every unit is always available. Outages and maintenance
  are not modelled.

The **total** thermal energy is set by the balance (load minus hydro, wind,
solar and the fixed net import), so it does not depend on these costs while no
load is unserved. **How that energy splits between thermal plants follows the
placeholder merit order and is not a result.**

## Result (base case, HiGHS, full-year solve)

Run: QA `pass`, publishable, termination optimal; about 45 s.

| GWh | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec | Year |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Load + net export | 1,326 | 1,315 | 1,340 | 1,227 | 1,365 | 1,393 | 1,443 | 1,427 | 1,174 | 1,225 | 1,300 | 1,544 | 16,081 |
| Regulating HPPs | 259 | 239 | 268 | 325 | 549 | 781 | 877 | 636 | 425 | 381 | 346 | 260 | 5,346 |
| Seasonal + small HPPs | 388 | 323 | 524 | 775 | 800 | 597 | 549 | 416 | 367 | 424 | 361 | 329 | 5,855 |
| Wind + solar | 12 | 12 | 16 | 16 | 16 | 16 | 13 | 16 | 17 | 14 | 9 | 12 | 168 |
| Net import (fixed) | 334 | 388 | 245 | 0 | 0 | 0 | 0 | 98 | 17 | 16 | 111 | 297 | 1,506 |
| **Thermal (model)** | 332 | 354 | 289 | 110 | 0 | 0 | 3 | 261 | 348 | 389 | 472 | 647 | **3,205** |
| Thermal, actual 2025 | 365 | 412 | 189 | 0 | 0 | 6 | 56 | 317 | 294 | 232 | 335 | 562 | 2,767 |

Main points:

- **Load is served in every hour; unserved energy is 0.** Total load including
  losses is 15,621 GWh. The 1,506 GWh of import includes the months with net
  export, which are added to load.
- **2026 thermal output is about 3,205 GWh**, which is 438 GWh (+16 %) more
  than in 2025. Load grows by 737 GWh (+5 %, GSE P50). Mean hydrology is
  214 GWh higher than 2025, and the fixed net flow stays at the 2025 level, so
  thermal plants cover the rest.
- **December is the tightest month.** Thermal output is 647 GWh, and all
  thermal units run at full output in 286 hours of the year. The model has no
  outages, so a forced outage in those hours would need extra import or cause
  a shortage. Studying that risk needs outage data, which the repository does
  not have.

## 2027 model

`samples/projects/georgia_2027_balance.json` is built with
`python scripts/build_georgia_2026_model.py --year 2027`. It is the 2026 model
with two differences:

- **Load:** the GSE P50 2027 hourly load (16,128 GWh, +507 GWh).
- **Ruisi wind farm, 206 MW** (`wind-ruisi`; Ruisi is in Kareli municipality,
  Shida Kartli). The user named this plant as a 2027 addition. Two
  **assumptions** apply until project data is available:
  - it operates all year;
  - it uses the Kartli wind capacity factor (43 %, measured 2024–2025 at the
    nearest operating wind farm near Gori), giving about 780 GWh/year.

Every other input (hydrology, 2025 net flow, thermal placeholders) is the same
as in 2026.

| GWh | 2026 | 2027 |
|---|---|---|
| Load + net export | 16,081 | 16,588 |
| Wind (2027: Ruisi 780) | 149 | 929 |
| HPPs (2027: 74 GWh spilled in May–July) | 11,201 | 11,127 |
| **Thermal** | **3,205** | **3,007** |
| Hours with all thermal units at full output | 286 | 274 |

Both runs pass QA, are publishable and have no unserved energy. Ruisi lowers
thermal output by about 200 GWh while load grows by 507 GWh. Winter is still
the bottleneck: December thermal output is 647 GWh in 2026 and 642 GWh in
2027, because wind in December is low.

## Comparing with a 2026 balance (actual or plan)

1. Parse the balance PDF:

   ```
   python scripts/parse_georgia_balance.py 2026.pdf --year 2026 --out data/actuals/georgia_balance_2026.json
   ```

2. Compare it with a completed run:

   ```
   python scripts/compare_georgia_balance.py --workspace ./powersim_workspace --run <run-id> \
       --balance data/actuals/georgia_balance_2026.json --out comparison.csv
   ```

The CSV lists model and balance GWh, the difference and the difference in %,
per month and for the year. Rows are given for each plant, each plant group,
net flow and consumption. Only publishable runs that pass QA are compared, and
the comparison does not change the model.

The load and net-flow rows show differences in the **input** assumptions, not
model error. A difference in thermal energy is mostly explained by those
inputs plus hydrology. Read the input rows first.

## Limitations

- Single node (copper plate): no internal transmission or interconnector
  limits, and no N-1.
- Regulating HPPs have exact monthly energy but no reservoir levels, inflows or
  cascade coupling.
- Within each month, seasonal and small HPPs are flat. Daily peaking is not
  represented.
- Net interconnector flow is fixed to 2025. The optimiser cannot import more
  in tight hours.
- Thermal costs and availability are placeholders (see above).
