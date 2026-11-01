# Georgia outlook 2027–2032: dispatch, reserve, adequacy, RICE and CCGT-3

`scripts/georgia_outlook.py` builds and solves the Georgia power system for
2027–2032 in two scenarios. It answers three questions:

1. **New capacity:** if Tbilsresi III and IV retire, when and how much new
   firm capacity (RICE gas engines and/or a 250 MW CCGT-3) is needed?
2. **Reserve:** how much reserve does the system need, and does it have it?
3. **Adequacy:** how reliable is the system (LOLE, EENS) with and without that
   capacity?

`scripts/georgia_outlook_report.py` turns the results JSON
(`data/outlook/georgia_outlook_2027_2032.json`) into a Georgian HTML report and
an Excel workbook.

This is a **planning study**. It is not an official GSE forecast. The cost,
fuel-price and forced-outage inputs are generic assumptions and must be
replaced with approved data before the results are used for decisions.

## Cases

Every case starts from the 2026 unit-level model (`docs/GEORGIA_2026_MODEL.md`).

| | Scenario A: known projects only | Scenario B: PLEXOS build-out |
|---|---|---|
| Load | GSE P50 2027–2030; 2031–2032 = 2030 × 3.69 %/yr | same |
| Ruisi wind 206 MW | from 2027 | from 2027 |
| Battery 200 MW / 200 MWh | from 2029 (enters end of 2028) | same |
| Other wind, solar, new run-of-river | none | linear build-out to the PLEXOS 2030 fleet by 2030 (wind 1,909 MW, solar 1,496 MW, about +2,013 GWh run-of-river), constant after |
| Tbilsresi III + IV (272 MW) | retired from 2027 | retired from 2027 |
| Tkibuli (13 MW) | excluded (no output in 2024–2025) | excluded |
| Net interconnector flow | 2025 monthly balance, fixed | same |

Sensitivities for each year and scenario, run with that year's capacity plan:

- 2025 hydrology alone (less hydro energy over the year);
- 2024 hydrology alone (less regulating-hydro energy in winter: 995 against
  1,213 GWh in January, February, November and December);
- load +5 %;
- load −5 %.

Leap years (2028, 2032) have 8,760 hours with 29 February omitted, as in the
GSE projections.

## Dispatch

Each case is an 8,760-hour PowerSim project solved by the application executor:

- HiGHS LP, full-year horizon;
- regulating HPPs with exact monthly energy;
- battery state of charge tracked hour by hour;
- canonical QA and publication gate.

Only completed, QA-passing, publishable runs are used.

## Adequacy (LOLE / EENS)

PowerSim's generic chronological-adequacy workflow treats hydro as available at
full capacity in every hour. In Georgia the binding winter constraint is
reservoir **energy**, so that workflow would understate the risk. The outlook
therefore uses its own sequential Monte Carlo (`adequacy`), with 200 samples
per case. In each hour of each sample:

1. **Thermal units** are available or on forced outage. The state follows a
   two-state Markov chain whose steady state equals the forced-outage rate:
   failure probability λ = FOR·μ/(1−FOR) and repair probability μ = 1/MTTR,
   both per hour.
2. **Fixed supply:**
   - run-of-river hydro (monthly mean energy, flat within the month), wind and
     solar;
   - the fixed 2025 net import.

   Exports are interruptible and are excluded from load.
3. **Regulating hydro** (aggregate of the seven plants) is limited to its
   installed capacity in each hour and to its monthly energy over the month.
   For residual load *r* (load minus all other available supply):
   - the minimum unserved energy is Σr⁺ − min(E, Σmin(P, r⁺));
   - LOLE is minimised by fully covering the smallest positive residuals first
     (`hydro_dispatch_shortfall`).
4. **Battery:** cycles at most once a day. Its usable energy (90 % depth,
   85 % round trip) comes from that day's surplus and is discharged against
   that day's shortfalls, smallest first.

Hydrology is sampled 50/50 from the 2024 and 2025 balances. The target is
**LOLE ≤ 3 h/year**.

Not modelled:
- planned maintenance;
- hydro unit outages;
- correlated outages;
- weather years other than the single wind/solar profile year;
- extra emergency imports.

## Reserve

**Requirement** = largest single unit + the P99 of the unpredictable hourly
change in wind and solar output:

- **Largest unit:** Mtkvari IX, 300 MW. This is larger than an Enguri unit
  (260 MW) and the CCGT-3 block (250 MW).
- **Wind:** the full hourly change (persistence forecast error).
- **Solar:** the hourly change minus the mean change for the same hour of day
  in the same month. The sunrise and sunset ramp is predictable and is
  scheduled, not balanced.

The error series per MW come from the repository's Gori 2026 wind and solar
profiles. These are single-site profiles, so the estimate is conservative.

The PLEXOS 2030 profiles are not used for reserve. Their hourly changes reach
68–90 % of a region's capacity at the 99th percentile, against 23 % for the Gori
wind profile. That is plausible for energy totals but not for ramp
statistics. **This is a finding about the PLEXOS input data.**

**Available headroom** in each hour = unused capacity of thermal units and
regulating HPPs in the dispatch. The battery is not counted. A case meets the
reserve criterion when headroom ≥ requirement in every hour.

## Expansion logic

The expansion is evaluated year by year, and builds are cumulative:

1. RICE can be built from 2028 and CCGT-3 from 2029 (lead times). Nothing can
   be built for 2027.
2. For each feasible option, the model finds the smallest RICE capacity (in
   20 MW blocks) that brings LOLE to ≤ 3 h. The options are:
   - keep the current builds;
   - add CCGT-3 when allowed.
3. If the dispatch then lacks reserve headroom in any hour, the model adds RICE
   blocks to cover the shortfall.
4. Annual cost = annuity of the new capacity (8 %, 25 years) + fixed O&M +
   dispatch cost (fuel, VOM, unserved energy at 3,000 USD/MWh). The cheaper
   option is kept.

| Candidate | Size | Efficiency | Capital | Fixed O&M | VOM | FOR / MTTR | Source |
|---|---|---|---|---|---|---|---|
| CCGT-3 | 250 MW | 58 % | 1,109 USD/kW | 3.35 %/yr | 5.6 USD/MWh | 4 % / 40 h | PyPSA technology-data 2030 (EUR used as USD) |
| RICE | 20 MW blocks | 46 % | 850 USD/kW | 15 USD/kW·yr | 7.0 USD/MWh | 2 % / 24 h | **assumption, review required** |

Forced-outage rates of existing units (**generic assumptions, review
required**):

| Units | FOR | MTTR |
|---|---|---|
| Gardabani 1 and 2 | 4 % | 40 h |
| Mtkvari IX | 10 % | 72 h |
| G-Power | 5 % | 30 h |

Gas is priced at 5.5 USD/MMBtu in all years (illustrative).

## Tests

`tests/test_georgia_outlook.py`:

- checks the hydro-shortfall function against hand-calculated cases (a
  capacity-limited month and two energy-limited months);
- checks that the Markov availability matches the forced-outage rate;
- checks that the case builder follows the assumptions (battery from 2029,
  retirements, scenario B reaching the PLEXOS 2030 fleet, 2031 load growth);
- checks the reserve requirement and the annuity formula.

## Results (run of 1 November 2026, 200 Monte Carlo samples)

Every dispatch run passed QA and is publishable.

| | 2027 | 2028 | 2029 | 2030 | 2031 | 2032 |
|---|---|---|---|---|---|---|
| A: LOLE, Tbilsresi III–IV kept, no new capacity (h) | 0.2 | 2.5 | 17.8 | 58.8 | 121.6 | 215.0 |
| A: LOLE, III–IV retired, no new capacity (h) | 44.2 | 99.7 | 204.5 | 323.1 | 466.9 | 646.4 |
| A: RICE in the plan (MW, cumulative) | not buildable | 240 | 340 | 440 | 520 | 640 |
| A: LOLE with the plan (h) | 44.2 | 1.9 | 2.0 | 1.8 | 3.0 | 1.9 |
| A: thermal generation (GWh) | 3,007 | 3,480 | 4,138 | 4,791 | 5,458 | 6,150 |
| B: LOLE, III–IV retired, no new capacity (h) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| B: curtailed surplus (GWh) | 172 | 689 | 1,368 | 2,794 | 2,014 | 2,044 |

**Scenario A**

- RICE is needed from 2028. It grows to 640 MW by 2032.
- Year by year, CCGT-3 is never chosen.
- Whole-pathway NPV 2027–2032 (8 %):
  - RICE only: 1,022 M USD;
  - CCGT-3 from 2031: 1,034 M USD;
  - CCGT-3 from 2029: 1,040 M USD.
- In 2032 the CCGT-3 pathway is already cheaper per year (305.9 against
  309.0 M USD). CCGT-3 therefore becomes the better choice after the study
  horizon, so starting it around 2030–2031 is a reasonable alternative.

**2027**

- Retiring Tbilsresi III–IV raises LOLE from 0.2 h to 44 h.
- No new capacity can be built in time.
- 2027 therefore depends on extra imports or on postponing the retirement.

**Scenario B**

- The PLEXOS 2030 fleet over-supplies GSE demand (2.8 TWh curtailed in 2030).
- It needs no new firm capacity, even with 2024 winter hydrology and PLEXOS
  calm-wind / low-solar weather.
- The surplus is potential export. It needs export and grid capacity that
  this single-node model does not represent.

**Stress test (scenario A, 2024 winter hydrology)**

- RICE needed: 240 MW (2028) rising to 660 MW (2032).
- This is close to the main plan.

**Reserve**

- The requirement is 357 MW in A and up to 742 MW in B (2030). Minimum
  headroom (about 900–1,100 MW) covers it in every hour.
- The capacity need is driven by winter energy and LOLE, not by reserve.


The results, the HTML report and the Excel workbook are produced by:

```
python scripts/georgia_outlook.py --out data/outlook/georgia_outlook_2027_2032.json
python scripts/georgia_outlook_report.py data/outlook/georgia_outlook_2027_2032.json \
    --html georgia_outlook.html --xlsx georgia_outlook.xlsx
```

The run takes about 1–1.5 hours: about 100 full-year LP solves plus the Monte
Carlo cases.
