# What is PowerSim?

PowerSim is a decision-support simulation platform for the Georgian power
system. It links a browser-based planning UI with a Python optimization runner
through explicit JSON handoff files, so planners can prepare scenarios, solve
hourly dispatch and commitment cases, and review results without running the
solver inside the browser.

PowerSim v1.0 Release Candidate readiness is focused on a reliable demo and
repeatable study workflow, not on being a full commercial market suite.

## In one sentence

PowerSim turns Georgian-system input assumptions into hourly unit-commitment,
economic-dispatch, gas-use, hydro, renewable, import, BESS, and scenario KPI
outputs that can be loaded back into the HTML UI for review and comparison.

## What PowerSim does

* Builds schema-validated `powersim_input.json` files for a selected study year,
  horizon, scenario, fleet, demand profile, hydro inflow profile, renewable
  profile, gas constraints, and solver settings.
* Runs a Pyomo + HiGHS unit-commitment / economic-dispatch model through the
  Python solver.
* Supports short smoke runs, rolling-horizon month-length runs, annual 8760-hour
  runs, and a Monte-Carlo-style sweep across the configured inflow scenarios.
* Writes `powersim_results.json` and `powersim_results.xlsx` artifacts for each
  scenario run.
* Imports result JSON files into the HTML UI to display system KPIs, dispatch,
  lambda, monthly summaries, diagnostics, per-asset summaries, and scenario
  comparisons.

## What PowerSim is not

PowerSim is **not** a full PLEXOS clone. It is intentionally smaller and more
transparent for release-candidate demonstration and Georgian-system analysis.
The current model emphasizes a reproducible JSON contract, a practical UC/ED
workflow, and demo-ready scenario comparison.

PowerSim does not currently claim:

* full nodal or zonal transmission-network optimization;
* all market-clearing, ancillary-service, and settlement rules;
* complete commercial-production validation against private GSE source data;
* exhaustive hydro physics or water-management policy modeling;
* full replacement of licensed production planning tools.

## Primary personas

| Persona | Uses PowerSim to |
|---------|------------------|
| Planner / analyst | Build and compare scenarios, inspect dispatch and cost impacts, and export reports. |
| Model maintainer | Keep schema, Python runner, HTML importer, and sample outputs compatible. |
| Demo presenter | Show the full browser → JSON → solver → JSON → browser loop from a clean clone. |
| Data owner | Place private GSE files in the local `project_data/` folder without committing them. |

## Core workflow

1. Prepare `project_data/` using either the synthetic generator or private GSE
   source files kept outside git.
2. Build or export a PowerSim input JSON.
3. Run the Python horizon or MC sweep script.
4. Import the result JSON into the HTML UI.
5. Compare scenarios and export an Excel report.

## Release-candidate success criteria

A v1.0 RC demo is successful when Giorgi can:

* start from a clean clone;
* install dependencies;
* build synthetic `project_data/` without private GSE data;
* run a 168-hour smoke case;
* run at least one 720-hour GSE-baseline-style case from synthetic inputs;
* optionally run an 8760-hour annual case and MC sweep if time permits;
* open the HTML UI;
* import result JSON;
* compare at least two scenario result JSON files;
* export an Excel report;
* explain limitations and remaining validation blockers clearly.

## Data safety boundary

Private GSE source files belong only in local working folders such as
`project_data/` or another untracked private data path. They must not be
committed. Release-candidate documentation should describe expected filenames
and folder structure, but should never include proprietary values or private
source data extracts.

## Solver hardening v1.6.0 notes

- BESS can provide reserve up/down subject to discharge/charge headroom and SOC or empty-SOC over `reserve_duration_h` (default 1 hour).
- Eligible reserve assets with unsupported provider types are surfaced in diagnostics (`reserve_eligible_filtered`) rather than silently ignored.
- Rolling-horizon first-period ramp constraints use previous-window dispatch carryover where available.
- `system_summary.total_cost_usd` remains production/gross cost for compatibility; `system_summary.total_objective_cost_usd` and `diagnostics.objective_breakdown` report the full objective and closure gap.
- Stochastic summaries use full scenario result stores/objective costs and warn with `stochastic_profiles_not_switched` when no scenario-specific profiles or overrides are applied.
- Limitations remain: not a full PLEXOS clone, simplified DC-OPF, no AC voltage/reactive power, and no reserve market settlement.

## Adequacy & Expansion Stage 1

PowerSim includes a Stage 1 adequacy screening layer for PASA-style planning questions. It computes LOLE, LOLP, EENS, peak shortfall, reserve margin, firm capacity, and an `adequacy_pass` flag from the same JSON inputs used by the UC/ED workflow. The module supports fast `deterministic_derated` screening and a seeded `monte_carlo` forced-outage mode.

This is a transparent screening approximation, not a full PLEXOS PASA or long-term co-optimization. Thermal, hydro, and imports are derated by forced outage rate by default; wind and solar use availability profiles when supplied or capacity-credit assumptions otherwise; BESS firm capacity is limited to `min(power_mw, energy_mwh / required_storage_duration_h)`, defaulting to a 4-hour requirement. Recommended expansion builds should always be handed back to the UC/ED solver for chronological dispatch testing, and private GSE validation remains required before operational or investment decisions.
