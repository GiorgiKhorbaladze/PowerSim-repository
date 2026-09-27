# PowerSim 1.0 migration progress

## Current state

| Item | Status |
| --- | --- |
| Completed stage | Stage 3C closure and corrective hardening - merged in PR #77. |
| Merged migration PRs | #69 Stage 3A, #71 Thermal UC, #72 thermal economics, #73 BESS core, #74 pumped-hydro core, #75 demand-response core, #76 DR QA/publication corrective, #77 Stage 3C closure hardening. |
| Shared components | Wind, solar, simplified run-of-river, imports, thermal UC/economics, BESS with independently auditable canonical boundary state, pumped hydro with auditable segment flows and VOM, and demand response with structured validation and publication gating. |
| Components still legacy-owned | Thermal objective ownership; BESS depth-cost/end-target extensions; reservoir cascade coupling, gas coupling, reserves, network, stochastic, adequacy and expansion. |
| Release readiness | Not release-ready. Stage 3D reservoir-core migration is in review; cascade and gas remain next. |

## Stage 3B acceptance intent

The opt-in shared engine must reproduce legacy thermal dispatch and objective
within documented tolerances, while producing canonical thermal result records
and independent result-based QA for bounds, UC transitions and time-scaled
ramps. The legacy objective remains authoritative during the migration so that
cost terms cannot be counted twice. Canonical thermal results now reconstruct
the exact per-period variable, startup, no-load and CO2 terms used by that
objective, including its existing piecewise heat-rate expression. Objective
ownership remains a Stage 4C integration item; this is a deliberate,
documented limitation rather than a claim of completed migration.

## Stage 3C semantics and ownership

Demand response is a positive injection to the electricity balance: it is load
reduction supplied to the balance. `curtailed_mw` and `injection_mw` therefore
describe the same physical action from demand-side and balance-side viewpoints.
DR is permanently excluded from VRE resource-accounting QA.

The current solver intentionally retains two existing DR budget behaviours:

1. A single-window solve pro-rates `hours_per_year_max` by the study fraction
   of 8760 hours.
2. A rolling solve starts with the full configured annual call-out budget and
   decrements it only from committed slices.

PR #77 makes independent QA reproduce those existing semantics;
it does not alter the annual-budget equation. Whether the two policies should
be unified is a future modelling decision, not a refactoring change.

BESS shared core owns SOC, self-discharge, auxiliary draw, inverter power,
C-rate and ramp constraints. The legacy assembler still owns optional
`depth_multiplier` / `soc_deep_threshold`, `dis_deep`, `soc_end_target` and
`soc_end_penalty_usd_mwh` equations. The end target remains last-window-only.

Pumped hydro shared core owns structured validation, two-segment physical
flows, SOC and VOM extraction. Canonical records expose high/deep generation
and pumping, plus preceding SOC, so QA can reconstruct chronological SOC and
rolling continuity independently.

## Known modelling decision - one-sided depth selector

The legacy BESS shallow selector and pumped-hydro high-head selector impose
only `z=1 -> SOC >= threshold`; the reverse implication is not encoded.
Consequently the comments claiming an "iff" are mathematically false. A
feasible counterexample is SOC above threshold with `z=0`, which can expose
the deep BESS cost or deep pumped-hydro efficiency. The objective may often
prefer the intuitive state, but it does not prove it. This PR characterizes
and reports the issue without changing the equation, because adding the
reverse implication would deliberately change optimization behaviour and
requires explicit modelling approval before a PowerSim 1.0 correction.

## Next work

1. Stage 3D: migrate the existing cascade transport coupler with explicit
   delayed water ports and independent conservation QA.
2. Stage 3D: gas constraints/coupling, with independent QA and parity.

## Stage 3D reservoir core - pending merge

The shared `hydro_reg` component now owns validated non-cascade reservoir
physics: storage, own inflow, turbine release conversion, spill, storage
bounds, rolling storage carry-over, final storage floor, monthly storage
targets, minimum release and window-level head-efficiency interpolation.
Its canonical records explicitly publish Mm3 state, prior state, Mm3/h own
inflow/release/spill, cascade contribution, efficiency and spill cost.
Independent QA reconstructs the water balance from resolved input and those
records. It does not inspect the live Pyomo balance.

Cascaded reservoirs deliberately remain legacy-owned until the cascade
coupler is migrated. This avoids silently treating upstream water as zero.

### Confirmed legacy soft end-level penalty defect

The legacy `end_short` variable/constraint is declared after the Pyomo
objective is constructed. Therefore the advertised `end_level_penalty` is
not charged by the live objective, despite appearing in the output-side
reconstruction. Activating that term would alter dispatch and economics, so
the Stage 3D shared core excludes assets using this optional feature. They are
classified as legacy compatibility only, not validated PowerSim 1.0 core.
Correcting it requires a separate explicit modelling decision.

## Blockers

No routine engineering blocker. The one-sided deep-selector formulation is a
documented modelling decision requiring approval before its mathematics can be
changed.
