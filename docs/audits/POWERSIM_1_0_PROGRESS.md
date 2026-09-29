# PowerSim 1.0 migration progress

## Current state

| Item | Status |
| --- | --- |
| Completed stage | Stage 3D reservoir-hydro, cascades and gas coupling - merged in PR #81. Stage 4A reserve engine - merged in PR #82. Stage 4B DC network hardening - merged in PR #83. PR #84 made objective QA mandatory. Stage 4C shared deterministic integration - merged in PR #85. |
| Merged migration PRs | #69 Stage 3A, #71 Thermal UC, #72 thermal economics, #73 BESS core, #74 pumped-hydro core, #75 demand-response core, #76 DR QA/publication corrective, #77 Stage 3C closure hardening, #79 reservoir-hydro core, #80 cascade migration, #81 Stage 3D closure, #82 reserve engine, #83 DC network hardening, #84 deterministic objective QA gate, #85 deterministic shared integration. |
| Shared validated components | Wind, solar, simplified run-of-river, imports, thermal UC/economics, non-committable reservoir hydro/cascades with independent water-balance QA, BESS with independently auditable canonical boundary state, pumped hydro with auditable segment flows and VOM, demand response with structured validation and publication gating, canonical gas accounting, data-driven reserves, and DC network flow/balance QA. |
| Components still legacy-owned | Committable reservoir-hydro UC/pmin/startup behavior; thermal objective construction; optional BESS depth-cost/end-target extensions; reservoir `end_level_penalty`; stochastic, adequacy and expansion. Pumped hydro and DR remain unsupported reserve providers. |
| Release readiness | Not release-ready. Stage 5A shared stochastic UC implementation is awaiting PR/CI acceptance; Stage 5B follows only after that gate. |

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

1. Stage 5A: stochastic UC parity using the shared deterministic component path.

## Stage 3D reservoir core and cascades - merged in PRs #79 and #80

The shared `hydro_reg` component now owns validated non-cascade reservoir
physics: storage, own inflow, turbine release conversion, spill, storage
bounds, rolling storage carry-over, final storage floor, monthly storage
targets, minimum release and window-level head-efficiency interpolation.
Its canonical records explicitly publish Mm3 state, prior state, Mm3/h own
inflow/release/spill, cascade contribution, efficiency and spill cost.
Independent QA reconstructs the water balance from resolved input and those
records. It does not inspect the live Pyomo balance.

The shared cascade extension preserves existing `cascade_upstream`, delay,
gain, and `turbined_only` / `release_plus_spill` semantics. Its canonical
records include cascade inflow and QA reconstructs it from the resolved
topology and upstream canonical release/spill records.

### Corrective rolling-cascade boundary work - merged in PR #81

PR #80's shared local-window equation cannot independently prove delayed
upstream water across a rolling boundary. PowerSim v1.0 therefore excludes
shared rolling cascades with `cascade_travel_delay_h > 0` and rejects them
with structured validation. Non-rolling delayed cascades and rolling
zero-delay cascades remain validated. Delayed rolling transport remains
legacy compatibility only pending an explicit boundary-history migration.

## Stage 3D gas coupling - merged in PR #81

Canonical thermal records carry per-period physical gas consumption in Mm3,
reconstructed as dispatch MW × duration hours × heat-rate-derived Mm3/MWh.
Independent QA validates this conversion and reconstructed annual/monthly caps
for resolved eligible units. Rolling 60-minute and 15-minute tests prove that
only committed-slice gas consumption decrements the carried annual budget.
Corrupt canonical gas volumes fail QA and publication.

## Stage 4A reserve engine - merged in PR #82

Reserve products are data-driven by id and direction (`up`, `down`, or
`symmetric`). Fixed and profile-backed requirements are fail-closed when a
declared profile is absent or incomplete. Canonical reserve records expose
raw and derated provision, product requirement/shortfall, provider capability
and shortfall penalty. Independent QA reconstructs product coverage,
eligibility, provider capability, cross-product stacking and penalty from
resolved input plus extracted records; QA failure blocks publication.

The existing thermal, reservoir-hydro, run-of-river, import and BESS provider
scope remains preserved. Pumped hydro and demand response are explicitly
filtered rather than represented as validated reserve providers.

Reserve provider response-time/ramp deliverability is **not** validated. The
`response_time_label` field is descriptive text only and PowerSim does not
infer a numerical response time from it. Validated reserve scope is therefore
requirements, direction/symmetry, eligibility, derating, headroom/footroom,
cross-product stacking, BESS power/SOC energy sufficiency and shortfall cost.

## Stage 4B DC network hardening - merged in PR #83

Validated network mode is explicit and fail-closed: it requires buses,
branches, one reference bus, a valid bus for every asset and an explicit load
mapping. Branch flow uses only `susceptance_mw_per_rad`; legacy `x_pu` is
accepted only together with `base_mva` and is converted deterministically.
Canonical results publish signed flow, bus angle and nodal injection. QA
reconstructs flow, limits, nodal balance and reference angle without reading
live Pyomo constraints. AC voltage, reactive-power and losses are not claimed.

The current formulation has only a system-level unserved-energy slack. It is
not a locational shortage variable, so a DC-network run with nonzero unserved
energy fails QA and publication. Validated DC scope excludes locational
scarcity/unserved-energy studies until bus-level unserved variables exist.

## Stage 4C deterministic shared integration - merged in PR #85

The production publication path now executes an explicit objective QA check
against full-precision canonical cost streams and the solver incumbent
objective. Presentation summaries may remain rounded, but validated QA no
longer uses them or the former 0.5% compatibility tolerance. It fails closed
when a required cost stream is absent, non-numeric or does not close to the
canonical reconstruction.

The shared acceptance fleet covers thermal, non-committable reservoir hydro,
run-of-river, wind, solar, BESS, pumped hydro, demand response, imports, gas,
reserves and DC network at 60-minute, 15-minute and supported rolling
resolution. Independent component/network/reserve/objective QA must pass for
publication. Canonical corruption of objective, network, storage/water or
reserve data is covered by publication-gate regression tests.

### Ownership audit

| Area | v1.0 ownership/status |
| --- | --- |
| Thermal bounds, UC and ramps | Shared authoritative for migrated thermal assets |
| VRE availability and RoR bounds | Shared authoritative |
| Reservoir water balance and non-committable pmax | Shared authoritative; committable hydro remains legacy compatibility only |
| BESS and pumped-hydro SOC | Shared authoritative for validated core; documented selector extensions remain compatibility only |
| DR and gas budget | Shared authoritative |
| Reserve allocation/headroom/stacking | Shared authoritative within the documented provider and no-response-time scope |
| DC nodal balance/flows | Shared authoritative for zero-unserved DC studies |
| Objective assembly | Legacy Pyomo objective remains authoritative; full-precision canonical reconstruction is independently publication-gated |

## Stage 5A stochastic UC - implementation complete, awaiting PR/CI

Validated stochastic UC has one entrypoint and one joint Pyomo extensive-form
solve. Each scenario block is built by the deterministic shared assembler;
there is no independent scenario loop, consensus heuristic or second solve.
Thermal commitment, startup, shutdown and hot-start selectors are constrained
non-anticipatively in that single model, while dispatch and other supported
physical recourse remain scenario-specific. Stochastic rolling horizon is
explicitly unsupported and fail-closed.

Deterministic post-solve extraction is now the reusable
`extract_window_solution()` path used by both deterministic windows and the
already-solved EF blocks. Each scenario therefore exposes normal canonical
physical output and runs the same independent deterministic QA: component,
gas, reserve, DC network and full-precision objective checks. Per-scenario
diagnostics are explicitly marked `solve_scope=extensive_form`; only the
aggregate owns solver runtime, bound and actual gap.

The strict contract requires unique scenario ids and explicit, finite,
strictly-positive probabilities summing to one without normalization. Profile
overrides are validated before assembly for known references, numerical
finiteness and solved-horizon coverage. Aggregate QA independently checks the
probability contract, extracted non-anticipativity, probability-weighted
full-precision expected objective, scenario physical validity and complete
canonical extraction. Any failure blocks publication. The legacy independent
scenario loop and consensus heuristic remain legacy compatibility only.

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
