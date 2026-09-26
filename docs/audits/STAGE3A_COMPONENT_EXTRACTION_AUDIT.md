# Stage 3A component extraction audit

## Scope and outcome

Shared protocol, context, typed ports, deterministic registry, canonical extraction, objective
terms, and independent result QA were added. Wind, solar, simplified run-of-river, and imports own
their availability constraint in the opt-in hybrid deterministic assembler. Thermal, reservoir
hydro, BESS, pumped hydro, demand response, gas, reserves, and network remain legacy.

No equation was intentionally changed. The legacy implementation remains for rollback and
characterization. The shared engine is opt-in through
`solver_settings.component_engine="shared"`; the default is `legacy`. Both paths retain Stage-2
backend diagnostics and publication flow.

## Canonical extraction and QA

Shared solves extract full-precision `ComponentResult` records after a usable incumbent is loaded.
The records travel explicitly on `SolvedRows`; no function-global state is used. A non-rolling run
carries the full component result set. Rolling runs retain only committed-period component records
and discard overlapping look-ahead observations. Published compatibility JSON exposes the records
under `component_results`.

Component checks use the existing `QACheckResult` / `QAReport` contracts rather than a second
boolean QA system. They reconstruct finite/non-negative domains, availability/import bounds,
renewable resource accounting, and curtailment MW-to-MWh conversion from extracted results only.
They do not inspect Pyomo constraints. Component failures join the overall QA report and therefore
make the Stage-2 publication decision fail closed.

## Validation policy

Component validation emits the canonical Stage-1 `ValidationIssue` with deterministic asset paths.
The transitional shared deterministic path is explicitly a legacy-compatibility mode: VRE/RoR
missing-profile and negative-CF legacy fallbacks are preserved with warnings. Native-v1 mode rejects
those cases. Non-numeric/non-finite data and unsafe import capacity/profile cases are errors rather
than raw conversion exceptions.

## Evidence and tolerances

Analytic/parity tests cover zero/full/fractional/clipped VRE availability, degradation, wake loss,
maintenance, monthly wind availability, air-density and temperature effects, RoR clipping and water
value, scalar/profiled imports, 60-minute and 15-minute semantics, mixed legacy/shared dispatch,
canonical extraction, rolling committed-period transport, QA and publication blocking.

Availability parity uses `1e-10 MW` for direct formulas. Optimization objective parity uses
`$0.01`; dispatch uses the legacy three-decimal extraction. Independent component QA uses the
centralized full-precision `TolerancePolicy`.

Shared `CostTerm` contributions are directly parity-tested against the current deterministic cost
formula at 60- and 15-minute resolution. They are not yet inserted into the authoritative Pyomo
objective; the transitional assembler deliberately keeps the legacy objective to prevent
double-counting.

## Known limitations and next sequence

RoR remains a CF model. Exchange remains import-only. No reserve/network capability is claimed.
The LP marginal-price re-solve and non-deterministic workflow writers remain later migration work.
Recommended next sequence: (1) thermal UC and complete rolling boundary state, (2) BESS and pumped
hydro, (3) demand response, (4) reservoir hydrology/cascades, (5) gas coupling, then Stage 4 reserve
and nodal DC-network hardening. Each extraction must repeat old/new analytic and golden parity
before legacy removal.
