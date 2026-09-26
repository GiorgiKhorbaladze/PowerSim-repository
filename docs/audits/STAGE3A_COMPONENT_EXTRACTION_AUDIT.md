# Stage 3A component extraction audit

## Scope and outcome

Shared protocol, context, typed ports, deterministic registry, canonical extraction, objective
terms, and independent result QA were added. Wind, solar, simplified run-of-river, and imports can
own their availability constraint in the hybrid deterministic assembler. Thermal, reservoir hydro,
BESS, pumped hydro, demand response, gas, reserves, and network remain legacy.

No equation was intentionally changed. The legacy implementation remains for rollback and
characterization. The shared engine is opt-in through `solver_settings.component_engine="shared"`;
the default is `legacy`. Both paths retain Stage-2 backend diagnostics and publication flow.

## Evidence and tolerances

Analytic tests cover zero/profile/scalar availability, clipping, degradation, maintenance,
60-minute and 15-minute duration, objective terms, canonical extraction, QA, and a mixed fleet.
Availability parity uses `1e-10 MW` for direct formulas. Optimization objective parity uses
`$0.01`; dispatch uses the legacy three-decimal extraction. Independent QA uses the centralized
`TolerancePolicy` (`1e-6` internal absolute and `1e-7` relative in the current implementation).

Canonical QA reconstructs finite/non-negative values, availability/import bounds, renewable
accounting, and MW-to-MWh curtailment from results only. It does not inspect Pyomo constraints.

## Known limitations and next sequence

Canonical results are currently exposed by the compatibility session but legacy output serialization
is unchanged. RoR remains a CF model. Exchange remains import-only. No reserve/network capability
is claimed. Recommended Stage 3B sequence is: (1) thermal UC and complete rolling boundary state,
(2) BESS and pumped hydro, (3) demand response, (4) reservoir/RoR hydrology and cascades, (5) gas
coupling, (6) reserves, then (7) nodal network/DC-OPF. Each extraction should repeat old/new analytic
and golden parity before removing any legacy implementation.

