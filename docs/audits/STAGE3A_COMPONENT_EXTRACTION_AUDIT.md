# Stage 3A component extraction audit

## Scope and outcome

Shared protocol, context, typed ports, deterministic registry, canonical extraction, objective
terms, and independent result QA were added. Wind, solar, simplified run-of-river, and imports can
own their availability constraint in the hybrid deterministic assembler. Thermal, reservoir hydro,
BESS, pumped hydro, demand response, gas, reserves, and network remain legacy.

No equation was intentionally changed. The legacy implementation remains for rollback and
characterization. The shared engine is opt-in through `solver_settings.component_engine="shared"`;
the default is `legacy`. Both paths retain Stage-2 backend diagnostics and publication flow.

Successful shared windows carry full-precision records on `SolvedRows.component_results`, and
result construction exposes them as `component_results`. Rolling aggregation selects records by the
same global coordinates as committed hourly rows and excludes look-ahead periods. Component QA
returns canonical `QACheckResult` objects through the existing registry and final `QAReport`; any
mandatory failure therefore makes the result invalid and blocks publication through the Stage-2
gate.

## Evidence and tolerances

Analytic tests cover zero/profile/scalar availability, clipping, degradation, maintenance,
60-minute and 15-minute duration, objective terms, canonical extraction, QA, and a mixed fleet.
Availability parity uses `1e-10 MW` for direct formulas. Optimization objective parity uses
`$0.01`; dispatch uses the legacy three-decimal extraction. Independent QA uses the centralized
`TolerancePolicy` (`1e-6` internal absolute and `1e-7` relative in the current implementation).

Canonical QA reconstructs finite/non-negative values, availability/import bounds, renewable
accounting, and MW-to-MWh curtailment from results only. It does not inspect Pyomo constraints.

Validation reuses the Stage-1 `ValidationIssue` and `assets.<asset_id>.<field>` paths. Native mode
rejects malformed inputs. Explicit legacy mode preserves missing VRE-profile and negative-CF
fallbacks with warnings and uses scalar import `pmax` for a missing named profile. Shared CostTerms
are directly parity-tested at 60 and 15 minutes, but are not authoritative yet: the transitional
assembler retains the legacy objective expression to prevent double counting.

## Known limitations and next sequence

Legacy output serialization remains backward compatible. RoR remains a CF model. Exchange remains
import-only. No reserve/network capability
is claimed. Recommended Stage 3B sequence is: (1) thermal UC and complete rolling boundary state,
(2) BESS and pumped hydro, (3) demand response, (4) reservoir/RoR hydrology and cascades, (5) gas
coupling, (6) reserves, then (7) nodal network/DC-OPF. Each extraction should repeat old/new analytic
and golden parity before removing any legacy implementation.
