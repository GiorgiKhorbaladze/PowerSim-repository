# PowerSim 1.0 migration progress

## Current state

| Item | Status |
| --- | --- |
| Completed stage | Stage 3A merged in PR #69 |
| In progress | Stage 3B - shared Thermal UC |
| Shared components | wind, solar, simplified run-of-river, import |
| Components still legacy | thermal objective/heat-rate formulation, BESS, pumped hydro, demand response, reservoir hydro, gas coupling, reserves, network |
| Release readiness | Not release-ready; capability claims remain constrained by the architecture audit. |

## Stage 3B acceptance intent

The opt-in shared engine must reproduce legacy thermal dispatch and objective
within documented tolerances, while producing canonical thermal result records
and independent result-based QA for bounds, UC transitions and time-scaled
ramps. The legacy objective remains authoritative during the migration so that
cost terms cannot be counted twice. Piecewise heat-rate and CO2 objective
ownership remain a Stage 4C integration item; this is a deliberate, documented
limitation rather than a claim of completed migration.

## Next work

1. Complete thermal parity coverage for rolling-boundary, hot/cold starts,
   piecewise heat-rate and CO2.
2. Move thermal cost assembly only after objective reconstruction parity passes.
3. Start Stage 3C (BESS, pumped hydro and demand response) on a separate branch.

## Blockers

None identified for routine engineering. Electricity-system modelling decisions
will be escalated only if a physical equation or study interpretation must change.
