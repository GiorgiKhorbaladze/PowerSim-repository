# Stage 3B - shared Thermal UC extraction audit

## Scope

The opt-in `solver_settings.component_engine="shared"` deterministic path now
registers a `ThermalComponent`. It owns the following operational constraints
for thermal assets in that path:

- commitment/startup/shutdown transition;
- pmin/pmax commitment bounds;
- minimum up/down time;
- time-scaled ramping, including the rolling-boundary dispatch state;
- rolling-boundary remaining minimum up/down enforcement;
- hot-start eligibility constraints where the legacy multi-stage variables are
  present.

The component extracts canonical full-precision thermal observations, including
commitment, startup/shutdown and active pmin/pmax bounds. Independent QA
reconstructs thermal bounds, consecutive-period UC transitions and time-scaled
ramps from resolved input plus those canonical observations. It does not read
live Pyomo constraints.

## Parity and intentional non-migration

The legacy deterministic objective remains authoritative in this migration.
This deliberately preserves current no-load, startup, hot/cold start,
piecewise heat-rate and CO2 objective behavior without a risk of double
counting. The shared component emits audit-only variable-cost terms; they are
not added to the Pyomo objective.

Piecewise heat-rate formulation and CO2 cost ownership are therefore **not yet
migrated**, and this stage does not claim they are. They require objective
reconstruction parity before Stage 4C integration. Thermal pmax uses a narrow
legacy-availability callback in this transition, preserving existing
maintenance/temperature derating exactly until that availability physics is
separately characterized and extracted.

## Evidence

`tests/components/test_thermal_component.py` verifies shared-versus-legacy
objective/dispatch/commitment parity, canonical thermal extraction, QA failure
on a corrupted bound, and 15-minute ramp scaling. Existing
`tests/test_thermal_stage3.py` remains the characterization suite for the
legacy hot/cold, CO2 and derating behavior.
