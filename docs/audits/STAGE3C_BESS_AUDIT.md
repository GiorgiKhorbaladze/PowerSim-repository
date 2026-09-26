# Stage 3C - shared BESS core audit

## Scope

The shared deterministic path now owns core BESS state physics for migrated
assets: charge/discharge exclusivity, SOC balance, SOC limits, asymmetric
charge/discharge power limits, C-rate limit and time-scaled inverter ramps,
including rolling-boundary SOC and prior-flow state.

The existing objective remains authoritative. The component records matching
base degradation cost terms but does not add a second objective. Advanced
depth-dependent degradation and end-SOC soft-target constraints remain legacy
compatibility constraints until separately characterized.

## Evidence

`tests/components/test_bess_component.py` checks legacy/shared objective and
hourly-flow parity, canonical SOC/flow extraction, and independent result-based
QA failure when a power bound is corrupted. QA reconstructs bounds and
non-simultaneous charge/discharge from resolved input plus canonical results.
