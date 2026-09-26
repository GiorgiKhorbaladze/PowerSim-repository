# Run-of-river component

Run-of-river deliberately preserves the legacy simplified availability model:
`available[t] = pmax × clamp(cf[t], 0, 1) × maintenance[t] × temperature_derate[t]`.
If no profile is named, `cf` defaults to the asset value and then to `0.65`. An explicit zero
`cf` remains zero. There is no explicit water balance, reservoir state, inflow conversion, or
cascade physics in this component.

A named profile missing from a legacy-compatible run uses the same legacy CF fallback and emits a
warning; native-v1 validation rejects it. Dispatch is bounded by availability. `_dispMC` continues
to include the legacy asset-map maximum of ordinary marginal cost and positive
`hydro.water_value`; the shared cost term uses the same `(_dispMC + vom) × MWh` convention.
Those cost terms are parity evidence in Stage 3A, while the legacy objective remains authoritative.

Unused availability is the canonical spill-like curtailment quantity, not a newly introduced
volumetric spill equation. Full reservoir/cascade hydrology remains later Stage 3 work.
