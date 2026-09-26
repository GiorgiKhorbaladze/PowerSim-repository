# Run-of-river component

Run-of-river deliberately preserves the legacy simplified availability model:
`available[t] = pmax × clamp(cf[t], 0, 1) × maintenance[t] × temperature_derate[t]`.
If no profile is named, `cf` defaults to the asset value and then to `0.65`. There is no explicit
water balance, reservoir state, inflow conversion, or cascade physics in this component.

Dispatch is bounded by availability. `_dispMC` continues to include the legacy asset-map maximum
of ordinary marginal cost and positive `hydro.water_value`; its objective is dispatch MWh times
that marginal cost (plus the existing VOM rule). Unused availability is the canonical spill-like
curtailment quantity, not a newly introduced volumetric spill equation.

