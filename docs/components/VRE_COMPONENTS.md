# Wind and solar components

For capacity factor `cf[t]`, the migrated legacy equation is:

`available[t] = installed × min(1, max(0, cf[t]) × dc_ac_ratio) × inverter_efficiency × degradation × derates[t]`.

`dispatch[t]` is non-negative and no greater than availability. Curtailment is
`available-dispatch` MW and `(available-dispatch)×duration_h` MWh. The existing objective remains
`(_dispMC + vom) × dispatch × duration_h`; no curtailment penalty is activated.

Wind additionally preserves wake loss, twelve-bin monthly availability, and optional inverse
absolute-temperature air-density correction. Solar does not apply those wind-only multipliers.
Both preserve temperature and maintenance derating exactly once. Profiles retain window-local
indexing; maintenance uses `offset_h + round(index×duration_h)`, matching legacy behavior.

`CurtailmentPort` carries `available_mw - dispatch_mw`, rather than availability. Full-precision
canonical extraction converts that value to MWh using the canonical period duration.

The shared path provides no VRE reserve capability in Stage 3A. Negative CF and missing-profile
fallbacks exist only in explicit legacy compatibility mode and produce canonical warnings; native
mode rejects them. Shared CostTerm extraction is implemented and parity-tested, while the
transitional deterministic assembler still uses the legacy objective expression to avoid double
counting.
