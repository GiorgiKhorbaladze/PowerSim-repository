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
Direct parity tests cover these combined derates at 60-minute and 15-minute resolution.

Legacy compatibility and native-v1 validation are explicit. On the transitional shared solver path,
a missing VRE profile uses the legacy default and negative CF is clamped to zero, with canonical
warnings. Native-v1 mode rejects missing or negative profiles as validation errors. Non-numeric and
non-finite profile values are errors in both modes.

Shared `CostTerm` records are parity-tested but are **not yet added to the authoritative Pyomo
objective**; the transitional deterministic assembler intentionally keeps its legacy objective to
avoid double counting. Stage 3A provides no VRE reserve capability.
