# Legacy compatibility

`powersim.validation.legacy_adapter` wraps, but does not modify, `schema/powersim_schema.py`. It preserves the original payload in the immutable `ResolvedInputContract.legacy_payload`, invokes legacy validation when available, maps legacy errors/warnings to structured issues, and produces typed assets, profiles, units, time, settings, reserves, and network data where meaning is safe.

Legacy schemas `1.0`–`1.5` are recognized. Unknown versions fail closed. Unspecified profile units remain visibly `legacy_unspecified`; currency and water metadata are not invented. The adapter can represent partial horizons with `explicit_periods`. Legacy `x_pu` requires explicit positive `base_mva`; otherwise adaptation emits an error and does not fabricate susceptance.

Known limitations:

* Type-specific historical asset fields are retained only in the explicit, immutable `legacy_extensions` mapping pending Stage 3 component contracts. Native v1 inputs reject unknown asset fields, and compatibility extensions do not automatically activate physics.
* The original validator requires full-year time/profile arrays even when `study_horizon` requests a shorter solve. The CLI therefore accepts the existing 168-hour-study sample (which carries full-year arrays), but preserves errors and exits nonzero for physically truncated 168-period payloads. Full 8,760-period non-leap fixtures accepted by the legacy validator exit successfully.
* Some historical network field aliases are supported, but transformer tap/sign conventions without explicit provenance are not inferred.
* Stage 1 validates and normalizes contracts only; it does not route the mathematical solver through the new snapshot.

The adapter may be removed only after every supported legacy fixture has an explicit migration and all active entry points consume current contracts.
