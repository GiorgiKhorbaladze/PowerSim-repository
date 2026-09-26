# Stage 1 package and contracts audit

## Implemented

Stage 1 adds an installable `src/powersim` package, one product version source, CLI version/validation commands, typed v1 contracts, deterministic scenario resolution and fingerprints, semantic validation, a loss-preserving legacy adapter, explicit migration dispatch, generated JSON Schemas, tests, and minimal CI package checks.

The contract separates solver termination, result validity, QA status, and run lifecycle. The review corrections enforce one imported contract-version constant, a complete status/incumbent/validity matrix, and equality between duplicated envelope/diagnostic status values. Diagnostics use nullable observations. Network normalization requires unambiguous MW/radian susceptance and records per-unit conversion provenance. Units and calendar semantics, including exact full-year divisibility, are explicit.

Resolved snapshots now freeze the complete nested value graph and detach from editable projects. Scenario overlays cannot modify project identity or version lineage. Native assets fail on unknown fields; only the legacy adapter may preserve historical keys in the explicit immutable `legacy_extensions` compatibility payload.

## Evidence and limits

The Stage 1 suite covers import/version/CLI, typed round trips, schema generation, successful full-year and intentionally failing partial-horizon legacy CLI behavior, version rejection, protected overlays, deep immutability/fingerprints, native/legacy extension handling, network units, the full solver-status matrix, result/QA consistency, nullable diagnostics, calendar divisibility, and duplicate/dangling references. Existing JSON contract checks remain part of CI.

No solver equation, objective, UC, hydro, storage, reserve, stochastic, adequacy, expansion, or DC-flow equation was changed. `solver/powersim_solver.py` and `schema/powersim_schema.py` remain untouched.

## Stage 2 remainder

Stage 2 must implement actual backend protocols/adapters, route legacy solves through truthful normalized diagnostics, record reasons for unavailable diagnostic values, introduce independent result extraction/QA publication gates, and test the complete HiGHS/Gurobi termination matrix. Stage 3 and later own equation extraction; those changes are intentionally outside this work.
