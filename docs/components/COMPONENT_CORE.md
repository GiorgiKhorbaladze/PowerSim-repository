# Stage 3A component core

The component protocol is deliberately small and backend-neutral. `BuildContext` carries the
builder, ordered periods, canonical period duration, study/scenario metadata, resolved profiles,
boundary state, requested capabilities, asset references, typed power ports, cost terms, and
validation issues. Components never read solver-module globals.

Power uses period-average **MW** and energy uses **MWh**. Positive injection supplies the
electricity balance; positive withdrawal consumes from it. Cost terms are USD per period.
Canonical `ComponentResult` records remain full precision and name physical concepts rather than
Pyomo variables.

Successful shared solves attach these records to `SolvedRows.component_results` and expose them in
the additive top-level `component_results` result field. No module global is used. Rolling solves
retain only records whose global period coordinate is in the committed slice, so look-ahead values
cannot leak into published results. Component checks return the existing `QACheckResult` contract
and join the single final `QAReport`; a mandatory failure is invalid and non-publishable.

Stage 3A components support `deterministic` and `subhourly`. They do not advertise stochastic,
reserve, contingency, or network capabilities. Unsupported requested capabilities fail before
construction. Registry order is `wind`, `solar`, `hydro_ror`, `import`; aliases belong in adapters.

`solver_settings.component_engine` is the explicit `legacy|shared` migration flag and defaults to
`legacy`. The shared path replaces only migrated upper-bound constraints, so an asset cannot be
double constrained by old and new implementations. Remove the flag only after parity is retained
for the approved migration window, shared extraction is wired into the result envelope, and the
legacy path has no production consumers (at least two releases unless separately approved).

Validation emits the Stage-1 canonical `ValidationIssue`, using paths of the form
`assets.<asset_id>.<field>`. Native mode rejects malformed, non-finite, negative, missing, or short
inputs. The migration seam explicitly selects legacy mode: missing VRE profiles retain the historic
full-availability fallback and negative CF is clamped to zero, both with warnings. A missing import
profile falls back to scalar `pmax` with a warning; negative import capacity remains invalid.
