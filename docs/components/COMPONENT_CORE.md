# Stage 3A component core

The component protocol is deliberately small and backend-neutral. `BuildContext` carries the
builder, ordered periods, canonical period duration, study/scenario metadata, resolved profiles,
boundary state, requested capabilities, asset references, typed power ports, cost terms, and
canonical validation issues. Components never read solver-module globals.

Power uses period-average **MW** and energy uses **MWh**. Positive injection supplies the
electricity balance; positive withdrawal consumes from it. `CurtailmentPort` is the actual
`available - dispatch` quantity, not availability itself. Cost terms are USD per period.

Canonical `ComponentResult` records remain full precision and name physical concepts rather than
Pyomo variables. On the shared deterministic path they are carried explicitly on the solve payload,
then serialized under top-level `component_results`. Rolling runs keep only committed periods;
look-ahead observations are never published twice.

Stage 3A components support `deterministic` and `subhourly`. They do not advertise stochastic,
reserve, contingency, or network capabilities. Unsupported requested capabilities fail before
construction. Registry order is `wind`, `solar`, `hydro_ror`, `import`; aliases belong in
compatibility adapters.

Validation uses the Stage-1 `ValidationIssue` / `IssueSeverity` contracts with paths such as
`assets.<id>.<field>`. The transitional deterministic seam runs in legacy-compatibility mode:
legacy VRE missing/negative profile fallbacks are retained with warnings. Native-v1 component use
can disable compatibility mode and then rejects those inputs. Structurally invalid import
capacities/profiles remain errors because the legacy path did not provide a safe numeric fallback.

`solver_settings.component_engine` is the explicit `legacy|shared` migration flag and defaults to
`legacy`. The shared path replaces only migrated upper-bound constraints, so an asset cannot be
double constrained by old and new implementations. Remove the flag only after parity is retained
for the approved migration window, all supported components have moved to shared physics, and the
legacy path has no production consumers (at least two releases unless separately approved).
