# Stage 2 solver truthfulness audit

## Delivered

- Added backend-neutral solve protocol/outcome, strict registry, real HiGHS adapter and conditional real Gurobi adapter.
- Added exact normalized statuses, raw termination retention, independent incumbent handling, actual-gap calculation, backend version, effective options, timestamps and runtimes.
- Replaced canonical deterministic backend selection and solve block with the adapter seam. Explicit unsupported selections no longer silently fall back.
- Replaced hard-coded `solver_status = "solved"` and configured-gap-as-observed-gap reporting. Run state is returned explicitly: rolling windows retain coordinates and diagnostics, worst status deterministically dominates, and no-incumbent windows abort without committing placeholder periods.
- Rolling-window incumbent/bound/gap observations remain per-window. Multi-window aggregates deliberately report null objective, bound and gap because overlapping windows do not define a mathematically comparable global primal/bound pair; single full-horizon solves retain their real backend gap.
- Added independent finite, net-supply balance and basic-domain QA, centralized tolerance policy, objective reconstruction hook, and fail-closed publication decision. `build_result_store` runs QA in explicit persisted-output tolerance mode, evaluates publication, and finalizes/attaches diagnostics, QA and the decision.
- Gurobi warm starts explicitly submit finite Pyomo values as native `Start` attributes through APPSI's persistent interface. `LPWarmStart=2` is supplemental; the submitted-value count is captured in diagnostics. HiGHS continues to use its native APPSI warm-start path.
- Added focused backend/status/gap/options/QA/publication tests and a CI Stage-2 test step.

No mathematical model equation was changed.

## Remaining legacy paths / Stage 3

The marginal-price fixed-commitment ED re-solve still constructs HiGHS directly. Applying the gate to the stochastic module's independent writer and adequacy/expansion/security auxiliary writers, canonical component extraction, complete objective reconstruction, richer storage/hydro/reserve/network QA, and final elimination of diagnostic-only legacy placeholder construction remain Stage 3 work. Adequacy, expansion, stochastic orchestration and security screening were deliberately not redesigned.

## Truthfulness conclusions

Supported claims now match implementation: HiGHS and conditionally licensed Gurobi only. `cplex` remains parse-compatible but runtime-unsupported. A returned solve call alone cannot establish validity; extraction and independent QA are separately required. Requested tolerance and observed gap are separate and missing observations remain null.
