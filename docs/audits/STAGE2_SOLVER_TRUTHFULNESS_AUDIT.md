# Stage 2 solver truthfulness audit

## Delivered

- Added backend-neutral solve protocol/outcome, strict registry, real HiGHS adapter and conditional real Gurobi adapter.
- Added exact normalized statuses, raw termination retention, independent incumbent handling, actual-gap calculation, backend version, effective options, timestamps and runtimes.
- Replaced canonical deterministic backend selection and solve block with the adapter seam. Explicit unsupported selections no longer silently fall back.
- Replaced hard-coded `solver_status = "solved"` and configured-gap-as-observed-gap reporting. Legacy no-incumbent placeholder rows remain structurally available but carry invalid canonical diagnostics and cannot pass the publication gate.
- Added independent finite, balance and basic-domain QA, centralized tolerance policy, objective reconstruction hook, and fail-closed publication decision.
- Added focused backend/status/gap/options/QA/publication tests and a CI Stage-2 test step.

No mathematical model equation was changed.

## Remaining legacy paths / Stage 3

The marginal-price fixed-commitment ED re-solve still constructs HiGHS directly. Full rolling-window diagnostic aggregation, application of the publication gate to every historical JSON writer, canonical component extraction, complete objective reconstruction, richer storage/hydro/reserve/network QA, and elimination of legacy placeholder payloads remain Stage 3 work. Adequacy, expansion, stochastic orchestration and security screening were deliberately not redesigned.

## Truthfulness conclusions

Supported claims now match implementation: HiGHS and conditionally licensed Gurobi only. `cplex` remains parse-compatible but runtime-unsupported. A returned solve call alone cannot establish validity; extraction and independent QA are separately required. Requested tolerance and observed gap are separate and missing observations remain null.
