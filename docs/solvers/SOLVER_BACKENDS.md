# Solver backends

Stage 2 supports two real Pyomo APPSI adapters: **Gurobi** and **HiGHS**. Gurobi is conditional on an importable installation and usable licence. HiGHS is the required CI backend. `auto` tries Gurobi first and then HiGHS; the diagnostics record both `requested_backend` and the selected `backend`. An explicit unavailable or unknown selection fails and never falls back. In particular, legacy input may still parse `cplex`, but the runtime registry rejects it.

Adapters own availability/version detection, strict option translation, solve execution and normalized diagnostics. Supported portable options are `mip_gap`, `time_limit_s`, `threads`, and `log_to_console`; invalid values and unknown spellings fail before solve. Compatibility pass-through exists only in the low-level normalization API when explicitly requested and is not used by canonical solves.

Diagnostics retain effective options, UTC solve timestamps, backend and orchestration runtime, raw termination, objective/bound observations, and unavailable-field metadata where applicable. APPSI offers no safe portable cancellation mechanism, so `cancel()` returns false; future process orchestration can terminate a worker externally.

The deterministic legacy assembler now delegates its canonical solve to this registry. Its equations and extraction format are unchanged. The LP marginal-price re-solve and the stochastic/adequacy/expansion wrappers remain legacy paths for Stage 3; stochastic scenarios reach the adapter indirectly through the deterministic solve.

## Incumbents and gaps

An incumbent exists only when the backend reports a finite best feasible objective. Variables are loaded only then. Requested MIP tolerance is configuration; it is never copied into `actual_mip_gap`. When finite incumbent and bound observations exist, the fallback relative gap is `abs(incumbent - bound) / max(abs(incumbent), 1e-10)`, valid for minimization and maximization. Missing/non-finite observations produce `null`.
