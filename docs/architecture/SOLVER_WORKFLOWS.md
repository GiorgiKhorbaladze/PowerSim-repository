# Solver backends and workflow engines

## Backend abstraction and result contract

The backend protocol covers availability/capabilities, option validation, model solve, interruption, log/event capture, raw result retention and normalized diagnostics. Workflow code cannot branch on HiGHS/Gurobi APIs.

Required normalized `status` values are exactly: `optimal`, `feasible`, `time_limit`, `infeasible`, `unbounded`, `numerical_error`, `solver_error`. `time_limit` remains `time_limit` whether or not an incumbent exists; usability is expressed separately.

Every solve attempt records:

| Field | Rule |
|---|---|
| `backend`, `backend_version` | actual executable/library, never requested alias |
| `termination_condition` | raw condition plus normalized mapping |
| `incumbent_objective` | finite incumbent or `null` with reason |
| `best_bound` | finite bound or `null` with reason |
| `actual_mip_gap` | computed consistently from incumbent/bound, or `null`; never echo requested gap |
| `requested_mip_gap` | effective configured tolerance |
| `runtime_s` | backend runtime and orchestration wall time separately |
| `result_validity` | `valid|valid_with_warnings|invalid` after extraction/QA |
| `qa_status` | `pass|warn|fail|not_run` |

Also record sense, nodes/iterations where available, effective options, model size, log artifact, solve-attempt ID and whether an incumbent exists. Status mapping has adapter-specific tests for every terminal condition.

## Workflow ownership

### Deterministic UC/ED

Reference assembler for chronological component physics, system coupling, rolling/look-ahead policy and optional LP price resolve. ED is UC with commitment disabled/fixed, not a divergent model. Rolling execution persists complete boundary state and reports each window plus aggregate diagnostics.

### Stochastic UC

Owns scenario tree, probability normalization, stage mapping, non-anticipativity and expected/risk objective. It instantiates the same component builders with scenario coordinates. The current consensus wrapper is not to be represented as an exact two-stage solution; heuristic modes, if retained, are explicitly named. Extensive-form support advances only after deterministic component parity and cross-scenario QA.

### Adequacy

Owns adequacy sampling/accounting rather than dispatch physics. Modes are explicit: (1) deterministic derated screening, (2) chronological probabilistic simulation with outages/seed/convergence, and (3) expansion-plan adequacy feedback. LOLE, LOLP, EENS and reserve-margin definitions specify denominator, chronology and treatment of storage/hydro/imports. No mode silently substitutes for another.

### Security / SCUC

Security screening may identify candidate contingencies, but “SCUC” requires preventive/corrective decision policy plus contingency-specific topology, flows and limits. Generator, import and branch outages are modeled distinctly. PTDF/LODF or angle formulations are acceptable only with validated equivalence and island handling. Current capacity-only post-solve checks remain labelled screening until replaced.

### Capacity expansion

Owns candidate/vintage investment variables and inter-year accounting, and calls shared operational/adequacy representations at the chosen fidelity. 1.0 is a scoped screening solver: assumptions about representative chronology, perfect foresight, transmission, retirement, reliability and finance must be visible. It must not be marketed as a full integrated resource plan without corresponding evidence.

## Capability matrix required before release

Maintain a machine-readable matrix by workflow × component × feature × backend. `supported` requires validation/acceptance evidence; `limited` names the limitation; `experimental` is opt-in and excluded from 1.0 claims; missing means `unsupported`. CI verifies the matrix against registered components and documentation.
