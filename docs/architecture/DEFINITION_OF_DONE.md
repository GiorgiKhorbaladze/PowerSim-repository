# PowerSim 1.0 FINAL — Definition of Done

“Implemented” is not equivalent to “done.” Evidence must be reproducible in CI or a versioned validation report. Any failed mandatory item blocks 1.0 FINAL.

## Release gates

### G0 — architecture and claims

- Target layers, ownership, component contracts, workflow capability matrix and migrations are approved.
- Public claims match tested behavior; limitations appear in UI, API and documentation.
- The schema, package, solver, workflow and result versions have one documented source of truth.

### G1 — contracts and package

- `powersim` installs from a clean checkout with pinned supported Python ranges; imports have no install-at-runtime behavior.
- Versioned input, project, scenario, run, progress, result, QA and error contracts have JSON Schema/OpenAPI publication and migration tests.
- Inputs have explicit units, calendar/timezone/resolution, stable IDs and cross-reference validation.
- Unknown fields/version handling is intentional; golden contracts round-trip across Python, API and UI.

### G2 — validated deterministic core

- Shared component physics replaces workflow-local copies for every supported component.
- Deterministic UC/ED passes analytic fixtures and reference cases for commitment, ramping, min-up/down, hydro, VRE, BESS, pumped hydro, DR, exchange, gas and rolling boundaries.
- Reserve engine co-optimizes declared products with eligibility, direction, response/ramp, headroom/footroom and energy-duration limits.
- DC network uses a declared slack/reference, nodal injection balance, angle/flow equations, branch limits and islands policy.
- Objective reconstruction agrees with the extracted incumbent within documented tolerance.

### G3 — solver truthfulness and QA

- HiGHS and Gurobi adapters normalize all required statuses without converting time limit or feasible incumbents into `optimal`.
- Every run reports backend/version, raw termination condition, incumbent objective, best bound, actual and requested MIP gaps, runtime, result validity and QA status; unavailable values are explicit `null` with reason.
- Automatic post-solve checks cover every invariant in `MODEL_INVARIANTS.md`, including rolling-window seams.
- Failed extraction, nonfinite values, infeasible/unbounded/numerical/error states and QA failures cannot be published as valid results.

### G4 — workflow credibility

- Deterministic is the parity reference.
- Stochastic UC is a genuine extensive-form or equivalently proven algorithm: declared first-stage decisions are non-anticipative, recourse uses the shared physics, probabilities/objective/CVaR are verified, and unsupported component combinations are rejected.
- Adequacy modes are separately named and validated: deterministic screening, probabilistic chronological simulation, and capacity-credit/expansion feedback. Each reports assumptions, sampling error and seed where relevant.
- Security/SCUC is genuine contingency analysis: line topology is altered per contingency, post-contingency flows and emergency limits are enforced/evaluated, generator/import outages are represented, and violations are traceable. Capacity-only screening cannot be labelled SCUC.
- Expansion is explicitly scoped; candidates, vintages, retirements, annualization, chronology/representative periods, adequacy/operational coupling and excluded investment questions are documented and tested.

### G5 — application and operations

- Users can create/open projects; configure assets, profiles, network, reserves and scenarios; validate; run/cancel; monitor; inspect status/results/QA; compare; and export without manual HTML → JSON → terminal → JSON → HTML handling.
- JSON import/export and a fully offline CLI/package workflow remain available.
- Run operations are idempotent, state transitions are durable, progress reconnects, failures are actionable, and artifacts are immutable/addressed by run ID.
- Confidential data need not enter Sites; deployment supports UI/API/worker/data separation, least privilege, encrypted transport, configurable retention and auditable access.
- Browser E2E tests exercise the ordinary happy path, validation failures, solver failure, cancellation, reconnect, comparison and export.

### G6 — release quality

- CI includes formatting/lint/type checks, unit/property/contract/integration tests, solver matrices, package build/install, migrations, security/dependency checks, deterministic reproducibility, browser E2E and documentation link/claim checks.
- Linux offline core is demonstrated; documented supported OS/browser/backend combinations pass.
- Reference datasets have provenance/licensing; generated and binary artifacts are governed and size-limited.
- User, operator, API, model methodology and developer documentation agree with code.
- Upgrade/rollback, backup/restore and incident diagnostics are rehearsed.
- **Zero known P0 or P1 defects.** P2 exceptions require documented impact, workaround, owner and release approval.

## Severity and sign-off

P0 means unsafe/corrupt results, data exposure/loss or system-wide inability to run. P1 means materially wrong model outcomes/claims, invalid status reporting or failure of a primary workflow without a safe workaround. Sign-off requires product, modelling, architecture, QA/security and release owners, with links to evidence and the exact release commit.
