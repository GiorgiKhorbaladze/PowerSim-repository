# Incremental migration plan

The migration uses characterization tests and a strangler pattern. Existing validated paths remain runnable until their replacements meet equivalence and release gates. No destructive big-bang rewrite is permitted.

## Stage 0 — freeze and measure

Approve these documents; inventory claims/capabilities; capture golden inputs/results, solver logs, rolling seams and known defects; classify sensitive/binary data; establish performance and numerical baselines. Add deprecation policy. **Exit:** repeatable legacy baseline and agreed claim matrix.

## Stage 1 — package and contracts

Create installable `src/powersim`; introduce typed, versioned project/input/result/QA/run contracts and explicit migrations. Wrap current schema/CLI without changing equations. Publish JSON Schema/OpenAPI and contract fixtures. **Exit:** clean install, round trips and legacy JSON compatibility.

## Stage 2 — solver/result truthfulness

Add backend protocol/adapters and normalized status diagnostics. Route the legacy model through adapters; add independent result envelope and QA skeleton. Never infer actual gap from requested gap. **Exit:** termination matrix tests on HiGHS/Gurobi and invalid results blocked.

## Stage 3 — extract shared physics

Define ports/build/boundary protocols. Extract one component at a time from `powersim_solver.py`, beginning with passive VRE/exchange, then thermal, storage/DR, hydro/cascades, gas, reserves and network. Run old/new formulations on tiny analytic and golden cases; keep compatibility facades. **Exit:** deterministic parity within approved tolerances and complete rolling state.

## Stage 4 — deterministic, reserve and DC-network hardening

Make deterministic assembler canonical; implement product-correct reserve coupling and technically correct nodal DC formulation. Complete invariant QA and LP price-resolve semantics. **Exit:** Gates G2–G3 and performance budget.

## Stage 5 — workflow parity

In parallel after Stage 4 contracts: rebuild stochastic on shared components/non-anticipativity; split and validate adequacy modes; implement genuine contingency topology/flows for security; constrain expansion claims and connect shared operational/adequacy abstractions. **Exit:** every advertised matrix cell has evidence; heuristic legacy modes are renamed/deprecated.

## Stage 6 — projects, runs and storage

Introduce immutable project versions/scenario overlays, durable run state/events, provenance manifests, artifact abstraction and database migrations. Adapt `run_horizon.py`, `batch.py` and `db_ingest.py` to services. **Exit:** restart/idempotency/cancellation/reproduction integration tests.

## Stage 7 — unified application

Build API resources and Sites/UI flow against OpenAPI. Move embedded HTML logic behind shared contracts; retain JSON import/export and offline client. Implement secure separated deployment. **Exit:** browser E2E ordinary workflow with no terminal relay; confidentiality tests.

## Stage 8 — deprecate and release

Run dual paths through a published deprecation window, migrate samples/docs, remove misleading claims and only then remove legacy modules/adapters. Build/install, offline, security, backup/restore and full release matrices pass. **Exit:** all Definition-of-Done gates and zero P0/P1.

## Recommended agent sequence and parallelism

1. Architecture/contracts owner, then packaging owner.
2. Backend/status owner and QA/result owner in parallel after result contract freeze.
3. Component protocol owner, then leaf component owners in parallel by file boundary.
4. Deterministic/rolling integrator alone on central assembler; reserve/network owners integrate through reviewed ports.
5. Stochastic, security, adequacy and expansion owners in parallel after deterministic/shared-physics gate.
6. Project/run/storage and API owners in parallel after run contracts; UI works against mocks concurrently.
7. E2E/release/docs owner integrates last.

The central schema/version registry, build context/component registry, deterministic assembler, result envelope, DB migration head, package exports/lock and CI workflow always have one owner at a time.

## Rollback and removal rule

Every stage has a feature flag or adapter rollback, data migration backup, equivalence report and observable success criteria. Remove a legacy path only after two releases or an explicitly approved window, no production consumers remain, replacement telemetry is healthy, and its tests are transferred—not deleted.
