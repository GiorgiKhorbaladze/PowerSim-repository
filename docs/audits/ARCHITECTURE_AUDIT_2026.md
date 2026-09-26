# PowerSim architecture audit — 2026

**Snapshot audited:** commit `1b093e0` (26 September 2026 review).  
**Method:** repository-wide file inventory; source/symbol and contract inspection; tests/CI/docs review; targeted reading of model, orchestration, security and persistence paths. Labels describe the migration disposition, not immediate deletion.

## Executive findings

The repository contains meaningful deterministic physics and regression coverage, but is a source-tree application rather than an installable layered package. `powersim_solver.py` is a 3,288-line model/orchestrator/extractor/exporter; workflow and solver concerns leak across modules. Status/gap/provenance are not yet a dependable product contract. Network/security claims are especially risky: `powersim_scuc.py` explicitly skips line outages and performs capacity screening, not SCUC. The consensus stochastic wrapper does not actually enforce its calculated consensus in recourse; an extensive-form module exists but has a separate physics surface. Adequacy is a compact screening/sampling calculation, while expansion is a separate planning approximation. These distinctions must be visible.

The HTML and AI FastAPI pieces demonstrate a control surface but ordinary operation remains file/terminal handoff. SQLite is a useful prototype projection, not yet a durable run system. Schema validation is hand-coded and version authority is repeated across Python/HTML/docs. README also contains stale BESS-sizing claims after that module was removed.

## Module disposition

| Existing area | Disposition | Evidence and migration action |
|---|---|---|
| `solver/powersim_solver.py` | **REFACTOR**, then **REMOVE AFTER MIGRATION** | Preserve characterized equations while extracting components, workflow assembly, backend, extraction/QA and export. Keep thin facade until parity. |
| `solver/powersim_adequacy.py` | **REFACTOR** | Useful metrics/screening base, but separate deterministic derating, probabilistic chronological adequacy and expansion feedback with explicit assumptions. |
| `solver/powersim_expansion.py` | **REFACTOR** | Retain as scoped screening; move costs/candidates/contracts to shared domain and connect validated adequacy/operational representations. |
| `solver/powersim_scuc.py` | **REPLACE** | Capacity-only post-solve verifier ignores line contingencies; retain renamed screening logic if useful, but implement genuine topology/flow contingency analysis before SCUC claim. |
| `solver/powersim_stochastic.py` | **DEPRECATE** | Consensus heuristic computes votes but uses independent scenario costs; retain only as clearly named heuristic/benchmark during migration. |
| `solver/powersim_stochastic_efs.py` | **REFACTOR** | Candidate genuine extensive form, but reconcile every component with shared physics, backend/status/result/QA contracts and acceptance cases. |
| `solver/powersim_dataio.py` | **REFACTOR** | Split parsers, project resolver, normalization/units, validation and provenance; retain robust file readers. |
| `solver/powersim_asset_mapper.py` | **REFACTOR** | Preserve domain mapping knowledge; split source-specific import adapter from canonical assets and diagnostics. |
| `solver/cost_database.py`, `load_dataio.py` | **KEEP**, then **REFACTOR** | Useful adapters/reference data; add licensing/provenance, typed contracts and package resources. |
| `solver/powersim_kpi.py`, `bess_report.py` | **REFACTOR** | Move to results/report layer; version KPI formulas and consume canonical results. |
| `schema/powersim_schema.py` | **REPLACE** behind compatibility adapter | Hand-coded 1,188-line validation mixes schema versions/domain rules. Introduce typed versioned contracts, generated JSON Schema and migrations; preserve legacy load tests. |
| `scripts/run_horizon.py` | **REFACTOR** | Become thin CLI over project validation/run services; remove subprocess and duplicated workflow logic. |
| `scripts/batch.py` | **REFACTOR** | Preserve queue semantics but delegate durable/idempotent runs; current sequential subprocess manifest is not a run manager. |
| `scripts/db_ingest.py` | **REPLACE** behind adapter | SQLite projection is useful locally; canonical run/results store needs migrations, normalized diagnostics/QA/provenance and transactional service. |
| remaining `scripts/*` | **KEEP**, then **REFACTOR** | Treat builders/import/report/sweeps as thin package clients; eliminate path manipulation and direct internals. |
| `backend/*` | **REFACTOR** | Preserve safety, intent and summarization tests; separate API/application/domain, add project/run resources, durable jobs/auth, and make AI optional. Current API is assistant-oriented, not the product backend. |
| `html/PowerSim_v4.html` | **REPLACE** after feature migration | Monolithic offline prototype duplicates contracts/state; unified app should consume API/shared contracts. Preserve as temporary offline/reference client. |
| `html/powersim_ai_chat.*` | **PARTIALLY MERGE INTO APPLICATION** | Reuse confirmed-action UX and safe summaries; do not make AI or Sites a solver/data store. |
| generated profile JS / RoR binding JS | **MERGE INTO SHARED COMPONENT / DATA ADAPTER** | Move data binding and validation to canonical project import; UI renders references rather than owning model semantics. |
| `tests/*` | **KEEP AND REFACTOR** | Strong targeted regressions are valuable. Organize unit/contract/integration/solver/E2E, eliminate direct-script ambiguity, add negative/status/network/security/stochastic parity and invariant tests. |
| `.github/workflows/python-checks.yml` | **REFACTOR** | Broad checks exist, but add package build, lint/type, full pytest discovery, backend matrices, E2E, security and doc/claim checks; avoid hand-maintained file lists. |
| `docs/*` | **REFACTOR** | Preserve useful workflows/safety/domain notes, reconcile versions and remove superseded claims. Architecture docs become normative; generated references derive from contracts. |
| `README.md` | **REPLACE** after migration | Current v4/file-handoff framing conflicts with 1.0 target and includes removed BESS-sizing module; rewrite only when implemented behavior changes. |
| samples/reference data/binary archives | **KEEP WITH GOVERNANCE** | Separate small licensed fixtures from operational/reference data; add provenance, checksums, retention and repository-size policy. |

## Feature and claim risks

* Deterministic features are concentrated in a single solve function, increasing regression and parity risk.
* HiGHS usage is primary; Gurobi behavior and normalized diagnostics lack an acceptance matrix.
* “SCUC (N-1)” naming overstates a verifier that skips transmission outages.
* Two stochastic entry points and an additional stochastic wrapper inside the monolith make semantics ambiguous.
* DC/network code must be proven by nodal balance, flow equation, limits, slack/islands and contingency fixtures before “technically correct” claims.
* Current output validation is structural; automatic independent invariant QA is incomplete.
* CI invokes a mixture of scripts/unittest/pytest and manually enumerates files, making omission easy.
* No packaging metadata/lock, durable run state, browser E2E, or unified product backend exists yet.

## Open PR review: #63, #64 and #65

GitHub PR pages/API and pull refs were unavailable from this execution environment (HTTP/tunnel 401/403), and the supplied repository contains no refs or metadata for these PRs. A content-based merge recommendation would therefore be fabricated. Provisional disposition for **each of #63, #64 and #65: `partially reused` pending mandatory re-review**—do not merge or close in this architecture task. Cherry-pick only contract-compatible, independently tested leaf changes; supersede conflicting monolith/schema/UI architecture, and close only after replacement evidence exists. The architecture owner must record title/head SHA/file diff/tests for each PR before implementation Stage 1; then choose exactly `merged as-is`, `partially reused`, `superseded`, or `closed`.

This provisional decision is intentionally conservative: no unknown PR should merge as-is into a frozen architecture, while potentially valuable tests/data/bug fixes should not be discarded unseen.

## Priority recommendations

1. Freeze honest capability language and remove SCUC/exact-stochastic overclaims from user-facing material before further features.
2. Establish installable contracts, solver result truthfulness and independent QA before equation extraction.
3. Characterize the deterministic model, then extract shared components incrementally.
4. Harden reserves and DC network; only then rebuild stochastic/security and distinguish adequacy/expansion modes.
5. Add project/run/provenance services before replacing the UI handoff.

The dependency order, parallel ownership and release gates are normative in the adjacent architecture documents.
