# PowerSim 1.0 target architecture

**Status:** architecture decision baseline  
**Audience:** product, modelling, backend, UI, QA, and release owners  
**Rule:** this document defines the destination; `MIGRATION_PLAN.md` defines how to reach it without a rewrite.

This document describes the **PowerSim 1.0 target state**, not the capability of the current `main` branch. No target capability may be presented as already implemented until its applicable release gate in `DEFINITION_OF_DONE.md` has passed with recorded evidence.

## Product boundary

PowerSim 1.0 is an offline-capable, production-quality platform for chronological electricity-system planning and operational studies. It supports deterministic UC/ED, stochastic UC, adequacy assessment, security-constrained UC, and scoped capacity-expansion screening using shared representations of thermal, reservoir and run-of-river hydro, wind, solar, BESS, pumped hydro, demand response, reserves, DC networks, and imports/cross-border exchange.

Within those workflows, 1.0 supports project/scenario management, validated versioned inputs, rolling horizons, HiGHS and Gurobi backends, auditable termination and gap reporting, automatic result QA, comparison/reporting, a durable run store, and a unified browser application. JSON remains a first-class import/export and automation contract.

PowerSim 1.0 **does not claim** to perform AC power flow, voltage or reactive-power studies, transient stability, electromagnetic-transient simulation, protection coordination, production-cost-to-meter commercial settlement, or exhaustive market-rule simulation. DC flow and N-1 claims apply only to declared buses, branches, monitored elements, contingencies, and remedial assumptions. Adequacy results are not dynamic reliability studies. Expansion is a planning screen, not a siting, permitting, or investment-grade financial model.

## Ten layers and dependency rule

Dependencies point downward; lower layers never import UI or workflow code.

1. **Application/UI** — ChatGPT Sites / PowerSim UI renders projects, editors, run progress, results, comparisons and QA. It issues API commands; it never builds or solves Pyomo models.
2. **Project and scenario manager** — immutable base project plus explicit scenario overlays, lineage, permissions and artifact references.
3. **Validation** — parse, migrate, normalize, semantic/cross-field validate, and fingerprint versioned contracts before queuing.
4. **Run manager** — state machine, idempotency, cancellation, resource limits, events, artifact paths and recovery.
5. **Workflow engines** — deterministic UC/ED, stochastic UC, adequacy, security/SCUC and expansion orchestration.
6. **Shared physical component library** — variables, constraints, parameters, state boundaries and result extraction reused by workflows.
7. **Solver backend abstraction** — backend capability detection, options, solve lifecycle, normalized status and diagnostics for HiGHS/Gurobi.
8. **QA/result validation** — independent invariant checks; never trusts a solver's success flag alone.
9. **Results/reports/database** — canonical results contract, artifact store, comparisons, database projections and reports.
10. **Reproducibility/provenance** — content hashes, schema/model/code/backend versions, effective settings, environment and random seeds span every layer.

Cross-cutting security, observability and authorization apply to layers 1–10. The Python package is authoritative for contracts and computation. GitHub is authoritative for versioned source, migrations, documentation, reference data and tests; neither Sites nor a database may silently redefine model behavior.

## Target package shape

```text
src/powersim/
  contracts/       # versioned domain/input/result models and migrations
  validation/      # syntactic and semantic validators
  projects/        # project/scenario manifests and overlay resolution
  runs/            # run service, state machine, events and provenance
  components/      # thermal, hydro, vre, storage, dr, reserves, network, exchange
  workflows/       # deterministic, stochastic, adequacy, security, expansion
  solvers/         # protocol, highs.py, gurobi.py, status normalization
  qa/              # invariant evaluators and tolerances
  results/         # writers, readers, reports, comparisons, database adapters
  api/             # transport-only FastAPI routes and DTO mapping
  cli/             # thin commands using the same application services
```

No workflow owns duplicate component equations. A workflow selects components, indexing and coupling policy. Components receive typed build contexts rather than importing a concrete solver. Reporting consumes canonical results, not live Pyomo objects. Legacy entry points remain adapters during migration.

## Authority and contracts

The normalized project snapshot is the only solver input. Scenario overlays are resolved and validated before execution; they cannot mutate their base. Every run persists: resolved input, contract version, hashes, workflow/version, code commit and dirty flag, backend/version, effective options, platform/dependency lock identity, seed, timestamps, normalized result envelope, QA report and logs.

A solver result is publishable only when its normalized status permits an incumbent, required values are finite, extraction succeeds, and QA is not `fail`. “Optimal” describes solver termination, while `result_validity` and `qa_status` describe usability; they are not interchangeable.

## Architectural decisions

* Sites is an application/control client, never a source of equations or confidential authoritative datasets.
* The API is transport-neutral and invokes the same services as CLI/offline use.
* Jobs execute outside request processes; progress is event-driven and reconnectable.
* Large/confidential data stays in a user-controlled solver/data environment. Sites stores opaque IDs and presentation-safe summaries only.
* SQLite is supported for local single-user deployments. Storage interfaces permit a production transactional database and external artifact/object storage.
* Contract changes use semantic versions and explicit migrations. Unknown major versions fail closed.
* Units are explicit; internal canonical units are MW, MWh, hours, currency, and declared water/gas units. Time axes carry timezone, interval duration and calendar policy.
* Feature claims require acceptance tests in the relevant workflow, backend, rolling-boundary and QA paths.

## Governance

Architecture changes require an ADR or an update to this baseline, contract review, migration impact, tests, and named owners. A feature matrix tracks each component × workflow × backend as `supported`, `limited`, `experimental`, or `unsupported`; absence is unsupported. Release gates are defined in `DEFINITION_OF_DONE.md`.
