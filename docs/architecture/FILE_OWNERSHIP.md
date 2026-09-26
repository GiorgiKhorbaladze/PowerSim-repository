# File ownership and parallel-agent boundaries

Ownership means one merge authority at a time, not exclusive knowledge. Changes crossing boundaries require interface-first review and must not bundle opportunistic edits.

| Workstream owner | Exclusive target during migration | May proceed in parallel after contracts freeze |
|---|---|---|
| Contracts/architecture | `src/powersim/contracts/`, schema migrations, capability registry, architecture docs | all teams consume tagged contracts |
| Component core | component protocol, build context, registries and shared coupling interfaces | component implementations on separate files |
| Thermal/VRE | `components/thermal.py`, `wind.py`, `solar.py`, `ror.py` | hydro/storage/network teams |
| Hydro/storage/DR | reservoir, cascade, BESS, pumped hydro, DR components | thermal/network teams |
| Network/reserves/exchange | network, reserve engine and exchange components | other physical components after port contract |
| Deterministic workflow | deterministic assembler and rolling state coordinator | backend adapters and QA using frozen interfaces |
| Stochastic/security | their workflow modules and acceptance fixtures | each other only after deterministic parity baseline |
| Adequacy/expansion | their workflow modules and definitions | stochastic/security |
| Solver backends | solver protocol plus `highs.py`/`gurobi.py` | workflow work against fakes/protocol |
| QA/results | invariant engine, canonical extraction, reports and storage adapters | API/UI after result schema freeze |
| Run/API | projects/runs/application services, API and DB migrations | UI via OpenAPI mock |
| UI/E2E | application client and browser tests | backend via frozen OpenAPI |
| Packaging/CI/docs | build metadata, CI orchestration, operator/user docs | all, but central workflow edited by this owner |

## Central files: single owner only

At any instant one named integrator owns contract version/index, component registry/build context, workflow capability matrix, result envelope, database migration head, package exports/dependency lock, main CI workflow and legacy `solver/powersim_solver.py`. Other agents add leaf files or send patches to that owner. Avoid multiple agents editing monolith/schema/CI simultaneously.

## Integration rules

1. Freeze protocols and golden JSON before parallel implementations.
2. Each migration PR is behavior-preserving or a separately reviewed behavior change, never both implicitly.
3. Legacy adapters are thin and have deletion criteria/tests.
4. Every workstream owns unit tests and fixture namespaces; shared golden fixtures have one custodian.
5. Rebase/merge in dependency order: contracts → core protocols → leaf components/backends → workflows → QA/results → services → UI → release.
6. Architecture owner arbitrates cross-layer dependency violations; modelling owner arbitrates equations; release owner controls claim promotion.
