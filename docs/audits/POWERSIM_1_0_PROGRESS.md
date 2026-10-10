# PowerSim 1.0 migration progress

## Current release status

The engineering migration and release operation are complete. PowerSim 1.0.0 was published as `v1.0.0` at `02fbdcd1bc21a8c2e85ce31dc305707b010d1c3e` on 10 October 2026.

Validated evidence on the release-preparation PR:

- Python 3.10, 3.11 and 3.12 core CI passed.
- Clean wheel/sdist install, installed CLI/server and compact queued solve passed.
- Real browser E2E passed: project save, two real deterministic runs, QA/validity display, comparison and visible invalid-self-compare handling.
- Windows portable package and installer lifecycle passed: silent install, installed executable solve, persistence after restart and silent uninstall.
- The sanitized Georgia 2026 baseline completed a fresh 8,760-hour rolling acceptance with QA pass and publishable result.
- The full pytest tree and no-orphan-test gate passed.

The repository is versioned as PowerSim `1.0.0`. The tag-triggered release workflow completed successfully and the public GitHub Release **PowerSim 1.0** is available at https://github.com/GiorgiKhorbaladze/PowerSim-repository/releases/tag/v1.0.0. Published tag-derived artifacts are the Python wheel, source distribution and Windows x64 portable ZIP.

## Completed architecture

| Area | Status |
| --- | --- |
| Shared physical core | Thermal UC/economics, wind, solar, run-of-river, reservoir hydro/cascades, BESS, pumped hydro, demand response, exchange and gas accounting have canonical extraction and independent QA within the documented scope. |
| Deterministic workflow | Validated 60-minute, 15-minute and supported rolling UC/ED, reserves, DC network, objective reconstruction and publication gating. Rolling runs report a committed canonical objective, not a fabricated global Pyomo objective. |
| Advanced workflows | Shared stochastic UC, genuine N-1 security UC, chronological probabilistic adequacy and scoped expansion screening are routed through the application. |
| Platform | Typed projects/scenarios/runs, immutable resolved inputs, provenance, persistent run history, safe identifiers, async local execution and same-origin API/UI are complete. |
| Application | `powersim serve` starts the local backend and static UI. Browser E2E exercises the real solve-to-publication path. |
| Packaging | Wheel/sdist and Windows portable/installer build paths are tested. The final tag workflow builds tag-derived distributions and Windows assets. |

## Current capability boundary

Read the final [capabilities](../POWERSIM_1_0_CAPABILITIES.md) and [limitations](../POWERSIM_1_0_LIMITATIONS.md) documents before study use.

Important exclusions remain fail-closed or compatibility-only:

- DC scope is not AC voltage/reactive-power analysis and rejects non-locational scarcity studies.
- Rolling cascades with nonzero travel delay are unsupported.
- Reservoir `end_level_penalty`, committable reservoir UC and optional storage-depth extensions remain legacy compatibility-only.
- The BESS/pumped-hydro depth selector has a documented one-sided implication.
- Pumped-hydro and demand-response reserve provision, and reserve activation-time/ramp deliverability, are not validated.
- Islanded N-1, stochastic rolling, detailed adequacy network/hydrology/correlated-outage treatment and full expansion optimization are not claimed.

## Migration inventory

Merged implementation PRs: #69, #71-#91, #94-#96, #102-#115 and #117.

Key final release engineering PRs:

- #106: full pytest/no-orphan gate and browser E2E infrastructure.
- #107: clean artifact installation.
- #108-#109: Windows packaging and real packaged solve.
- #110-#111: visible run history, comparison and persistent state.
- #112: sanitized Georgia application baseline.
- #113: publishable release smoke.
- #114: installed Windows lifecycle acceptance.
- #115: annual rolling completion and truthful rolling objective scope.
- #116: final capability, limitation, guide, release-note and audit documents.
- #117: 1.0.0 versioning, final installer metadata and tag-release artifact workflow.

## Release completion

No repository-side release action remains. Future work should be tracked as a new versioned scope and must preserve the capability and limitation boundaries above.
