# PowerSim 1.0 release notes

## Unreleased - PowerSim 1.0 application completion

Not yet released or versioned. Merging is subject to CI and independent review.

- `powersim serve` now serves the PowerSim 1.0 browser application at `/` (packaged in `powersim/ui/static`). The legacy v4 static prototype under `html/` is archived and no longer served or packaged.
- New application API endpoints: system/capability status, samples, project get/validate/validated save/export, validation-gated run creation, run cancel/summary/export, gated compare, and reference catalogue browse/quality/review/import.
- Results are shown and exported only for valid, QA-passing, publishable runs; failure causes are persisted and shown.
- Disabled assets are now excluded from solver input. Before this change the solver ignored `enabled = false`.
- Explicit-period profiles are now solved at their native values. Before this change a profile shorter than a year (for example a 4-period demand `[20, 22, 25, 21]`) was stretched over the whole year by the solver's resampling and solved as `[20, 20, 20, 20]`. Profile lengths other than `time.periods`, 8,760 hourly values or one full year at the study resolution are now a blocking validation error.
- Plant-parameter reference catalogue rebuilt section-aware from the workbook, with quality checks and an auditable field-level import (see `docs/REFERENCE_DATA.md`).
- Tests: browser E2E of the full user journey, reference catalogue/import tests, API tests, a `powersim serve` smoke test, and clean installs on Python 3.10-3.12 in CI.
- Fixed: variable cost is counted exactly once. `_dispMC` fell back to `vom` for assets without a positive heat rate × fuel price, and the objectives then added `vom` again. Imports and zero-heat-rate units therefore paid VOM twice, reported an implied fuel cost and could lose merit order to costlier units. `_dispMC` is now the fuel/energy marginal cost excluding VOM (heat rate × fuel price, else an explicit `mc`, else 0). The UC, stochastic and shared-component objectives, the ED re-solve, the price fallback and the reported SRMC all use `_dispMC + vom`. The ED re-solve and the reported thermal SRMC previously omitted VOM. Regression: `tests/test_variable_cost_single_count.py` (hand-checked 1-hour fixture, objective 3,943.9 USD). Results of earlier runs with imports or zero-heat-rate units are not comparable with new runs.

## Major changes

- Shared deterministic components and independent QA/publication gate.
- Shared stochastic UC, genuine N-1 security UC, chronological adequacy and scoped expansion screening.
- Typed Project/Scenario/Run platform with immutable snapshots and provenance.
- Same-origin local application with background execution, persisted run history and browser E2E.
- Reproducible wheel/sdist installation and Windows portable/installer packaging.
- Sanitized Georgia 2026 planning template with 2027-2030 demand scenarios.
- Final annual acceptance requires all 8,760 periods to be committed and a publishable canonical result.

## Data scope

No private GSE operational source data are distributed. The public Georgia demonstration is sanitized and aggregated.

## Upgrade notes

Legacy JSON and solver pathways remain compatibility interfaces where documented. New application runs use typed workflow selection and immutable resolved inputs.
