# PowerSim 1.0 release notes

## Unreleased - PowerSim 1.0 application completion

Not yet released or versioned. Merging is subject to CI and independent review.

- `powersim serve` now serves the PowerSim 1.0 browser application at `/` (packaged in `powersim/ui/static`). The legacy v4 static prototype under `html/` is archived and no longer served or packaged.
- New application API endpoints: system/capability status, samples, project get/validate/validated save/export, validation-gated run creation, run cancel/summary/export, gated compare, and reference catalogue browse/quality/review/import.
- Results are shown and exported only for valid, QA-passing, publishable runs; failure causes are persisted and shown.
- Disabled assets are now excluded from solver input. Before this change the solver ignored `enabled = false`.
- Plant-parameter reference catalogue rebuilt section-aware from the workbook, with quality checks and an auditable field-level import (see `docs/REFERENCE_DATA.md`).
- Tests: browser E2E of the full user journey, reference catalogue/import tests, API tests, a `powersim serve` smoke test, and clean installs on Python 3.10-3.12 in CI.
- Known issue: see the import/no-heat-rate cost double count in the limitations document.

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
