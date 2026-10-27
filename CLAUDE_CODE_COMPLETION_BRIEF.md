# PowerSim 1.0 - Completion Brief for Claude Code

## Role and operating rule

You are taking ownership of completing the PowerSim 1.0 repository. Work directly in this repository, make the implementation changes, run the tests, and leave the application runnable. Do not merely provide a plan or a review.

Treat the present codebase as authoritative. Inspect it before changing it. Preserve its typed contracts, validation behaviour, QA gates, solver semantics and existing public API unless a change is necessary to make the end-to-end application truthful and usable.

## The confirmed defect

The current command:

```bash
powersim serve --workspace ./powersim_workspace
```

starts the Python/FastAPI application successfully, but it mounts the repository's legacy `html/` directory at `/`. The displayed browser title is `PowerSim v4.0 - GSE ER&A Decision Support System`, and it exposes the previous static prototype interface. It is not the intended PowerSim 1.0 application.

In `src/powersim/server.py`, `_default_static_dir()` resolves to the top-level `html/` directory and `create_application()` mounts it using `StaticFiles`. The server therefore presents legacy UI assets even though the Python backend and application services are newer.

This is a release-blocking defect. Do not claim completion while the legacy v4 static interface is still the default product experience.

## Required product outcome

`powersim serve` must launch one coherent PowerSim 1.0 browser application at the root URL. It must be a genuine operational UI, not a static prototype disconnected from the backend.

The UI must support this user flow without manual API calls:

1. Open the application and see the PowerSim 1.0 identity, current project/workspace state and available workflows.
2. Load the sanitized Georgia demonstration project, or create a project from a typed project document.
3. Inspect and edit assets, scenarios and relevant planning inputs through controlled forms or structured editors.
4. Validate and save the project. Surface structured diagnostics clearly, including path, severity and remediation text.
5. Create and launch a run through the application API.
6. See run status transition and the reason if the run cannot start.
7. View QA outcome and results only when the corresponding result is valid and publishable.
8. Compare compatible saved runs.
9. Export the project/run results through the application, with clear limits on what is actually supported.

The existing supported workflows are deterministic UC/ED, stochastic UC, N-1 security UC, chronological adequacy and scoped capacity-expansion screening. Do not imply AC load flow, voltage/reactive studies, EMT, transient stability, protection coordination, commercial market settlement, or any other unsupported capability.

## Backend and run lifecycle requirements

Ensure the browser UI uses the existing same-origin API rather than reimplementing model logic in JavaScript.

- Wire the local solver executor into the application service if it is still missing or disabled.
- Preserve typed conversion from resolved project input to solver input.
- Ensure a run has an explicit lifecycle: created/queued, running, completed, failed/cancelled as appropriate.
- Persist the `ResultEnvelope`, diagnostics, QA status and publishability state in the workspace.
- Surface failure causes in the UI. A blank dashboard, false success state or silently rejected run is unacceptable.
- Do not manufacture numeric results. If a solver, input or optional dependency is unavailable, show an explicit truthful capability/error state.
- Keep backend calculations server-side and prevent unvalidated client-side inputs from bypassing validation or QA gates.

## Reference-data integration: power plant workbook

The repository branch includes normalized reference-data exports under `data/reference/plant_parameters/` generated from the user-provided workbook `Power Plant Parameters English.xlsx`. Use them as the initial editable reference catalogue and expose them in the UI as importable/reference data, not as automatically approved study data.

Files expected in that directory:

- `manifest.json`
- `stations_by_location.json`
- `reservoirs.json`
- `hydropower_assets.json`
- `hpp_summary.json`
- `plant_detail_sheets.json`

Source characteristics:

- 139 station records by location, spanning hydropower, solar/wind and thermal assets.
- 83 reservoir records, including storage/active volume, level limits, dam height and coordinates.
- 125 hydropower-asset rows, with hydraulic and location-related fields where supplied.
- 138 individual power-plant technical sheets, retained as structured rows so no source detail is discarded.
- The workbook source SHA-256 and source-sheet inventory are in `manifest.json`.

The UI and backend must provide a `Reference data` or equivalent workflow that allows users to:

1. Browse and search the station/reservoir catalogue.
2. Select records to stage into a project.
3. Map source fields explicitly to the typed PowerSim project schema.
4. Show a field-level import review before save.
5. Record provenance: source workbook, source sheet/record, import timestamp and any manual override.
6. Keep unrecognised or non-model fields as attached reference metadata rather than silently dropping them.

### Mandatory data-quality safeguards

This workbook is a reference source, not a final model input. The import must therefore produce blocking validation errors or review warnings for:

- missing capacity, unit, fuel, technology or regulation-type fields where the selected PowerSim component requires them;
- duplicate or near-duplicate plant names across source tables;
- inconsistent naming between the station catalogue and hydropower table;
- conflicting installed capacity, unit count or available capacity values;
- implausible or invalid reservoir volume/level ordering;
- unmapped required PowerSim input fields;
- assumptions created by the importer.

Known initial data conditions to handle visibly:

- The station catalogue has two records without installed and available capacity.
- The hydropower table has one row without a plant name.
- At least 20 hydropower names are duplicated in the source, and names do not always match the station catalogue exactly. Examples include `Dariali HPP`, `Ortachala HPP`, `Shuakhevi HPP`, `Zahesi`, and spelling variants such as `Gumat HPPi` / `Gumati HPP-1`.
- Numeric/unit formatting in the source can be inconsistent, including comma decimals and unit strings such as `3x37,6`.

Never silently normalize a conflicting record into a study model. Provide an auditable mapping/override decision.

## UI and design requirements

- Replace the legacy v4 page as the root experience. It may remain only as a clearly labelled archival/compatibility asset outside the default route if retaining it is necessary.
- Use a maintainable frontend structure consistent with the repository's existing dependency and packaging choices. Do not introduce a heavyweight framework solely for appearance.
- Use professional, concise English as default UI language. Georgian labels may be included where current code already supports localization, but do not mix unrelated terminology.
- Make data status prominent: draft, validation failed, valid, run queued/running, QA failed, QA passed, publishable/non-publishable.
- Ensure normal desktop usability: navigation, forms, tables, inline error messages, progress feedback and empty states.
- Do not present non-functional controls. Every prominent button must either work or be hidden/disabled with a specific reason.
- Provide accessible labels, keyboard operation for core flows and semantic status/error messages.

## Tests and release evidence

Before declaring completion, run and report:

1. Existing unit/integration tests, updated for any changed API/UI contracts.
2. Browser E2E test covering the full 15-step user journey above, including a validation failure, a successful project save, a run submission, observable run outcome, QA gate and results/compare visibility.
3. Tests for reference-data import mapping and the quality safeguards listed above.
4. A smoke test of `powersim serve --workspace <temporary workspace>` proving the root page is the new PowerSim 1.0 UI, not the old v4 HTML.
5. Packaging/install smoke test from a clean environment using supported Python 3.10-3.12.

Update CI so these tests are included. Repair stale CI selection if new application, security, adequacy, expansion, run-manager or UI connector test modules are not being collected.

## Documentation and versioning

Update README, capabilities, limitations, user guide and release notes so they describe the implemented product accurately. State clearly that the reference catalogue requires user review before it is used in a study.

Only set a final release version when all acceptance criteria pass. If anything cannot be completed, document the exact blocker, retain failing tests where appropriate, and do not use completion language.

## Acceptance checklist

Do not finish until every item is true:

- [ ] The root URL is a new PowerSim 1.0 application, not the legacy v4 HTML.
- [ ] A user can complete the project-to-result flow in the browser without manual API calls.
- [ ] Backend execution, run persistence, diagnostics and QA visibility are real and truthful.
- [ ] Reference catalogue browsing, staged mapping, provenance and quality checks work.
- [ ] No source conflict is silently accepted as a valid study input.
- [ ] E2E, import, backend and packaging tests pass and CI runs them.
- [ ] Documentation accurately states implemented capabilities and limitations.
