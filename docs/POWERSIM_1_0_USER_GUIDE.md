# PowerSim 1.0 user guide

## Install and launch

From a source checkout, install with `python -m pip install .` and start with:

```bash
powersim serve --workspace ./powersim_workspace
```

To install a release artifact, use `python -m pip install dist/powersim-1.0.0-py3-none-any.whl`. Supported Python versions are 3.10-3.12.

Open the local URL printed by the server (by default `http://127.0.0.1:8000/`). The root URL is the PowerSim 1.0 browser application; it talks only to the same-origin API under `/api`. The Windows installer launches the same local application and opens the browser automatically.

The header always shows the **project status** (no project / draft with unsaved changes / validation failed / saved - not run-ready / saved - valid, run-ready), the **backend status** (executor and HiGHS solver availability) and the **workspace** (number of projects and runs).

## Study flow in the browser

1. **Overview** - open a saved project, load a sample, or create a project from a typed project document (paste JSON or choose a file; *Insert minimal template* gives a starting point). *Validate document* shows server diagnostics before anything is saved.
2. **Project** - edit the name, workflow, currency and typed solver settings (time limit, MIP gap). The time axis is shown read-only and is edited in the *Project document (advanced JSON editor)*, which can edit any part of the typed project. Applying the document only changes the working copy.
3. **Assets** - inspect all assets; add, edit, enable/disable or remove them. Disabled assets stay in the project but are excluded from runs. Reference-catalogue assets show their source sheet/row, field origins, overrides and attached reference metadata, and must be marked reviewed (with a review note) before they can be enabled in a run.
4. **Scenarios** - rename, add (id, name, JSON overlay) or delete scenarios. Overlays merge mappings into the base project and replace lists/values; they cannot change project identity.
5. **Validate** and **Save project** (sticky bar). Every finding is shown with severity, path, code, message and remediation. Contract errors block saving. Run-readiness errors (for example an enabled but unreviewed reference asset) allow saving but block run creation.
6. **Runs** - choose the base case or a scenario and *Create and launch run*. The button is disabled with the reason when the project has unsaved changes, is not run-ready, or no solver/executor is available. If the server refuses the run, the structured reason is shown. The runs table and the lifecycle timeline update while the run moves through draft, validating, queued, preparing, solving, validating results, reporting and completed/failed. Draft and queued runs can be cancelled.
7. **Results** - shows run status, QA status, publication state, failure cause (for failed runs), QA checks and solver diagnostics. Numeric results (KPIs, chart, by-resource and monthly tables, or workflow metrics) are shown **only** when the result is valid and passes QA and the publication gate; otherwise the page states that results are withheld and why.
8. **Compare** - compare two completed runs. Metrics are shown only when both runs are publishable and use the same workflow.
9. **Export** - *Export project JSON* downloads the saved project document. *Export run bundle (ZIP)* downloads the immutable resolved input, manifest, run events, hash verification and README; it includes `result.json` and CSV summaries only for publishable results, and diagnostics without numeric results otherwise. Exports are planning-model outputs only; there is no report, settlement or regulatory filing export.

## Samples

- **Georgia 2026 baseline** (`samples/projects/georgia_2026_baseline.json`) - a sanitized, aggregated 8,760-hour deterministic UC/ED demonstration with editable Georgia 2027-2030 demand scenarios. It is not private GSE operational data and not an official dispatch case.
- **Compact 4-hour demonstration** - a two-resource case for a quick end-to-end check.

## Workflows

- Deterministic UC/ED: operational dispatch, UC, reserves and validated DC studies.
- Stochastic UC: explicit scenario probabilities and shared extensive-form physics.
- N-1 Security UC: validated non-islanding contingencies.
- Chronological adequacy: LOLE, LOLP and EENS in the dedicated adequacy scope.
- Capacity expansion screening: scoped investment screening only.

The non-deterministic workflows need workflow-specific inputs. When one of them is selected, the **Project** page shows a *Workflow inputs* form:
- **Stochastic UC:** scenarios with probabilities (they must sum to 1) and an optional demand profile per scenario; requires the shared component engine.
- **N-1 security UC:** contingencies (unit, import or line outage), reserve products (direction, requirement, eligible units) and the shared component engine.
- **Chronological adequacy:** Monte Carlo samples, random seed and the forced outage rate of each asset.
- **Capacity-expansion screening:** planning years, discount rate, reserve margin and candidates (CAPEX, fixed O&M, maximum build, life, capacity credit, capacity factor).

*Validate* runs each workflow's own solver validator, so missing or invalid inputs appear under Validation and block run creation instead of failing during the solve.

## Language

The header's **ქართული / English** button switches the interface between English and Georgian. The choice is remembered in the browser. Data values (asset ids, scenario names, codes) are never translated. Terminology follows `docs/LOCALIZATION_TERMINOLOGY.md`.

## Charts, reports and sensitivity

- **Results** shows two charts for dispatch studies: dispatch by technology (hourly, or daily mean for long horizons) and the monthly energy balance (stacked by technology, with unserved energy in red and a demand marker). The monthly values are also available as a table.
- **Excel report** downloads a workbook with Summary (run, provenance, key results, limits), Monthly balance, By resource, Hourly system, Hourly by resource and QA checks.
- **Printable report** opens a static page; print it to save it as PDF.
- Reports are only available for publishable runs.
- **Sensitivity analysis** (Runs page): choose an input (a profile such as demand, or an asset's capacity, VOM, fuel price, heat rate or monthly energy), a starting scenario and factors (for example `0.9, 1.1`). PowerSim saves one scenario per factor and launches the runs. Each scenario uses a compact overlay `{"scale": {"profiles": {"demand": 1.1}}}` or `{"scale": {"assets": {"<id>": {"vom": 1.2}}}}`, which you can also write by hand. Unknown ids or fields and negative factors are validation errors.
- **Compare → Scenario overview** lists the latest completed run of every scenario with served/unserved energy, curtailment, objective, average price and peak, plus one table per sensitivity group.

## Calibration against measured data

For a publishable run, open **Results** → *Calibration against measured data* → *Download calibration template*.

The workbook has an **Hourly** sheet (one row per study period) and a **Monthly** sheet (Jan–Dec). Columns are:
- `demand`;
- `kind:<kind>`, the sum of all assets of a kind (for example `kind:hydro_ror`, `kind:wind`);
- individual asset ids.

Enter **measured** energy in MWh in either sheet or both; leave columns without data empty. Upload the file with *Compare with measured data*. For every filled column PowerSim reports model and measured totals, bias % (positive = model above measured), MAE, RMSE and, for hourly data, the correlation, plus a month-by-month table.

Each comparison is stored in the run folder (`calibration-<timestamp>.json`, including the file's SHA-256). Calibration describes the fit only: it never changes the model or its inputs.

## Bulk parameters with Excel

On the **Assets** page, *Download Excel template* produces a workbook generated from the saved project:
- **Assets** sheet: one row per asset, with kind, enabled, capacities, fuel type, heat rate, fuel price, VOM/import price, availability and inflow profile ids, reviewed and review note, and a read-only source column;
- **Profiles** sheet: one column per profile, where the header is the profile id;
- **Instructions** sheet.

Fill it in with approved data and upload it with *Check workbook*.
- An empty cell leaves the value unchanged; nothing is cleared implicitly.
- Only existing assets can be edited.
- Numbers must be finite and non-negative, and a comma decimal is accepted.
- Kinds and TRUE/FALSE values are checked, and profile references must resolve.
- Profiles need `time.periods` or 8,760 values.
- Reference assets can be marked reviewed only with a review note.

Every error names its cell, for example `Assets!J5`. The change list (asset, field, current, new) is shown before anything is saved. *Apply changes to project* saves a new revision. The project metadata records the file name, SHA-256, user and every change under `parameter_imports`, and reference assets record the workbook as the origin of each changed field.

## Reference data (plant-parameter catalogue)

The **Reference data** page exposes the normalized catalogue built from `Power Plant Parameters English.xlsx` (see [reference data](REFERENCE_DATA.md)). **The catalogue is reference data and requires user review before it is used in a study.**

1. Browse and search the Stations, Reservoirs, Hydropower and HPP-summary tables; open *Details* for a record's quality findings, similarly named records in other tables and the plant technical sheet.
2. Open the *Quality report* tab for all catalogue findings (missing capacities, duplicates, naming variants, conflicting capacities/unit counts, reservoir ordering, number formats).
3. Tick station plant rows to stage them and choose *Review import mapping*. The review lists every PowerSim field with its value, status (mapped, derived, assumed, override, conflict, missing, unmapped required), origin and source cell, plus every issue with remediation.
4. Resolve blocking issues with manual overrides; every override needs a reason and is recorded in provenance. *Review again with overrides* re-runs the server-side review.
5. Acknowledge the warnings and importer assumptions, optionally enter your name, and *Import into project*. The open project must be saved. A new project revision is saved with the imported assets **disabled** and marked *requires review*; unmapped source columns are attached as reference metadata that the solver never reads.
6. Before enabling an imported asset, check its values against approved sources, add the solver inputs the workbook does not contain (cost basis for thermal, availability profile for run-of-river/solar/wind, inflow and reservoir model for reservoir hydro) and mark it reviewed. Validation blocks runs until this is done.

A failed QA, invalid result or non-publishable envelope must not be used as a study result. Unserved energy of zero does not make a result publishable when canonical QA rejects it. See the limitations document before selecting features outside the validated core.
