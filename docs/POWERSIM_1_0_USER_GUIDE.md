# PowerSim 1.0 user guide

## Install and launch

For developers, install with `pip install powersim` and start with `powersim serve --workspace ./powersim_workspace`.

Open the local URL printed by the server. The Windows installer launches the same local application and opens the browser automatically.

## Study flow

1. Load or create a typed project.
2. Select a scenario and workflow.
3. Validate and save the project.
4. Create and launch a run.
5. Monitor queued, preparing, solving, result-validation and completed/failed states.
6. Review QA, validity and publication status before using results.
7. Use Compare Runs for compatible workflow summaries.

Bundled samples/projects/georgia_2026_baseline.json is a sanitized, aggregated planning demonstration with editable Georgia 2027-2030 demand scenarios.

## Workflows

- Deterministic UC/ED: operational dispatch, UC, reserves and validated DC studies.
- Stochastic UC: explicit scenario probabilities and shared extensive-form physics.
- N-1 Security UC: validated non-islanding contingencies.
- Chronological adequacy: LOLE, LOLP and EENS in the dedicated adequacy scope.
- Capacity expansion screening: scoped investment screening only.

A failed QA, invalid result or non-publishable envelope must not be used as a study result. See the limitations document before selecting features outside the validated core.
