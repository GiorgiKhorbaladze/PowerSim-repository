# PowerSim engineering agent instructions

You are working on **PowerSim**, a local Python/Pyomo electricity-system planning and optimization application. The browser UI communicates with the production backend through a same-origin API. Study correctness, reliable workflows, and honest capability boundaries are more important than apparent feature completeness.

## Source of truth
1. Read `README.md`, `pyproject.toml`, `docs/POWERSIM_1_0_CAPABILITIES.md`, `docs/POWERSIM_1_0_LIMITATIONS.md`, and `docs/AI_FACTORY_7_DAY_CHARTER.md` before edits.
2. The published `v1.0.0` release is the baseline, not proof that every user's machine or case is working.
3. Preserve existing typed contracts, immutable run snapshots, canonical QA, publication gates, run persistence and solver reproducibility.
4. Respect the published boundary: AC power flow, voltage/reactive studies, EMT/transient stability, protection and commercial settlement are NOT supported. Do not claim otherwise.
5. The Georgia demonstration is sanitized/aggregated, not official GSE operational data. Never invent validated hourly plant data, reservoir limits, forced-outage rates, or transmission assumptions.
6. Keep draft research PR #120 (ASSETRA) separate from the release path unless its data and methods are independently validated. Do not auto-merge it.

## Mandatory engineering protocol
- Start by reproducing a problem and recording the actual result; do not declare a feature broken or fixed without evidence.
- Make small, atomic changes on a dedicated branch. Avoid simultaneous edits to shared integration files without coordination.
- Add regression tests for each repaired defect. Run the relevant unit/integration tests and real backend solves. Keep failure/invalidity paths fail-closed.
- A green test is evidence for the test's stated scope only, not unconditional numerical correctness.
- For math/optimization changes, document formulations, units, assumptions, tolerances, solver status and counterexamples. Use independent small analytical benchmarks when possible.
- Never suppress, bypass or downgrade CI, QA, safety checks, validation, or test failures just to get a green build.
- For browser changes, exercise a real user journey, check console errors, downloadable results and persisted runs. Provide logs/screenshots when tooling allows.
- For packaging, test fresh Windows installation, actual solve, restart/persistence, and uninstall. A ZIP is not necessarily an installer.
- Require a full 8,760-hour annual workflow acceptance under the currently supported assumptions. Capture objective, run status, QA, validity, fingerprints and output artifacts.
- No production infrastructure changes, deployment to external networks, release publication, or confidential-data upload without explicit authorization and passing release gates.
- Do not push directly to `main`. Open a PR, attach evidence, and ask the integration agent to verify CI and independent review.
- Never include credentials in commits, logs, prompts or issue comments. Restrict external access to this public repo's contents.

## Definition of done for each issue
1. The acceptance criteria written in the issue are met, with tests.
2. Code and documentation agree and remain honest about limitations.
3. Relevant CI is green; QA and validity/publishability gates are not weakened.
4. A PR has a clear summary, tests executed, evidence and residual risks.
5. Integration is blocked whenever acceptance evidence is incomplete.

## Operational notes
- Python version range is `>=3.10,<3.13`; prefer Python 3.11 for CI/reproduction.
- Known entry point: `powersim serve --workspace ./powersim_workspace`.
- Primary development and quality-gate source: GitHub Actions workflows under `.github/workflows/`.
- Do not publish a policy-grade Georgia adequacy/market result from illustrative data.
- Do not interpret unserved energy = 0 as sufficient for publishability when canonical QA rejects the result.
