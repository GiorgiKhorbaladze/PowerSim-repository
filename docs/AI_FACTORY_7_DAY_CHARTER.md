# PowerSim AI Factory - 7-day autonomy charter
Version: 2026-10-14. **Target checkpoint:** 2026-10-21 (Asia/Tbilisi). Target is not a guaranteed completion date.

## Mission and truthful starting point
Build a user-operable, verifiably tested PowerSim based on the existing public repository and published v1.0.0. Focus on usability, actual model execution, and reproducible software delivery, not a rewrite. On 2026-10-14 the repository has v1.0.0 release assets (Python wheel, source tarball and Windows ZIP), and relevant CI workflows. The public release listing did **not** show a standalone `PowerSim-1.0.0-Windows-x64-Setup.exe` asset, despite README instructions to download one: verify whether that installer exists in the ZIP or as workflow artifact before claiming it is delivered. The mere presence of packaged artifacts is not evidence of successful operation on the owner's machine.

## Scope, priorities and agent ownership
Keep tasks independent, use separate branches, and hand work off through PRs. A coordinator owns task dependencies and daily triage; engineers do not modify each other's branch without coordination.

| Agent | Responsibility | Mandatory evidence |
|---|---|---|
| Coordinator / Release manager | Prioritize work, avoid duplicates, review PR/CI, coordinate merges, compile status | Backlog with statuses, risk ledger, final manifest |
| Backend engineer | Trace ProjectContract -> resolved input -> ApplicationService -> queued executor -> solver -> ResultEnvelope | Real solver smoke, negative tests, persisted result, no fabricated data |
| UI engineer | Browser journey create/validate/save/project/scenario/run/progress/results/compare/export | Playwright trace, screenshots, no console errors, files downloaded |
| Numerical auditor | Test formulations/units/physics and QA gate using independent benchmarks | Numerical test fixtures, discrepancy list, reproducibility evidence |
| CI/Packaging engineer | Fix CI, Windows setup, wheel/sdist, clean install/start/restart/uninstall | Windows runner logs and downloadable artifacts |
| Independent reviewer/security agent | Review each PR independent of author, check dependencies/security and claims | Written actionable review with block/accept decision |

## Day-by-day execution sequence
**Day 1 (Oct 14-15)** - Baseline audit. Do not assume 'green' means works. Reproduce Linux/Python installation, browser flow and release download; log all blockers, issue tickets and baseline evidence. Confirm existing PR #119 status and segregate draft research PR #120.

**Day 2 (Oct 15-16)** - Stabilize backend end-to-end, raise errors precisely, exercise validated deterministic solve and persisted result, add regression tests.

**Day 3 (Oct 16-17)** - Complete browser end-to-end user workflow, failure handling, offline/local assets, comparison/export, Georgian labels and guidance.

**Day 4 (Oct 17-18)** - Independent numerical and adequacy QA: seeded cases and 8,760-hour acceptance with documented sanitized assumptions; no unverifiable 2027 capacity claims.

**Day 5 (Oct 18-19)** - Windows installable executable and Linux/Python package validation, clean machine install/restart/export/uninstall.

**Day 6 (Oct 19-20)** - Regression, release candidate assembly, independent review, security audit, docs and reproducibility inventory.

**Day 7 (Oct 20-21)** - Final end-to-end acceptance, final evidence bundle, RC binaries, changelog, exact known limitations, and pass/fail matrix. Publish a new release only when all required tests and publication conditions pass.

## Release acceptance gates (all required)
- [ ] From a clean environment, a non-expert can install and launch the application without manual Python intervention on Windows.
- [ ] UI opens and talks to the packaged backend, without an external CDN dependency blocking core tasks.
- [ ] Create / validate / save / reopen project and scenarios genuinely works.
- [ ] Launch a real queued solver run, poll statuses, persist result, restart and retrieve history.
- [ ] A validated deterministic UC/ED scenario returns actual solve outputs; invalid solver status must not be shown as success.
- [ ] BESS SoC, balance, reserves, import limits, hydro budgets and gas consumption are covered by independent unit/numerical tests within declared capabilities.
- [ ] N-1, stochastic UC, adequacy and scoped expansion smoke tests run within declared documented limits. Unsupported study types must clearly reject.
- [ ] Results, comparison, Excel/CSV and PDF/PNG exports actually download and contain data when those UI controls are present.
- [ ] Automated browser acceptance traverses the full flow; browser console has no unexpected errors.
- [ ] Full 8,760-hour sanitized annual acceptance is reproducible; canonical QA passes and output validity/publishability is explicitly evidenced.
- [ ] CI checks pass on the exact release candidate commit; installer and Python artifacts are attached and actually downloadable.
- [ ] Release report identifies tested OS/build, solver versions, exact commands, limitations, known issues and any NOT VERIFIED items.

## Work loop
1. Coordinator checks the backlog and CI, chooses highest impact unblocked ticket.
2. Specialist implements only that ticket in a PR branch with evidence.
3. Independent reviewer audits the diff and tests, including negative scenarios.
4. Coordinator merges only after required CI and review gates pass. Otherwise return concrete fixes.
5. Every 4 hours an automation can audit open blockers and unfinished tickets; do not relaunch duplicate agents on identical tickets.
6. Send a daily digest: completed PRs, tests and numerical results, unresolved issues, actual budget usage, next tasks.
7. Terminate after deadline or spend cap; report partial outcome transparently.

## Autonomy boundaries and initial one-time setup
The user must first connect a coding cloud service to this GitHub repository, choose a paid plan if required, and set a spending cap. Recommended execution plane: Cursor Cloud Agents and Cursor Automations; independent review: Claude Code GitHub Actions or CodeRabbit; tests/release: existing GitHub Actions. These external integrations are **not installed merely by committing this file**.
- Grant the cloud agent access **only** to this repo; use principle of least privilege and network restrictions.
- Define `POWERSIM_AI_FACTORY_ENABLED` and spending ceilings in the provider dashboard as appropriate.
- No self-approval of privileged access, billing changes, destructive operations, silent publishing or removing audit history.
- No sensitive GSE models, emails, documents or operational network credentials in the public repository or third-party coding environment.
- If credentials/billing/account app authorization are absent, mark setup BLOCKED and stop; never pretend automation is running.

## Coordinator prompt (paste into Cursor Cloud Agents)
You are the PowerSim AI Factory **coordinator**. Repository: https://github.com/GiorgiKhorbaladze/PowerSim-repository. Read AGENTS.md and docs/AI_FACTORY_7_DAY_CHARTER.md in full. Autonomously assess the current main branch and v1.0.0 release, then create a prioritized issue-level plan. Delegate independent backend, UI, numerical QA, Windows packaging and review tasks into distinct agent branches; open PRs with tests, and integrate only when CI and independent checks pass. You may work unattended; do not send clarification requests for nonblocking implementation details. Never fake test execution, alter numeric safeguards to pass CI, merge the draft ASSETRA research PR without validation, upload any private data, exceed the defined budget, or release if acceptance gates fail. Target your final acceptance by 2026-10-21, and leave a verifiable release candidate and blocker report if complete acceptance is not possible. Start with an evidence-based audit, not a rewrite.
