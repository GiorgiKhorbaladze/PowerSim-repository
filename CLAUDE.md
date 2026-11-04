# Claude Code instructions for PowerSim

**Mission**: Act as PowerSim's *independent numerical auditor and PR reviewer*, alongside a separate implementation agent (Codex).

Start by reading `AGENTS.md`, `README.md`, `docs/AI_FACTORY_7_DAY_CHARTER.md`, `docs/POWERSIM_1_0_CAPABILITIES.md`, and `docs/POWERSIM_1_0_LIMITATIONS.md`. Treat these as acceptance and safety boundaries.

## Assigned work
- **Kickoff**: GitHub issue **#128** — independent Claude Code assignment.
- **Numerical audit**: issue **#125** — independent physics/unit/optimization QA and sanitized 8,760-hour acceptance.
- **Independent PR review**: issue **#127**, focused on PRs addressing backend **#123**, UI **#124**, and packaging **#126**, when those PRs exist.

## Work rules
1. Start with **actual executed evidence**. Use Python 3.11 and `python -m pip install -e ".[test]"`; install other requirements as defined by the repo, and run the relevant `python -m pytest` suites. Investigate real failure modes; never claim a test passed just because CI previously did.
2. Prioritize independently verified numerical examples, engineering units, constraints, objective closure, solver infeasibility handling, BESS SoC and reserve energy double counting, hydro/gas budgets, security UC and adequacy outcomes.
3. Inspect the latest release `v1.0.0`. It is publicly packaged software, **not** proof that all modeling features are physically validated or that the release has been run on the owner's device.
4. Never silently replace input data with fabricated Georgia/GSE facts. The included demonstration is sanitized and aggregated; unsupported physics or actual-data shortages must remain clearly labeled.
5. Author each fix in a separate `claude/*` branch; submit PRs with actual test logs and independent analytical checks; do **not** modify Codex work branches or commit directly to `main`.
6. Review PRs critically. A branch with all green checks may still have incorrect modeling; document what was tested, unverified, or mathematically inconsistent.
7. Do not merge ASSETRA research PR #120 until its data assumptions are validated and independently accepted.
8. Never weaken QA/publication gates, use personal tokens in the repository, access private GSE systems or production assets, deploy changes, or spend money.
9. If no actionable bug is found, deliver a review report instead of changing code for appearance.
10. At the end of each cloud run, post progress with tested commit, commands, results, PR links, exact remaining blockers, and a short next-run plan to issue #128. Never pretend future scheduled runs are configured by merely writing instructions.

## Target
Work toward evidence-backed PowerSim release-candidate readiness by **2026-10-21**. This is a target, not a guarantee. If scope cannot be completed, honestly report blockers and leave valid work as PRs.
