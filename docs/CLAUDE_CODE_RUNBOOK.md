# Claude Code launch instructions for PowerSim AI Factory

## Cloud kickoff - owner action required once
1. Open **https://claude.ai/code** in the existing paid Claude account (web or the Claude mobile app's Code tab).
2. Connect GitHub only if Claude asks for it. Grant access to **GiorgiKhorbaladze/PowerSim-repository**; do not grant access to private GSE data or unrelated organizations.
3. Select repository `GiorgiKhorbaladze/PowerSim-repository`, branch `main`.
4. Choose cloud editing mode that allows edits and PR creation. Paste the kickoff prompt below and send it. A GitHub issue by itself does **not** trigger a Claude cloud session.
5. When Claude has started, leave the page: a remote task can continue on its own until it finishes, errors, or exhausts usage quota. A single task does not remain alive for a week after completion.

## First cloud task - exact copy/paste
You are Claude Code, the independent numerical auditor and PR reviewer of PowerSim. Work in the connected GitHub repository GiorgiKhorbaladze/PowerSim-repository, starting from main. Read CLAUDE.md, AGENTS.md and docs/AI_FACTORY_7_DAY_CHARTER.md. Execute GitHub issue #128 and numerical QA issue #125. Independently verify current Python/Pyomo model mathematics, engineering units, BESS SoC and reserve energy double counting, hydro/gas resource constraints, N-1 rejection paths, adequacy metrics and the existing sanitized 8,760-hour acceptance. Run real Python 3.11 tests and record commands/results; do not invent data or loosen QA to pass. You are working alongside Codex: review any open backend/UI PRs for issues #123 and #124, but do not duplicate edits or merge other branches. Fix only reproducible problems on a new claude/* branch, with tests and a PR. Update issue #128 with a verified report even if nothing needs fixing. Do not write to main, publish new releases, merge draft research PR #120, or touch private GSE data. Target an evidence-backed RC by October 21, 2026; report any unresolved or NOT VERIFIED criteria clearly. Begin execution now, without requesting routine implementation decisions.

## Recurring cloud routine - second owner action if available
Visit **https://claude.ai/code/routines** and create one new **remote/cloud** routine for the same repository. Use a daily schedule through Oct 21, if the UI supports an end date; if it does not, deactivate after Oct 21. A new routine is not created by these instructions or by a normal cloud session.

**Routine name:** PowerSim - Claude numerical audit & PR review

**Routine prompt:**
Review issues #128, #125, #127, new implementation PRs and CI in GiorgiKhorbaladze/PowerSim-repository. Read CLAUDE.md, AGENTS.md and docs/AI_FACTORY_7_DAY_CHARTER.md. Do not restart duplicate work already being done. Review outstanding changes independently for numerical validity, limits, solver truthfulness and security. Where reproducible defects exist, make a focused tested claude/* PR; otherwise only comment with evidence-based audit results. Never merge PR #120 without validated data, weaken QA, change production credentials, access private GSE data, or publish a release. Record a concise progress note and next steps on issue #128. Stop initiating further implementation after 2026-10-21. Treat failed runs and usage caps as blockers.

## What counts as launched?
- GitHub **#128 exists**: assignment prepared, but not execution.
- `CLAUDE.md` committed on `main`: instructions available to cloud agent, but not execution.
- A cloud session is listed as running or completed under Claude Code: **first actual task was launched**.
- A routine is visible in Claude Code Routines with its next run time: **recurring independent work has been configured**.
- A PR with actual test evidence: **tangible implementation/audit output**, not automatic proof of complete numerical validity.

**Avoid** posting account passwords, OAuth tokens, project secrets, and proprietary GSE hourly input data into prompts or this public repository.
