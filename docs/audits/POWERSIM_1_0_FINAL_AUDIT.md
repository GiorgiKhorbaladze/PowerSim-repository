# PowerSim 1.0 final audit

## Audit status

Release-candidate engineering evidence is complete at `8a1d8fb1d7e6ab3aeb8eddb2463ce30a8c916f39`, which versions the application as PowerSim 1.0.0.

The release tag and GitHub Release have not yet been created. This audit must not be read as evidence that a tag-derived release artifact exists until the tag workflow completes.

## Verified release-candidate evidence

| Gate | Evidence |
| --- | --- |
| Python matrix | Python 3.10, 3.11 and 3.12 core CI passed on PR #117. |
| Test coverage | Full pytest tree and no-orphan pytest gate passed. |
| Browser application | Real browser E2E passed: project save, two queued deterministic runs, QA/validity display, comparison and an explicit same-run comparison failure. |
| Installed package | Wheel/sdist build and clean virtual-environment install passed, including installed CLI, server and compact real solve. |
| Windows | Portable application and silent installer lifecycle passed: install, real queued deterministic solve, restart with persisted history and uninstall. |
| Annual acceptance | A fresh sanitized Georgia 2026 8,760-hour rolling application acceptance passed with QA and publication gating. |
| Resolution/workflows | Compact 15-minute and rolling deterministic, stochastic, N-1 security, chronological adequacy and scoped expansion tests are in the validated CI tree. |
| Release automation | `.github/workflows/release.yml` builds wheel, sdist, Windows zip and installer from a `v*` tag and publishes those tag-derived assets. |

## Architecture ownership

The browser UI and API manage typed projects, scenarios, immutable resolved run snapshots, lifecycle and result presentation only. Python/Pyomo workflows own electrical equations. A bounded local executor routes deterministic UC/ED, stochastic UC, N-1 security UC, chronological adequacy and scoped expansion without duplicating solver physics.

Result envelopes are stored only after run-id/fingerprint checks, canonical QA and publication gating. Invalid, corrupted, unsupported or no-incumbent results fail closed.

## Data scope

The committed Georgia baseline is sanitized and aggregated. Its 131-asset demonstration registry is not private GSE operational data; the runnable fleet is explicitly documented as an aggregate/subset representation.

## Legacy inventory

Keep as compatibility-only or explicitly unsupported: committable reservoir UC, reservoir `end_level_penalty`, optional BESS depth/end-target behavior, the one-sided BESS/pumped-hydro depth selector, rolling delayed cascades, unvalidated pumped-hydro/DR reserves and unvalidated reserve activation-time deliverability. No AC, voltage/reactive-power, EMT, protection or market-settlement capability is claimed.

## Remaining release operation

Create annotated tag `v1.0.0` at `8a1d8fb1d7e6ab3aeb8eddb2463ce30a8c916f39`, verify the tag workflow succeeds, and publish **PowerSim 1.0** with the tag-built artifacts. At that point update this audit with the tag workflow run and GitHub Release URL.
