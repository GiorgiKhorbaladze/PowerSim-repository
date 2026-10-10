# PowerSim 1.0 final audit

## Audit status

PowerSim 1.0.0 was released as tag `v1.0.0` at `02fbdcd1bc21a8c2e85ce31dc305707b010d1c3e` on 10 October 2026. The tag-triggered release workflow completed successfully and the public GitHub Release **PowerSim 1.0** was published with tag-derived artifacts.

Release: https://github.com/GiorgiKhorbaladze/PowerSim-repository/releases/tag/v1.0.0

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
| Release automation | `.github/workflows/release.yml` built and published tag-derived wheel, sdist and Windows portable ZIP assets from `v1.0.0`. |

## Architecture ownership

The browser UI and API manage typed projects, scenarios, immutable resolved run snapshots, lifecycle and result presentation only. Python/Pyomo workflows own electrical equations. A bounded local executor routes deterministic UC/ED, stochastic UC, N-1 security UC, chronological adequacy and scoped expansion without duplicating solver physics.

Result envelopes are stored only after run-id/fingerprint checks, canonical QA and publication gating. Invalid, corrupted, unsupported or no-incumbent results fail closed.

## Data scope

The committed Georgia baseline is sanitized and aggregated. Its 131-asset demonstration registry is not private GSE operational data; the runnable fleet is explicitly documented as an aggregate/subset representation.

## Legacy inventory

Keep as compatibility-only or explicitly unsupported: committable reservoir UC, reservoir `end_level_penalty`, optional BESS depth/end-target behavior, the one-sided BESS/pumped-hydro depth selector, rolling delayed cascades, unvalidated pumped-hydro/DR reserves and unvalidated reserve activation-time deliverability. No AC, voltage/reactive-power, EMT, protection or market-settlement capability is claimed.

## Release record

The release operation is complete. The published release contains the Python wheel, source distribution and Windows x64 portable ZIP, all built by GitHub Actions from the `v1.0.0` tag. This record does not expand the capability boundary documented above.
