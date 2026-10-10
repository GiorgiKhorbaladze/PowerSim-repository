# PowerSim 1.0 final audit

## Release evidence

This audit is completed only after the final tagged commit and its release CI finish. The release workflow must build wheel, sdist and Windows artifacts from the tag itself.

Current verified pre-tag evidence includes:

- Python 3.10, 3.11 and 3.12 full core CI.
- Full pytest tree and no-orphan pytest gate.
- Real browser E2E: visible project save, two queued deterministic runs, QA/validity result display and comparison.
- Clean wheel/sdist install, installed CLI/server and real compact solve.
- Windows portable and silent-installer lifecycle: install, installed executable solve, restart with persisted history and uninstall.
- Publishable 8,760-hour sanitized Georgia 2026 application acceptance.
- Compact 15-minute, rolling deterministic, stochastic, security, adequacy and expansion suites in the validated test tree.

## Architecture ownership

UI and API control projects/runs only. The Python/Pyomo backend owns electrical equations. Result envelopes are accepted only after immutable fingerprint matching, canonical QA and publication gating.

## Known limitations

See [PowerSim 1.0 limitations](../POWERSIM_1_0_LIMITATIONS.md). No unsupported capability is promoted by this audit.
