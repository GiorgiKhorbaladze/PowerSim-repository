# PowerSim 1.0

PowerSim is a local electricity-system planning, simulation and optimization application. Its Python/Pyomo backend owns the electrical equations; the browser UI manages projects, scenarios, runs, QA and results.

## Quick start

### Windows

Download and install `PowerSim-1.0.0-Windows-x64-Setup.exe`. Launch **PowerSim** from the Start Menu. It starts a loopback-only local server and opens the browser UI.

### Developer installation

From a source checkout:

```bash
python -m pip install .
powersim serve --workspace ./powersim_workspace
```

Or install the wheel built by the release workflow:

```bash
python -m pip install dist/powersim-1.0.0-py3-none-any.whl
powersim serve --workspace ./powersim_workspace
```

Open the local URL printed by the command.

## Study workflow

1. Load or create a typed project.
2. Select a scenario and a workflow.
3. Validate, save, create and launch a run.
4. Monitor queued, solving, QA and publication state.
5. Inspect results only when QA passes and the result is valid/publishable.
6. Compare compatible saved runs.

The sanitized, aggregated Georgia planning demonstration is at `samples/projects/georgia_2026_baseline.json`; it includes editable 2027-2030 demand scenarios. It is not private GSE operational data and is not an official dispatch case.

## Validated workflows

- Deterministic UC/ED, rolling horizon and sub-hourly operation within the documented component scope.
- Stochastic UC extensive form.
- Deterministic N-1 security UC within non-islanding contingency scope.
- Chronological probabilistic adequacy.
- Scoped capacity-expansion screening.

PowerSim does not claim AC load flow, voltage/reactive-power studies, transient stability, EMT, protection coordination or commercial market settlement.

Read the [capabilities](docs/POWERSIM_1_0_CAPABILITIES.md), [limitations](docs/POWERSIM_1_0_LIMITATIONS.md) and [user guide](docs/POWERSIM_1_0_USER_GUIDE.md) before conducting real studies.
