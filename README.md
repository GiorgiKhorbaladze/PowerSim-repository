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

Open the local URL printed by the command. The root URL is the PowerSim 1.0 browser application, which uses the same-origin API under `/api`. The previous v4 static prototype in `html/` is archived and not served.

## Study workflow

1. Load the Georgia demonstration or create a project from a typed project document.
2. Edit assets, scenarios and planning inputs.
3. Validate and save. Diagnostics show the path, severity and remediation.
4. Create and launch a run, then follow its lifecycle and failure cause, if any.
5. Inspect results only when QA passes and the result is valid and publishable.
6. Compare compatible saved runs and export the project or a run bundle.

## Reference data

`data/reference/plant_parameters/` contains a normalized transcription of `Power Plant Parameters English.xlsx`. The **Reference data** page lets you browse it, see its quality report and stage station records into a project through an auditable field-level review. **The catalogue requires user review before it is used in a study.** Imported assets are disabled until reviewed. See [reference data](docs/REFERENCE_DATA.md).

The sanitized, aggregated Georgia planning demonstration is at `samples/projects/georgia_2026_baseline.json`; it includes editable 2027-2030 demand scenarios. It is not private GSE operational data and is not an official dispatch case.

## Validated workflows

- Deterministic UC/ED, rolling horizon and sub-hourly operation within the documented component scope.
- Stochastic UC extensive form.
- Deterministic N-1 security UC within non-islanding contingency scope.
- Chronological probabilistic adequacy.
- Scoped capacity-expansion screening.

PowerSim does not claim AC load flow, voltage/reactive-power studies, transient stability, EMT, protection coordination or commercial market settlement.

Read the [capabilities](docs/POWERSIM_1_0_CAPABILITIES.md), [limitations](docs/POWERSIM_1_0_LIMITATIONS.md) and [user guide](docs/POWERSIM_1_0_USER_GUIDE.md) before conducting real studies.
