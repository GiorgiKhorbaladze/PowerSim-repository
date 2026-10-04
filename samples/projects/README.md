# Sanitized planning projects

`georgia_2026_baseline.json` is a PowerSim typed application project for a reproducible local planning demonstration.

- Demand uses the committed hourly P50 projection in `data/gse_load_2026_2030.json`.
- The separate `samples/demo_asset_registry_2026.json` is the sanitized 131-asset registry (4,770.1935 MW).
- The runnable optimisation representation deliberately uses two synthetic aggregate resources: 1,800 MW thermal and 1,500 MW import. It is not the literal plant registry and is not private GSE operational data.
- The project contains editable 2027-2030 scenarios. They replace only the committed demand projection. They do not assert plant commissioning schedules or other unverified planning assumptions.
- The template deliberately excludes features outside the validated release scope.

Use this project for reproducible software acceptance and planning-method demonstrations. Do not use it as an official operational dispatch case without controlled import and validation of authorised data.
