# Sanitized planning projects

`georgia_2026_baseline.json` is a PowerSim typed application project for a reproducible local planning demonstration.

- Demand uses the committed hourly P50 projection in `data/gse_load_2026_2030.json`.
- The separate `samples/demo_asset_registry_2026.json` is the sanitized 131-asset registry (4,770.1935 MW).
- The runnable optimisation representation deliberately uses two synthetic aggregate resources: 1,800 MW thermal and 1,500 MW import. It is not the literal plant registry and is not private GSE operational data.
- The project contains editable 2027-2030 scenarios. They replace only the committed demand projection. They do not assert plant commissioning schedules or other unverified planning assumptions.
- The template deliberately excludes features outside the validated release scope.

Use this project for reproducible software acceptance and planning-method demonstrations. Do not use it as an official operational dispatch case without controlled import and validation of authorised data.

# Georgia 2026 unit-level model

`georgia_2026_balance.json` is a 38-asset, 8,760-hour model of 2026 built by `scripts/build_georgia_2026_model.py`. It uses the GSE P50 2026 load, the 2026 capacity registry, mean 2024-2025 plant output from the user-supplied monthly balances, and the 2025 monthly net interconnector flow. Thermal efficiency, VOM and fuel prices are generic placeholders marked `review_required`. Not an official forecast. See `docs/GEORGIA_2026_MODEL.md`.

`georgia_2027_balance.json` is the same model with the GSE P50 2027 load and the 206 MW Ruisi wind farm (assumed in operation all year with the Kartli wind capacity factor).

# Georgia 2030 (PLEXOS reconstruction)

`georgia_2030_plexos.json` - see `docs/GEORGIA_PLEXOS_MODEL.md`.
