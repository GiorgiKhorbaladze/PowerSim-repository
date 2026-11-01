# PowerSim 1.0 limitations

PowerSim 1.0 is deliberately fail-closed outside its validated scope.

- DC network studies do not model AC voltage, reactive power or losses. A DC run with nonzero system-level unserved energy is not a validated locational-scarcity result and is blocked from publication.
- Islanding N-1 contingencies are rejected. Stochastic SCUC and stochastic rolling are unsupported.
- Rolling cascades with nonzero travel delay are unsupported in the validated shared path.
- Committable reservoir-hydro UC and reservoir end_level_penalty remain legacy compatibility only.
- The legacy BESS/pumped-hydro depth selector is one-sided; it is not independently validated core physics.
- Pumped-hydro and demand-response reserve provision are not validated. Reserve response-time/activation-ramp deliverability is not validated.
- Chronological adequacy uses independent outages and a transparent chronological dispatch/storage model; it excludes network, UC, hydro chronology, correlated/common-mode outages and capacity accreditation claims.
- Scoped expansion is screening, not an integer multi-year chronological investment optimization.
- The Georgia example is sanitized and aggregated. It is not private GSE operational data and is not an official dispatch case.
- The Georgia 2026 unit-level model (`docs/GEORGIA_2026_MODEL.md`) is single-node, uses mean 2024-2025 hydrology without a reservoir model, fixes net interconnector flow to 2025, and uses generic placeholder thermal efficiency, VOM and fuel prices with full availability. Its total thermal energy follows from the energy balance; the split between thermal plants is not a result.

## Application and reference data

- The browser application has structured editors for project metadata, workflow, typed solver settings, assets, scenarios and workflow-specific inputs (stochastic scenarios, contingencies and reserve products, adequacy samples and forced outage rates, expansion candidates). The time axis, profile values and DC networks are still edited in the JSON project-document editor (or the Excel parameter workbook for profiles).
- The application executor uses the typed `solver_settings`. Solver settings stored only inside `legacy_payload` are not used for application runs.
- Exports are the project JSON and a run bundle (snapshot, manifest, events, hash verification and, for publishable runs only, the result envelope and CSV summaries). There is no report generator, settlement export or regulatory-filing format.
- Running runs cannot be cancelled; only draft and queued runs can. One run executes at a time.
- The plant-parameter catalogue is reference data that requires user review. Imported assets are disabled and review-required. Fuel, cost, availability, inflow and reservoir-model inputs are not in the workbook and must come from approved sources. Reservoir records are browsable but are not mapped to solver storage parameters. Name matching across tables is heuristic and never treated as identity unless exact.
