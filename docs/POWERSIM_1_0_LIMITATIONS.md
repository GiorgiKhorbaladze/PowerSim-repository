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

## Application and reference data

- The browser application has structured editors for project metadata, workflow, typed solver settings, assets and scenarios. The time axis, profiles, networks, reserves and workflow-specific inputs (stochastic scenarios, contingencies, adequacy and expansion blocks) are edited in the JSON project-document editor. If those inputs are missing, the backend rejects the run and shows the cause.
- The application executor uses the typed `solver_settings`. Solver settings stored only inside `legacy_payload` are not used for application runs.
- Exports are the project JSON and a run bundle (snapshot, manifest, events, hash verification and, for publishable runs only, the result envelope and CSV summaries). There is no report generator, settlement export or regulatory-filing format.
- Running runs cannot be cancelled; only draft and queued runs can. One run executes at a time.
- The plant-parameter catalogue is reference data that requires user review. Imported assets are disabled and review-required. Fuel, cost, availability, inflow and reservoir-model inputs are not in the workbook and must come from approved sources. Reservoir records are browsable but are not mapped to solver storage parameters. Name matching across tables is heuristic and never treated as identity unless exact.
- Known solver accounting issue (pre-existing, not changed by this release): for assets without a heat rate (for example imports), the solver's dispatch marginal cost falls back to `vom` and `vom` is then added again in the objective (`solver/powersim_solver.py`, per-period variable-cost term `(_dispMC + vom) * p`). Cost and objective figures for such assets, including the Georgia import aggregate, are therefore counted twice. Do not use these cost figures until this is corrected and benchmarked.
