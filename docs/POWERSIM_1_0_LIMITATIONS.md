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
