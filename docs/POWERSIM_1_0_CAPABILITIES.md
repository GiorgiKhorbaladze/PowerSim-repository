# PowerSim 1.0 capabilities

## Validated

- Deterministic UC/ED, including rolling horizon and sub-hourly operation within the documented shared-component scope.
- Thermal UC, wind, solar, simplified run-of-river, non-committable reservoir hydro, BESS, pumped hydro, demand response, imports, gas budgets, reserves and DC network studies with independent QA and publication gating.
- Stochastic UC extensive form with shared deterministic physics. Stochastic rolling is not supported.
- Deterministic N-1 security UC for validated generator/import/line contingencies that do not island the DC network.
- Chronological probabilistic adequacy with seeded independent outages and chronological BESS state.
- Same-origin local application, immutable run inputs, provenance, asynchronous local execution, persisted run history, scenario comparison, browser E2E, wheel/sdist installation and Windows local application packaging.

## Screening

- Scoped capacity expansion: hourly continuous-MW LP screening only. It is not a chronological investment UC or network-expansion model.
- Legacy adequacy and expansion interfaces retained for compatibility are screening only.

## Unsupported or fail-closed

See [limitations](POWERSIM_1_0_LIMITATIONS.md). PowerSim does not claim AC power flow, voltage/reactive-power studies, transient stability, EMT, protection coordination or commercial settlement.
