# Shared component interfaces

## Build protocol

Every physical component implements a stable protocol independent of workflow and backend adapter:

```python
class Component(Protocol):
    kind: str
    contract_version: str
    def validate(self, asset, context) -> list[Issue]: ...
    def declare_parameters(self, builder, context) -> None: ...
    def declare_variables(self, builder, context) -> None: ...
    def add_constraints(self, builder, context) -> None: ...
    def objective_terms(self, builder, context) -> list[CostTerm]: ...
    def boundary_state(self, solution, at) -> BoundaryState: ...
    def extract(self, solution, context) -> ComponentResult: ...
    def qa(self, resolved_input, result, context) -> list[CheckResult]: ...
```

`BuildContext` supplies canonical time/scenario/contingency sets, `Δt`, buses, workflow capabilities, initial/boundary state, profiles and shared registries. Components may publish typed ports—power injection/withdrawal, reserve capability, fuel use, water flow, state and cost—but may not reach into another component's private variables. Couplers (balance, reserve, cascade, gas and network) consume ports.

## Common asset contract

All assets require stable `id`, `kind`, display metadata, enabled state, bus (unless the workflow is explicitly copperplate), commissioning/retirement applicability, canonical units, profile references and provenance. Optional features are explicit, never inferred from absent keys. Validation rejects dangling profiles/buses, impossible bounds, ambiguous units and unsupported workflow combinations.

## Component responsibilities

| Component | Owns | Publishes / consumes |
|---|---|---|
| Thermal | commitment/start/stop, output bounds, ramps, min-up/down, heat-rate/fuel/emissions and costs | power, reserves, fuel use, costs, boundary commitment/dispatch/age |
| Reservoir hydro | release/spill/storage, conversion/head policy, inflow, cascade outflow and targets | power/reserves, water ports, state and water/penalty costs |
| Run-of-river hydro | inflow-derived availability, optional minimum flow and curtailment/spill | power/reserves where credible, water accounting |
| Wind / solar | availability, clipping/derates/degradation and curtailment | power, optional qualified reserves, curtailment |
| BESS | charge/discharge modes, SOC, losses, degradation/throughput, terminal policy | net power, reserve capability, SOC boundary and costs |
| Pumped hydro | pumping/generation, reservoir/energy state, efficiency and operating modes | net power, reserves, water/energy boundary |
| Demand response | shed/shift, rebound, duration/event/energy budgets and cost | controllable withdrawal, reserves only when qualified, carryover budget |
| Reserves | product definition, requirement, direction, response time, duration, eligibility and substitution | consumes provider offers; publishes requirement/shortfall/cost diagnostics |
| Network | buses/branches, angle/flow/injection equations, slack/islands, limits/loss policy | consumes nodal power ports; publishes flows/congestion/security state |
| Imports/exchange | directional capacity/profile, price, ramp/energy/take-or-pay and outage identity | nodal power, reserve eligibility if declared, costs and boundary use |

## State and chronology

Boundary states are typed, versioned and complete enough to make a split horizon equivalent to a continuous formulation under the declared look-ahead/terminal policy. They include thermal on/off duration and prior dispatch, storage/hydro states, delayed cascade queues, DR budgets/events, cumulative gas/energy limits and any active min-up/down obligation. Missing state is an error, not a default, after the first window.

## Capability negotiation

Each workflow publishes required and optional capabilities. Components publish supported capabilities (scenario indexing, contingency indexing, integer decisions, boundary state, reserves). Model construction fails before solve if a requested combination is unsupported. This prevents silent deterministic/stochastic/security feature drift.

## Result naming

Results use stable physical concepts, explicit units and coordinates, not Pyomo names. Each series identifies asset, variable, unit, time axis and optional scenario/contingency. Derived KPIs declare formula/version. Component and QA schemas are contract-tested against UI/report/database consumers.
