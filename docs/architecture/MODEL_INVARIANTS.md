# Model invariants and post-solve QA

QA independently recomputes residuals from the resolved input and canonical extracted result. It must not merely evaluate Pyomo constraint bodies from the solved model. Checks apply per interval/scenario/contingency and at rolling seams.

## Tolerance policy

For equality residual `r = lhs-rhs`, pass when `|r| ≤ A + R·max(1, |lhs|, |rhs|)`. Defaults are `A_power=1e-4 MW`, `A_energy=1e-4 MWh`, `A_binary=1e-6`, `A_water=1e-6` in the declared canonical water volume, `A_gas=1e-6` in the declared gas unit, `A_cost=$0.01`, and `R=1e-7`. Inequality violation uses the same scale rule. Aggregated energy/cost checks use `R_aggregate=1e-6`.

Results within 10× the pass threshold are `warn`; beyond that are `fail`. A project may request stricter tolerances but may not loosen release defaults without recording an approved tolerance profile. Integrality checks use distance to the nearest integer. Missing/nonfinite required data is always `fail`.

## Mandatory checks

| Invariant | Independent reconstruction | Required scope |
|---|---|---|
| Electricity balance | generation + discharge + imports + load shed = demand + charge + pumping + exports + modeled auxiliary load + curtailment only where convention requires | every interval; system balance when copperplate |
| Nodal balance | injections − withdrawals = signed incident branch flows | every bus, interval, scenario and contingency; sum of nodal residuals cross-checks system balance |
| Hydro water balance | storage[t] = storage[t−1] + inflow + upstream delayed release − release − spill − losses; generation/release conversion and bounds | every reservoir and seam; initial/terminal targets separately reported |
| BESS SOC | SOC[t] = retained SOC[t−1] + η_charge·charge·Δt − discharge·Δt/η_discharge; auxiliary treatment and power/energy bounds | every BESS and seam; forbid simultaneous modes where configured |
| Pumped-hydro energy | upper/lower reservoir or equivalent energy state reconciles generation, pumping, efficiencies, inflows/spill and Δt | every unit/reservoir and seam |
| Ramping | dispatch change, including startup/shutdown formulation and initial/seam state, respects MW/h × Δt limits | every dispatchable unit and adjacent interval |
| Commitment transition | `u[t]−u[t−1]=startup[t]−shutdown[t]`; binary/integrality and no contradictory events | every committable unit, initial state and seam |
| Minimum up/down | every start/shutdown activates the complete required forward/backward duration, including carry-in age and horizon end policy | every committable unit and seam |
| Reserve headroom | awarded upward/downward products plus dispatch fit available limits without double counting; eligibility and response/ramp constraints hold | unit/product/interval; scenario policy declared |
| Reserve energy sufficiency | storage/hydro/DR/import energy can sustain each product for its declared duration after efficiency, state and competing awards | provider/product/interval |
| Transmission | DC flow equals susceptance × angle difference (with tap/phase policy); normal/emergency bounds, reference angle and island rules hold | branch/interval/scenario/contingency |
| Gas | unit heat/fuel reconstruction, shared pipeline/supply/period caps and rolling cumulative use reconcile | unit and constraint group at every applicable period |
| Objective | sum all variable, startup/shutdown, no-load, fuel, emissions, import/export, reserve, penalty, water/terminal and risk terms | total and term breakdown; compare incumbent objective |

Also mandatory are domain/bound checks, profile availability/curtailment, demand-response neutrality/rebound/budget, import/export bounds, reserve requirement satisfaction, chronology length, duplicate IDs and unit consistency.

## QA contract

Each check returns `check_id`, `version`, `scope`, `status` (`pass|warn|fail|not_applicable`), tolerance profile, maximum absolute/scaled violation, count, total checked, and up to N worst witnesses containing IDs/time/scenario/contingency and reconstructed terms. Overall `qa_status` is the worst applicable status; `not_run` is allowed only for results without an incumbent and never implies validity.

`result_validity` is `valid`, `valid_with_warnings`, or `invalid`. A feasible/time-limited incumbent can be valid if extraction and mandatory QA pass. An optimal solve with failed QA is invalid. Reports must display both solver status and QA status.
