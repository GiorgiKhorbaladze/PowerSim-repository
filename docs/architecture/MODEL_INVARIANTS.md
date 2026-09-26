# Model invariants and post-solve QA

QA independently recomputes residuals from the resolved input and canonical extracted result. It must not merely evaluate Pyomo constraint bodies from the solved model or delegate pass/fail decisions to physical components. Checks apply per interval/scenario/contingency and at rolling seams.

## Tolerance policy

Mandatory QA operates on unrounded, full-precision canonical numeric results in memory whenever possible and persists its report with the result. For equality residual `r = lhs-rhs`, the internal pass threshold is `T_internal = max(T_solver, A + R·max(1, |lhs|, |rhs|))`, where `T_solver` is the effective primal/integrality feasibility tolerance translated into the invariant's units. Initial release defaults are `A_power=1e-4 MW`, `A_energy=1e-4 MWh`, `A_binary=1e-6`, `A_water=1e-6` in the declared canonical water volume, `A_gas=1e-6` in the declared gas unit, `A_cost=$0.01`, and `R=1e-7`; backend qualification may establish stricter values but must record the effective profile. Inequality violation uses the same scale-aware rule, and aggregated energy/cost checks begin with `R_aggregate=1e-6`.

Persisted-result reconciliation is a distinct check and must account for serialization quantization. Its threshold is `T_persisted = T_internal + Q`, where `Q` is a conservative propagation bound computed from each participating field's declared serialization resolution and the number/coefficient of rounded terms (for a single value rounded to step `q`, its contribution is at most `q/2`). Thus legacy fields serialized to approximately `0.001 MW` cannot be judged against `0.0001 MW` alone. New contracts record field precision or preserve sufficient digits. Persisted reconciliation detects corruption or inconsistent extraction; it must never reject an otherwise valid full-precision solution solely because export rounded it.

For either profile, results within 10× the applicable pass threshold are `warn`; beyond that are `fail`. A project may request stricter tolerances but may not loosen release defaults without recording an approved tolerance profile. Integrality checks use distance to the nearest integer. Missing/nonfinite required data is always `fail`. Every QA result identifies `internal_full_precision` or `persisted_reconciliation`, effective solver tolerance, scale terms and quantization allowance.

## Mandatory checks

| Invariant | Independent reconstruction | Required scope |
|---|---|---|
| Electricity balance | actual dispatched generation + discharge + imports + load shedding = demand + charge + pumping + exports + modeled auxiliary consumption; curtailment is excluded to avoid double counting | every interval; system balance when copperplate |
| Renewable resource accounting | available renewable power/energy = dispatched renewable power/energy + curtailed renewable power/energy, subject to an explicitly declared availability/curtailment convention | every renewable asset and interval, with interval-to-energy reconciliation |
| Nodal balance | injections − withdrawals = signed incident branch flows | every bus, interval, scenario and contingency; sum of nodal residuals cross-checks system balance |
| Hydro water balance | storage[t] = storage[t−1] + inflow + upstream delayed release − release − spill − losses; generation/release conversion and bounds | every reservoir and seam; initial/terminal targets separately reported |
| BESS SOC | SOC[t] = retained SOC[t−1] + η_charge·charge·Δt − discharge·Δt/η_discharge; auxiliary treatment and power/energy bounds | every BESS and seam; forbid simultaneous modes where configured |
| Pumped-hydro energy | upper/lower reservoir or equivalent energy state reconciles generation, pumping, efficiencies, inflows/spill and Δt | every unit/reservoir and seam |
| Ramping | dispatch change, including startup/shutdown formulation and initial/seam state, respects MW/h × Δt limits | every dispatchable unit and adjacent interval |
| Commitment transition | `u[t]−u[t−1]=startup[t]−shutdown[t]`; binary/integrality and no contradictory events | every committable unit, initial state and seam |
| Minimum up/down | every start/shutdown activates the complete required forward/backward duration, including carry-in age and horizon end policy | every committable unit and seam |
| Reserve headroom | awarded upward/downward products plus dispatch fit available limits without double counting; eligibility and response/ramp constraints hold | unit/product/interval; scenario policy declared |
| Reserve energy sufficiency | storage/hydro/DR/import energy can sustain each product for its declared duration after efficiency, state and competing awards | provider/product/interval |
| Transmission | DC flow equals `susceptance_mw_per_rad × (theta_from_rad − theta_to_rad − phase_shift_rad)` under the declared tap/sign convention; imported `x_pu` conversion reconciles to explicit `base_mva`; normal/emergency bounds, reference angle and island rules hold | branch/interval/scenario/contingency |
| Gas | unit heat/fuel reconstruction, shared pipeline/supply/period caps and rolling cumulative use reconcile | unit and constraint group at every applicable period |
| Objective | sum all variable, startup/shutdown, no-load, fuel, emissions, import/export, reserve, penalty, water/terminal and risk terms | total and term breakdown; compare incumbent objective |

Also mandatory are domain/bound checks, profile availability/curtailment, demand-response neutrality/rebound/budget, import/export bounds, reserve requirement satisfaction, chronology length, duplicate IDs and unit consistency.

## QA contract

Each check returns `check_id`, `version`, `scope`, `status` (`pass|warn|fail|not_applicable`), tolerance profile, maximum absolute/scaled violation, count, total checked, and up to N worst witnesses containing IDs/time/scenario/contingency and reconstructed terms. Overall `qa_status` is the worst applicable status; `not_run` is allowed only for results without an incumbent and never implies validity.

`result_validity` is `valid`, `valid_with_warnings`, or `invalid`. A feasible/time-limited incumbent can be valid if extraction and mandatory QA pass. An optimal solve with failed QA is invalid. Reports must display both solver status and QA status.
