# Solver termination mapping

The only normalized values are `optimal`, `feasible`, `time_limit`, `infeasible`, `unbounded`, `numerical_error`, and `solver_error`. Raw termination text is preserved separately.

| Raw termination family | Normalized status |
|---|---|
| optimal / globally optimal / locally optimal | `optimal` |
| feasible | `feasible` |
| time/max-time limit | `time_limit` |
| infeasible | `infeasible` |
| unbounded | `unbounded` |
| numerical difficulty / invalid problem | `numerical_error` |
| unknown, ambiguous infeasible-or-unbounded, exception | `solver_error` |

Mapping order ensures a time limit remains `time_limit`, whether or not it has an incumbent. Incumbent availability is an independent flag. Ambiguous states fail closed rather than claiming feasibility. `solved` is not a solver status.

Infeasible, unbounded, numerical-error, solver-error, and any no-incumbent outcome is invalid. A time-limited incumbent may proceed to extraction and QA, but can be at most valid-with-warnings. For rolling horizons, the highest-severity window status is the aggregate status; all windows must have incumbents, and a no-incumbent window aborts the run. Consequently a later success cannot hide an earlier failure.
