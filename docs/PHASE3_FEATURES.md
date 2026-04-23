# PowerSim v4.0 — Phase 3 features (PLEXOS/Gurobi gap-closures)

This phase closes 11 of the gaps identified vs. PLEXOS / Gurobi.
Schema version bumps to **1.3** (v1.2 inputs keep loading with a warning).

| # | Gap | What shipped |
|---|-----|--------------|
| 2 | Stochastic UC | `solver/powersim_stochastic.py` — 2-stage consensus-UC with E[cost]/CVaR/expected+cvar objectives |
| 3 | Sub-hourly resolution | `resolution_min ∈ {60,30,15,5}`; profiles auto-resample, `dt` threaded through every constraint |
| 4 | Capacity expansion | `solver/powersim_expansion.py` — LP screening planner with CRF + reserve-margin + energy-closure constraints |
| 5 | Gurobi drop-in | `solver_settings.solver = auto|highs|gurobi|cplex`; availability probe falls back to HiGHS |
| 6 | Warm start | `solver_settings.warm_start = true` — rolled-forward `p`/`u` hints between rolling windows |
| 9 | Piecewise heat-rate curves | `heat_rate_curve: [[pmw,hr],…]` on any thermal asset — convex-combination λ, plugs into the MIP objective |
| 15 | SQL scenario store | `scripts/db_ingest.py` — SQLite with runs/run_by_unit/run_monthly tables + `--list`, `--compare`, `--query` |
| 16 | N-way Compare | HTML Compare tab now accepts 1+N result JSONs; KPI matrix + pivot bar chart |
| 17 | Inline input validation | Assets tab shows live red hints (pmin≥pmax, SoC-min≥SoC-max, efficiency=0, …) as you type |
| 18 | Batch orchestrator | `scripts/batch.py` — runs a queue of jobs, auto-ingests each run into the SQL store, writes a manifest |
| 19 | User-defined KPI templates | `solver/powersim_kpi.py` (Python) + identical in-browser evaluator; 6 built-in defaults, safe expression DSL |

## Backward compatibility

* v1.0/1.1/1.2 input JSONs still load (validator emits a warning).
* `resolution_min` defaults to 60 if absent — all legacy runs reproduce
  byte-for-byte identical numbers.
* `solver_settings.solver="auto"` is the default — HiGHS stays the
  shipping baseline unless Gurobi is installed and licensed.

## One-liner demos

```bash
# 15-min resolution, 24h, full GSE fleet
python scripts/run_horizon.py --project-dir project_data \
    --config tests/gse_2026_baseline.json --hours 24 \
    --rolling-window 24 --rolling-step 24 --out-dir out/gse_15min_24h \
    --mip-gap 0.02

# Capacity expansion screening
python solver/powersim_expansion.py --input my_input.json --output plan.json

# Stochastic 2-stage with P10/base/P90 tree
python solver/powersim_stochastic.py --input my_stoch_input.json --out-dir out/stoch

# Batch of MC scenarios with auto SQL ingest
python scripts/batch.py --queue batches/mc.json

# Query the store — top-10 cheapest A_mean runs
python scripts/db_ingest.py --query "
  SELECT run_id, horizon_hours, total_cost_usd, avg_lambda_usd_mwh
  FROM runs WHERE scenario='A_mean'
  ORDER BY total_cost_usd LIMIT 10"

# Evaluate a KPI template file against a result
python solver/powersim_kpi.py --results out/run_720h/powersim_results.json \
    --templates my_kpis.json
```

## KPI template DSL

A KPI template is `{id, label, unit, formula}`. Supported functions:

    sum(field [ | filter ])
    avg(field [ | filter ])   # same as mean()
    min(field [ | filter ])
    max(field [ | filter ])
    count(field | filter)
    p10(field), p50(field), p90(field)
    ratio(num_field / den_field)
    by_unit_sum(field, asset_id)
    hours_where(filter)

Filter operators: `==  !=  <  <=  >  >=`, `X in [n, n, ...]`, `X between a and b`.

Virtual fields: `hour_of_day`, `is_weekend` (derived from `hour_of_year`).
Base fields: any key in `hourly_system[i]` or `by_unit_summary[asset_id]`.

Example templates:

```json
[
  {"id":"peak_lambda",    "label":"λ avg 17-22",        "unit":"$/MWh",
   "formula":"avg(lambda_usd_mwh | hour_of_day in [17,18,19,20,21,22])"},
  {"id":"unserved_hours", "label":"Hours w/ unserved",  "unit":"h",
   "formula":"hours_where(unserved_mwh > 0.1)"},
  {"id":"p90_lambda",     "label":"P90 λ",              "unit":"$/MWh",
   "formula":"p90(lambda_usd_mwh)"},
  {"id":"gas_utilization","label":"Gas cap utilization","unit":"%",
   "formula":"ratio(gas_mm3h / gas_mm3h)"}
]
```

## SQL store quick-recipe

```sql
-- Compare scenarios within a tag
SELECT scenario, AVG(total_cost_usd) AS mean_cost,
       AVG(avg_lambda_usd_mwh) AS mean_lambda
FROM runs WHERE tag LIKE 'phase3:%'
GROUP BY scenario ORDER BY mean_cost;

-- Best/worst asset-level CF across tagged runs
SELECT asset_id, MIN(cf_pct), MAX(cf_pct)
FROM run_by_unit JOIN runs USING(run_id)
WHERE tag LIKE 'phase3:%' GROUP BY asset_id ORDER BY 3 DESC LIMIT 10;
```

## Notes / known limits

* Stochastic UC uses a consensus-commitment heuristic (2-stage) rather
  than a full deterministic-equivalent extensive-form solve.  Good
  enough for 3-5 scenarios on weekly horizons; scales much better than
  the full formulation.
* Capacity expansion is a *screening* LP: it returns recommended new-
  build MW by technology, not hourly dispatch.  Paste the winners into
  `assets` and re-solve to get operations.
* Gurobi path untested in this environment (no license); the auto-probe
  gracefully falls back to HiGHS.
