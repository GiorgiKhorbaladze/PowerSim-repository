# Independent QA framework

Stage 2 QA consumes resolved input plus canonical extracted data. It does not infer correctness from Pyomo constraint objects. The registry deterministically aggregates executed checks as `fail > warn > pass`; `not_run` is used only when none ran. Individual deferred/non-applicable checks remain visibly `not_run` and do not masquerade as passing.

Implemented checks are recursive finite-value inspection, interval electricity balance, and basic non-negative domain sanity. During Stage 2, `generation_mw` means **net system supply**: dispatch plus imports, BESS discharge, pumped-hydro generation and DR, less BESS charging and pumping already embedded by extraction. The independent identity is therefore `generation_mw + unserved_mw = load_mw`; QA must not add charging or pumping a second time. The objective-reconstruction interface exists but honestly reports `not_run` until Stage 3 exposes canonical component costs.

All thresholds live in `qa/tolerance.py`. Internal/full-precision checks use tighter absolute and relative tolerances than persisted/rounded reconciliation. The canonical legacy writer currently exposes generation at 0.01 MW and unserved energy at 0.001 MWh, so its wired QA call explicitly uses `persisted` reconciliation tolerance; each check records that mode in its witness. Checks over future unrounded canonical extraction will use `internal` mode. Checks report tolerance, maximum violation, count and witnesses where relevant.

## Publication gate

Publication is fail-closed and is now invoked by `build_result_store`, the canonical deterministic result path. It requires a usable solver status and incumbent, completed extraction, finite required values, executed mandatory QA, and no QA failure. The path attaches the QA report and publication decision, then finalizes result validity and QA status. A time-limit incumbent is usable but is qualified. A no-incumbent placeholder retained for diagnostic shape compatibility cannot publish and is never committed into a rolling run.

Stage 3 must define richer canonical interval/component schemas, implement complete objective reconstruction and component-specific conservation checks, and route the stochastic module's independent writer plus adequacy, expansion, security-screening, and historical auxiliary writers through this gate.
