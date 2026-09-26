# Independent QA framework

Stage 2 QA consumes resolved input plus canonical extracted data. It does not infer correctness from Pyomo constraint objects. The registry deterministically aggregates executed checks as `fail > warn > pass`; `not_run` is used only when none ran. Individual deferred/non-applicable checks remain visibly `not_run` and do not masquerade as passing.

Implemented checks are recursive finite-value inspection, interval electricity balance, and basic non-negative domain sanity. Electricity balance expects canonical interval fields (`load_mw`, `generation_mw`, with optional unserved, curtailment and storage charge). The objective-reconstruction interface exists but honestly reports `not_run` until Stage 3 exposes canonical component costs.

All thresholds live in `qa/tolerance.py`. Internal/full-precision checks use tighter absolute and relative tolerances than persisted/rounded reconciliation. Checks report tolerance, maximum violation, count and witnesses where relevant.

## Publication gate

Publication is fail-closed. It requires a usable solver status and incumbent, completed extraction, finite required values, executed mandatory QA, and no QA failure. A time-limit incumbent is usable but is qualified. A no-incumbent placeholder retained for legacy shape compatibility cannot publish. Final diagnostics must be updated consistently with QA and result validity before constructing the immutable result envelope.

Stage 3 must define richer canonical interval/component schemas, implement complete objective reconstruction and component-specific conservation checks, and route every legacy reporting path through this gate.
