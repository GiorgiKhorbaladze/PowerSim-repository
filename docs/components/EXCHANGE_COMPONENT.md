# Exchange component

The architecture calls this component exchange while the Stage 3A canonical registry kind remains
`import` for input compatibility. It is a unidirectional, non-negative power injection. Available
capacity is the named `pmax_profile[t]`, a scalar `pmax_profile`, or `pmax`, followed by existing
maintenance and temperature derates. Dispatch is bounded by available MW and cost remains the
legacy marginal cost times dispatched MWh.

Stage 3A does not model export, bilateral settlement, loop flow, AC physics, ramp/energy contracts,
or reserve eligibility. Future directions can add withdrawal/nodal ports without changing the
positive-injection convention.

Native validation rejects missing, malformed, non-finite, short, or negative profiles and negative
scalar capacity with canonical `assets.<id>.<field>` paths. Legacy mode falls back to scalar `pmax`
for a missing/invalid named profile and emits a compatibility warning. Canonical records join the
existing QA/publication gate. Shared CostTerms are parity-only in Stage 3A; the assembler retains
the legacy objective expression.
