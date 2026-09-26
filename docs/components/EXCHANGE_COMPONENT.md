# Exchange component

The architecture calls this component exchange while the Stage 3A canonical registry kind remains
`import` for input compatibility. It is a unidirectional, non-negative power injection. Available
capacity is the named `pmax_profile[t]`, a scalar `pmax_profile`, or `pmax`, followed by existing
maintenance and temperature derates. Dispatch is bounded by available MW and cost remains the
legacy `(_dispMC + vom) × dispatch × duration` contribution.

Negative scalar capacity, negative/non-finite profile capacity, and a named missing import profile
are validation errors. They are not silently clamped or defaulted because the corresponding legacy
cases did not have a safe deterministic numeric fallback.

Canonical import results are carried with the shared solve and participate in the unified QA report;
the independent import-bound check can therefore block publication. Shared cost terms are
parity-tested but the transitional assembler still uses the legacy objective expression.

Stage 3A does not model export, bilateral settlement, loop flow, AC physics, ramp/energy contracts,
or reserve eligibility. Future directions can add withdrawal/nodal ports without changing the
positive-injection convention.
