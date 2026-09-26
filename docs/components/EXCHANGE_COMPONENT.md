# Exchange component

The architecture calls this component exchange while the Stage 3A canonical registry kind remains
`import` for input compatibility. It is a unidirectional, non-negative power injection. Available
capacity is the named `pmax_profile[t]`, a scalar `pmax_profile`, or `pmax`, followed by existing
maintenance and temperature derates. Dispatch is bounded by available MW and cost remains the
legacy marginal cost times dispatched MWh.

Stage 3A does not model export, bilateral settlement, loop flow, AC physics, ramp/energy contracts,
or reserve eligibility. Future directions can add withdrawal/nodal ports without changing the
positive-injection convention.

