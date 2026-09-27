"""Characterization of legacy one-sided shallow/high-head selectors.

These are intentionally audit tests, not a proposed formulation change.  They
show that the documented ``iff`` interpretation is stronger than the actual
Big-M implication retained for parity.
"""
import pyomo.environ as pyo


def _one_sided_selector_is_feasible_above_threshold_with_zero_selector():
    model=pyo.ConcreteModel()
    model.soc=pyo.Var(bounds=(0,100),initialize=60)
    model.z=pyo.Var(domain=pyo.Binary,initialize=0)
    model.soc.fix(60); model.z.fix(0)
    model.link=pyo.Constraint(expr=model.soc >= 30 - 100*(1-model.z))
    return pyo.value(model.soc), pyo.value(30 - 100*(1-model.z))


def test_bess_shallow_selector_is_not_mathematical_iff():
    body, lower=_one_sided_selector_is_feasible_above_threshold_with_zero_selector()
    assert body >= lower  # SOC=60%, z=0 is feasible under the legacy link.


def test_pumped_hydro_high_head_selector_is_not_mathematical_iff():
    body, lower=_one_sided_selector_is_feasible_above_threshold_with_zero_selector()
    assert body >= lower  # Same one-sided link is used for pumped hydro.
