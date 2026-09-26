import pytest
import pyomo.environ as pyo

from powersim.components import BuildContext, stage3a_registry
from powersim.components.ports import PowerInjectionPort, PowerWithdrawalPort
from powersim.components.ports import CurtailmentPort
from powersim.workflows.deterministic import SharedComponentSession, build_stage3a_context


def test_registry_order_and_unknown_kind():
    registry = stage3a_registry()
    assert registry.kinds == ("wind", "solar", "hydro_ror", "import")
    with pytest.raises(KeyError, match="unknown component kind"):
        registry.create("battery", {"id": "x"})


def test_power_port_sign_and_units_are_explicit():
    assert PowerInjectionPort("g", {}).unit == "MW"
    assert PowerWithdrawalPort("load", {}).unit == "MW"


def test_capability_negotiation_rejects_unsupported_request():
    component = stage3a_registry().create("wind", {"id": "w", "type": "wind", "pmax": 1})
    context = BuildContext(None, (1,), 1, capabilities=frozenset({"stochastic"}))
    with pytest.raises(ValueError, match="stochastic"):
        component.declare_parameters(context)


def test_curtailment_port_is_availability_minus_dispatch():
    model = pyo.ConcreteModel()
    model.T = pyo.Set(initialize=[1])
    model.G = pyo.Set(initialize=["w"])
    model.p = pyo.Var(model.G, model.T)
    model.p["w", 1].value = 3.0
    assets = {"w": {"id": "w", "type": "wind", "pmax_installed": 10,
                     "availability_profile": "cf"}}
    context = build_stage3a_context(model, [1], 1, {"cf": [.5]}, assets,
                                    validation_mode="native")
    SharedComponentSession(context).build()
    port = next(port for port in context.power_ports if isinstance(port, CurtailmentPort))
    assert pyo.value(port.values[1]) == pytest.approx(2.0)
