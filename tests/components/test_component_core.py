import pytest

from powersim.components import BuildContext, stage3a_registry
from powersim.components.ports import PowerInjectionPort, PowerWithdrawalPort


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

