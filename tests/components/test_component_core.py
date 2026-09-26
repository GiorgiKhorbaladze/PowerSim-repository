import pytest

from powersim.components import BuildContext, stage3a_registry
from powersim.components.ports import PowerInjectionPort, PowerWithdrawalPort
from powersim.contracts import IssueSeverity, ValidationIssue


def test_registry_order_and_unknown_kind():
    registry = stage3a_registry()
    assert registry.kinds == ("wind", "solar", "hydro_ror", "import")
    with pytest.raises(KeyError, match="unknown component kind"):
        registry.create("battery", {"id": "x"})


def test_power_port_sign_and_units_are_explicit():
    assert PowerInjectionPort("g", {}).unit == "MW"
    assert PowerWithdrawalPort("load", {}).unit == "MW"


def test_capability_negotiation_rejects_unsupported_request():
    component = stage3a_registry().create(
        "wind", {"id": "w", "type": "wind", "pmax": 1}
    )
    context = BuildContext(None, (1,), 1, capabilities=frozenset({"stochastic"}))
    with pytest.raises(ValueError, match="stochastic"):
        component.declare_parameters(context)


def test_component_validation_uses_canonical_issue_contract_and_path():
    component = stage3a_registry().create(
        "wind",
        {"id": "w", "type": "wind", "pmax": "not-a-number", "availability_profile": "missing"},
    )
    context = BuildContext(None, (1,), 1, compatibility_mode=False)
    issues = component.validate(context)
    assert issues
    assert all(isinstance(issue, ValidationIssue) for issue in issues)
    assert all(issue.path.startswith("assets.w") for issue in issues)
    assert any(issue.severity == IssueSeverity.ERROR for issue in issues)


def test_global_period_coordinate_handles_subhourly_window_offsets():
    context = BuildContext(None, (1, 2), 0.25, offset_hours=2.0)
    assert [context.period_coordinate(i) for i in range(2)] == [8, 9]
