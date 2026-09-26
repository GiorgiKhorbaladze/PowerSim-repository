import pytest
from powersim.components import BuildContext, RunOfRiverComponent


def test_ror_preserves_simplified_profile_availability_and_water_value_cost():
    asset = {"id": "r", "type": "hydro_ror", "pmax": 40,
             "availability_profile": "river", "_dispMC": 17}
    component = RunOfRiverComponent(asset)
    context = BuildContext(None, (1, 2), 1, profiles={"river": [.25, 2]})
    assert [component.available_mw(context, i) for i in range(2)] == pytest.approx([10, 40])
    assert component.marginal_cost() == 17

