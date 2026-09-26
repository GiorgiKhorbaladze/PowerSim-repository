import pytest
from powersim.components import BuildContext, RunOfRiverComponent


def test_ror_preserves_simplified_profile_availability_and_water_value_cost():
    asset = {"id": "r", "type": "hydro_ror", "pmax": 40,
             "availability_profile": "river", "_dispMC": 17}
    component = RunOfRiverComponent(asset)
    context = BuildContext(None, (1, 2), 1, profiles={"river": [.25, 2]})
    assert [component.available_mw(context, i) for i in range(2)] == pytest.approx([10, 40])
    assert component.marginal_cost() == 17


def test_ror_default_clipping_maintenance_and_asset_map_water_value():
    from solver.powersim_solver import build_asset_map
    mapped = build_asset_map({"metadata":{"study_year":2026}, "assets":[{
        "id":"r", "type":"hydro_ror", "pmax":40, "mc":2,
        "hydro":{"water_value":19},
        "maint_windows":[{"start_h":0,"end_h":1,"availability_factor":.5}]}]})["r"]
    component = RunOfRiverComponent(mapped)
    context = BuildContext(None, (1,), 1)
    assert component.available_mw(context, 0) == pytest.approx(13)  # .65 default × 40 × .5
    assert component.marginal_cost() == 19
    profiled = BuildContext(None, (1,), 1, profiles={"river":[2]})
    mapped["availability_profile"] = "river"
    assert component.available_mw(profiled, 0) == 20  # clipped at 1, maintenance once
