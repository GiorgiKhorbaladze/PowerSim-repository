import pytest

from powersim.components import BuildContext, SolarComponent, WindComponent


@pytest.mark.parametrize("duration", [1.0, 0.25])
def test_wind_availability_degradation_maintenance_and_duration(duration):
    asset = {"id": "w", "type": "wind", "pmax_installed": 100,
             "availability_profile": "cf", "_degradation_factor": .9,
             "maint_windows": [{"start_h": 0, "end_h": 1, "availability_factor": .5}]}
    context = BuildContext(None, (1, 2), duration, profiles={"cf": [.4, 1]})
    component = WindComponent(asset)
    assert component.available_mw(context, 0) == pytest.approx(18)
    expected_second = 45 if duration == 0.25 else 90
    assert component.available_mw(context, 1) == pytest.approx(expected_second)


def test_solar_clipping_and_degradation_without_wind_extras():
    asset = {"id": "s", "type": "solar", "pmax_installed": 50,
             "availability_profile": "cf", "dc_ac_ratio": 1.4,
             "inverter_efficiency": .98, "_degradation_factor": .8,
             "wake_loss_frac": .5}
    context = BuildContext(None, (1,), 1, profiles={"cf": [1]})
    assert SolarComponent(asset).available_mw(context, 0) == pytest.approx(39.2)


def test_component_validation_reports_negative_capacity_and_missing_profile():
    component = WindComponent({"id": "w", "type": "wind", "pmax": -1,
                               "availability_profile": "missing"})
    context = BuildContext(None, (1,), 1)
    assert {issue.code for issue in component.validate(context)} == {"invalid_capacity", "missing_profile"}

