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


@pytest.mark.parametrize("bad", ["not-a-number", float("nan"), float("inf")])
def test_validation_never_raises_for_malformed_capacity_or_profile(bad):
    component = WindComponent({"id": "w", "type": "wind", "pmax": bad,
                               "availability_profile": "cf"})
    context = BuildContext(None, (1,), 1, profiles={"cf": [bad]})
    codes = {issue.code for issue in component.validate(context)}
    assert "invalid_capacity" in codes
    assert codes & {"non_numeric_profile", "nonfinite_profile"}


def test_native_and_legacy_profile_compatibility_are_explicit():
    asset = {"id": "w", "type": "wind", "pmax_installed": 10,
             "availability_profile": "missing"}
    native = BuildContext(None, (1,), 1, validation_mode="native")
    legacy = BuildContext(None, (1,), 1, validation_mode="legacy")
    assert WindComponent(asset).validate(native)[0].severity.value == "error"
    issue = WindComponent(asset).validate(legacy)[0]
    assert issue.severity.value == "warning" and issue.context["compatibility"]
    assert WindComponent(asset).available_mw(legacy, 0) == 10

    negative = BuildContext(None, (1,), 1, profiles={"cf": [-.5]}, validation_mode="legacy")
    neg_asset = {**asset, "availability_profile": "cf"}
    issues = WindComponent(neg_asset).validate(negative)
    assert any(item.code == "negative_profile" and item.severity.value == "warning" for item in issues)
    assert WindComponent(neg_asset).available_mw(negative, 0) == 0
