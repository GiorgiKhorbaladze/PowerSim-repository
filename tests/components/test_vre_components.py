import math

import pytest

from powersim.components import BuildContext, SolarComponent, WindComponent
from powersim.contracts import IssueSeverity


@pytest.mark.parametrize("duration", [1.0, 0.25])
def test_wind_availability_degradation_maintenance_and_duration(duration):
    asset = {
        "id": "w",
        "type": "wind",
        "pmax_installed": 100,
        "availability_profile": "cf",
        "_degradation_factor": 0.9,
        "maint_windows": [
            {"start_h": 0, "end_h": 1, "availability_factor": 0.5}
        ],
    }
    context = BuildContext(None, (1, 2), duration, profiles={"cf": [0.4, 1]})
    component = WindComponent(asset)
    assert component.available_mw(context, 0) == pytest.approx(18)
    expected_second = 45 if duration == 0.25 else 90
    assert component.available_mw(context, 1) == pytest.approx(expected_second)


@pytest.mark.parametrize(
    "cf,expected",
    [(0.0, 0.0), (0.4, 40.0), (1.0, 100.0), (2.0, 100.0)],
)
def test_wind_zero_fractional_full_and_clipped_cf(cf, expected):
    component = WindComponent(
        {
            "id": "w",
            "type": "wind",
            "pmax_installed": 100,
            "availability_profile": "cf",
        }
    )
    context = BuildContext(None, (1,), 1.0, profiles={"cf": [cf]})
    assert component.available_mw(context, 0) == pytest.approx(expected)


def test_solar_clipping_and_degradation_without_wind_extras():
    asset = {
        "id": "s",
        "type": "solar",
        "pmax_installed": 50,
        "availability_profile": "cf",
        "dc_ac_ratio": 1.4,
        "inverter_efficiency": 0.98,
        "_degradation_factor": 0.8,
        "wake_loss_frac": 0.5,
    }
    context = BuildContext(None, (1,), 1, profiles={"cf": [1]})
    assert SolarComponent(asset).available_mw(context, 0) == pytest.approx(39.2)


def test_legacy_missing_and_negative_vre_profile_are_compatibility_warnings():
    component = WindComponent(
        {"id": "w", "type": "wind", "pmax": 10, "availability_profile": "missing"}
    )
    legacy = BuildContext(None, (1,), 1, compatibility_mode=True)
    issues = component.validate(legacy)
    assert any(
        issue.code == "legacy_missing_profile_fallback"
        and issue.severity == IssueSeverity.WARNING
        for issue in issues
    )
    assert component.available_mw(legacy, 0) == pytest.approx(10)

    negative = WindComponent(
        {"id": "w2", "type": "wind", "pmax": 10, "availability_profile": "cf"}
    )
    negative_context = BuildContext(
        None, (1,), 1, profiles={"cf": [-0.25]}, compatibility_mode=True
    )
    issues = negative.validate(negative_context)
    assert any(issue.code == "legacy_negative_profile" for issue in issues)
    assert negative.available_mw(negative_context, 0) == 0


def test_native_v1_rejects_missing_negative_and_nonfinite_profiles_without_crashing():
    cases = [
        ({}, "missing_profile"),
        ({"cf": [-0.1]}, "negative_profile"),
        ({"cf": ["bad"]}, "nonfinite_profile"),
        ({"cf": [math.nan]}, "nonfinite_profile"),
    ]
    for profiles, expected_code in cases:
        component = WindComponent(
            {"id": "w", "type": "wind", "pmax": 10, "availability_profile": "cf"}
        )
        context = BuildContext(
            None, (1,), 1, profiles=profiles, compatibility_mode=False
        )
        issues = component.validate(context)
        assert expected_code in {issue.code for issue in issues}


def test_component_validation_reports_negative_capacity_and_invalid_degradation():
    component = WindComponent(
        {
            "id": "w",
            "type": "wind",
            "pmax": -1,
            "degradation_rate_per_year": "bad",
        }
    )
    context = BuildContext(None, (1,), 1)
    assert {"invalid_capacity", "invalid_degradation"} <= {
        issue.code for issue in component.validate(context)
    }
