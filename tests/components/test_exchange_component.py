import pytest

from powersim.components import BuildContext, ExchangeComponent


def test_exchange_scalar_and_profile_capacity():
    scalar = ExchangeComponent({"id": "i", "type": "import", "pmax": 20})
    assert scalar.available_mw(BuildContext(None, (1,), 1), 0) == 20

    profiled = ExchangeComponent(
        {"id": "i", "type": "import", "pmax": 99, "pmax_profile": "border"}
    )
    context = BuildContext(None, (1, 2), 0.25, profiles={"border": [0, 12.5]})
    assert [profiled.available_mw(context, i) for i in range(2)] == pytest.approx(
        [0, 12.5]
    )


@pytest.mark.parametrize(
    "asset,profiles,expected_code",
    [
        ({"id": "i", "type": "import", "pmax": -1}, {}, "invalid_capacity"),
        (
            {"id": "i", "type": "import", "pmax_profile": "border"},
            {"border": [-1]},
            "negative_import_capacity",
        ),
        (
            {"id": "i", "type": "import", "pmax_profile": "missing"},
            {},
            "missing_profile",
        ),
        (
            {"id": "i", "type": "import", "pmax_profile": "border"},
            {"border": ["bad"]},
            "nonfinite_profile",
        ),
    ],
)
def test_invalid_import_capacity_and_profiles_are_rejected(asset, profiles, expected_code):
    component = ExchangeComponent(asset)
    context = BuildContext(None, (1,), 1, profiles=profiles, compatibility_mode=True)
    issues = component.validate(context)
    assert expected_code in {issue.code for issue in issues}
