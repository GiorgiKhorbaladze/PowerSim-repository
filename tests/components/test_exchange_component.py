import pytest
from powersim.components import BuildContext, ExchangeComponent


def test_exchange_scalar_and_profile_capacity():
    scalar = ExchangeComponent({"id": "i", "type": "import", "pmax": 20})
    assert scalar.available_mw(BuildContext(None, (1,), 1), 0) == 20
    profiled = ExchangeComponent({"id": "i", "type": "import", "pmax": 99,
                                  "pmax_profile": "border"})
    context = BuildContext(None, (1, 2), .25, profiles={"border": [0, 12.5]})
    assert [profiled.available_mw(context, i) for i in range(2)] == pytest.approx([0, 12.5])


@pytest.mark.parametrize("asset,profiles", [
    ({"id":"i", "type":"import", "pmax":-1}, {}),
    ({"id":"i", "type":"import", "pmax_profile":"cap"}, {"cap":[-1]}),
    ({"id":"i", "type":"import", "pmax":"bad"}, {}),
    ({"id":"i", "type":"import", "pmax_profile":"cap"}, {"cap":["bad"]}),
])
def test_invalid_exchange_capacity_is_structured(asset, profiles):
    issues = ExchangeComponent(asset).validate(BuildContext(None, (1,), 1, profiles=profiles))
    assert issues and all(issue.path.startswith("assets.i.") for issue in issues)


def test_missing_import_profile_legacy_falls_back_but_native_rejects():
    asset = {"id":"i", "type":"import", "pmax":7, "pmax_profile":"missing"}
    legacy = BuildContext(None, (1,), 1, validation_mode="legacy")
    native = BuildContext(None, (1,), 1, validation_mode="native")
    assert ExchangeComponent(asset).validate(legacy)[0].severity.value == "warning"
    assert ExchangeComponent(asset).available_mw(legacy, 0) == 7
    assert ExchangeComponent(asset).validate(native)[0].severity.value == "error"

    invalid = BuildContext(None, (1,), 1, profiles={"missing": "bad"}, validation_mode="legacy")
    issue = ExchangeComponent(asset).validate(invalid)[0]
    assert issue.severity.value == "warning" and ExchangeComponent(asset).available_mw(invalid, 0) == 7
