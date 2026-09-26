import pytest
from powersim.components import BuildContext, ExchangeComponent


def test_exchange_scalar_and_profile_capacity():
    scalar = ExchangeComponent({"id": "i", "type": "import", "pmax": 20})
    assert scalar.available_mw(BuildContext(None, (1,), 1), 0) == 20
    profiled = ExchangeComponent({"id": "i", "type": "import", "pmax": 99,
                                  "pmax_profile": "border"})
    context = BuildContext(None, (1, 2), .25, profiles={"border": [0, 12.5]})
    assert [profiled.available_mw(context, i) for i in range(2)] == pytest.approx([0, 12.5])

