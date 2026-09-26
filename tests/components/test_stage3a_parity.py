import importlib.util
from dataclasses import replace
from pathlib import Path

import pytest

from powersim.components import BuildContext, ComponentResult, stage3a_registry
from powersim.components.ports import CurtailmentPort
from powersim.contracts import QAStatus
from powersim.qa import check_component_results

SPEC = importlib.util.spec_from_file_location(
    "legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py"
)
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


@pytest.mark.parametrize(
    "kind,asset,profiles",
    [
        (
            "wind",
            {
                "id": "w",
                "type": "wind",
                "pmax_installed": 100,
                "availability_profile": "p",
                "_degradation_factor": 0.9,
            },
            {"p": [0.3]},
        ),
        (
            "solar",
            {
                "id": "s",
                "type": "solar",
                "pmax_installed": 80,
                "availability_profile": "p",
                "_degradation_factor": 1,
            },
            {"p": [0.5]},
        ),
        (
            "hydro_ror",
            {"id": "r", "type": "hydro_ror", "pmax": 30, "availability_profile": "p"},
            {"p": [0.75]},
        ),
        ("import", {"id": "i", "type": "import", "pmax_profile": "p"}, {"p": [22]}),
    ],
)
def test_shared_availability_matches_legacy(kind, asset, profiles):
    component = stage3a_registry().create(kind, asset)
    context = BuildContext(None, (1,), 0.25, profiles=profiles)
    assert component.available_mw(context, 0) == pytest.approx(
        legacy.get_pmax_t(asset, 0, profiles, 0, 0.25), abs=1e-10
    )


@pytest.mark.parametrize("duration", [1.0, 0.25])
@pytest.mark.parametrize(
    "kind,asset,profiles",
    [
        (
            "wind",
            {
                "id": "w",
                "type": "wind",
                "pmax_installed": 100,
                "availability_profile": "cf",
                "_degradation_factor": 0.9,
                "wake_loss_frac": 0.1,
                "monthly_availability_factor": [0.8] + [1.0] * 11,
                "air_density_correction": True,
                "density_ref_temp_c": 15,
                "temp_profile_key": "temp",
                "temp_derating_curve": [[0, 1.0], [40, 0.8]],
                "maint_windows": [
                    {"start_h": 0, "end_h": 1, "availability_factor": 0.5}
                ],
            },
            {"cf": [0.6], "temp": [20.0]},
        ),
        (
            "solar",
            {
                "id": "s",
                "type": "solar",
                "pmax_installed": 100,
                "availability_profile": "cf",
                "dc_ac_ratio": 1.2,
                "inverter_efficiency": 0.98,
                "_degradation_factor": 0.95,
                "temp_profile_key": "temp",
                "temp_derating_curve": [[0, 1.0], [40, 0.8]],
                "maint_windows": [
                    {"start_h": 0, "end_h": 1, "availability_factor": 0.5}
                ],
            },
            {"cf": [0.8], "temp": [25.0]},
        ),
        (
            "hydro_ror",
            {
                "id": "r",
                "type": "hydro_ror",
                "pmax": 40,
                "availability_profile": "cf",
                "maint_windows": [
                    {"start_h": 0, "end_h": 1, "availability_factor": 0.5}
                ],
            },
            {"cf": [2.0]},
        ),
        (
            "import",
            {
                "id": "i",
                "type": "import",
                "pmax_profile": "border",
                "pmax": 50,
                "maint_windows": [
                    {"start_h": 0, "end_h": 1, "availability_factor": 0.5}
                ],
            },
            {"border": [30.0]},
        ),
    ],
)
def test_complex_availability_and_derating_are_applied_exactly_once(
    duration, kind, asset, profiles
):
    component = stage3a_registry().create(kind, asset)
    context = BuildContext(None, (1,), duration, profiles=profiles)
    shared = component.available_mw(context, 0)
    old = legacy.get_pmax_t(asset, 0, profiles, 0, duration)
    assert shared == pytest.approx(old, abs=1e-10)


def test_ror_water_value_cost_is_derived_by_legacy_asset_map_and_preserved():
    raw = {
        "assets": [
            {
                "id": "r",
                "type": "hydro_ror",
                "pmax": 40,
                "vom": 2,
                "hydro": {"water_value": 17},
            }
        ]
    }
    asset = legacy.build_asset_map(raw)["r"]
    component = stage3a_registry().create("hydro_ror", asset)
    assert asset["_dispMC"] == 17
    assert component.marginal_cost() == 19


@pytest.mark.parametrize("duration", [1.0, 0.25])
@pytest.mark.parametrize(
    "kind,asset",
    [
        ("wind", {"id": "a", "type": "wind", "pmax": 20, "_dispMC": 3, "vom": 1}),
        ("solar", {"id": "a", "type": "solar", "pmax": 20, "_dispMC": 4, "vom": 2}),
        (
            "hydro_ror",
            {"id": "a", "type": "hydro_ror", "pmax": 20, "_dispMC": 17, "vom": 2},
        ),
        ("import", {"id": "a", "type": "import", "pmax": 20, "_dispMC": 25, "vom": 1}),
    ],
)
def test_shared_cost_terms_match_legacy_objective_contribution(duration, kind, asset):
    pyo = pytest.importorskip("pyomo.environ")
    model = pyo.ConcreteModel()
    model.p = pyo.Var(["a"], [1])
    model.p["a", 1].fix(7.5)
    component = stage3a_registry().create(kind, asset)
    context = BuildContext(model, (1,), duration)
    term = component.objective_terms(context)[0]
    shared_cost = pyo.value(term.expression)
    legacy_cost = (asset["_dispMC"] + asset.get("vom", 0)) * 7.5 * duration
    assert shared_cost == pytest.approx(legacy_cost, abs=1e-10)


def test_curtailment_port_represents_available_minus_dispatch_not_availability():
    pyo = pytest.importorskip("pyomo.environ")
    model = pyo.ConcreteModel()
    model.p = pyo.Var(["w"], [1])
    model.p["w", 1].fix(4)
    component = stage3a_registry().create(
        "wind",
        {
            "id": "w",
            "type": "wind",
            "pmax_installed": 10,
            "availability_profile": "cf",
        },
    )
    context = BuildContext(model, (1,), 1.0, profiles={"cf": [1.0]})
    component.add_constraints(context)
    port = next(port for port in context.power_ports if isinstance(port, CurtailmentPort))
    assert pyo.value(port.values[1]) == pytest.approx(6.0)


def test_canonical_result_qa_and_curtailment_energy_uses_canonical_contract():
    result = ComponentResult(
        "w",
        "wind",
        1,
        available_mw=10,
        injection_mw=6,
        curtailed_mw=4,
        curtailed_mwh=1,
    )
    checks = check_component_results([result], 0.25)
    assert checks
    assert all(check.status == QAStatus.PASS for check in checks)


def test_legacy_default_and_shared_mixed_fleet_objective_dispatch_parity():
    assets = {
        "thermal": {
            "id": "thermal",
            "type": "thermal",
            "pmax": 100,
            "pmin": 0,
            "_committable": False,
            "_dispMC": 30,
            "_gas_rate": 0,
        },
        "w": {
            "id": "w",
            "type": "wind",
            "pmax_installed": 10,
            "availability_profile": "wind",
            "_committable": False,
            "_dispMC": 0,
            "_gas_rate": 0,
        },
        "s": {
            "id": "s",
            "type": "solar",
            "pmax_installed": 10,
            "availability_profile": "solar",
            "_committable": False,
            "_dispMC": 0,
            "_gas_rate": 0,
        },
        "r": {
            "id": "r",
            "type": "hydro_ror",
            "pmax": 10,
            "availability_profile": "ror",
            "_committable": False,
            "_dispMC": 5,
            "_gas_rate": 0,
        },
        "i": {
            "id": "i",
            "type": "import",
            "pmax": 10,
            "_committable": False,
            "_dispMC": 20,
            "_gas_rate": 0,
        },
    }
    profiles = {"wind": [0.5], "solar": [0.5], "ror": [0.5]}
    base = {"solver": "highs", "time_limit_s": 30}
    old = legacy.solve_window(assets, [30], profiles, [], {}, {}, base)
    new = legacy.solve_window(
        assets,
        [30],
        profiles,
        [],
        {},
        {},
        {**base, "component_engine": "shared"},
    )
    assert new[3] == pytest.approx(old[3], abs=0.01)
    assert new[0][0]["dispatch"] == old[0][0]["dispatch"]


def _shared_pipeline_input(horizon_hours=1, *, rolling=False):
    settings = {
        "solver": "highs",
        "time_limit_s": 30,
        "component_engine": "shared",
    }
    if rolling:
        settings.update({"rolling_window_h": 3, "rolling_step_h": 2})
    return {
        "assets": [
            {
                "id": "thermal",
                "type": "thermal",
                "committable": False,
                "pmin": 0,
                "pmax": 100,
                "mc": 30,
                "vom": 0,
            },
            {
                "id": "w",
                "type": "wind",
                "pmax_installed": 10,
                "availability_profile": "wind",
                "mc": 0,
                "vom": 0,
            },
        ],
        "profiles": {
            "demand": [8.0] * horizon_hours,
            "wind": [0.5] * horizon_hours,
        },
        "study_horizon": {"start_hour": 0, "horizon_hours": horizon_hours},
        "reserve_products": [],
        "solver_settings": settings,
    }


def test_shared_solve_carries_component_results_into_qa_and_publication_gate():
    inp = _shared_pipeline_input()
    assets = legacy.build_asset_map(inp)
    rows, elapsed, objective = legacy.solve_all(inp, assets, inp["profiles"], {})
    result = legacy.build_result_store(rows, assets, inp, elapsed, objective)

    assert result["component_results"]
    component_checks = [
        check for check in result["qa"]["checks"] if check["check_id"].startswith("component.")
    ]
    assert component_checks
    assert all(check["status"] == "pass" for check in component_checks)
    assert result["publication"]["publishable"] is True

    # Corrupt a canonical component observation without touching the legacy
    # hourly balance. Independent component QA must still fail publication.
    first = rows.component_results[0]
    rows.component_results[0] = replace(
        first,
        injection_mw=(first.available_mw or 0) + 5.0,
        curtailed_mw=0.0,
        curtailed_mwh=0.0,
    )
    invalid = legacy.build_result_store(rows, assets, inp, elapsed, objective)
    assert invalid["qa"]["status"] == "fail"
    assert invalid["publication"]["publishable"] is False
    assert invalid["diagnostics"]["result_validity"] == "invalid"


def test_rolling_shared_extraction_keeps_only_committed_periods():
    inp = _shared_pipeline_input(horizon_hours=4, rolling=True)
    assets = legacy.build_asset_map(inp)
    rows, _, _ = legacy.solve_all(inp, assets, inp["profiles"], {})
    wind_results = [item for item in rows.component_results if item.asset_id == "w"]
    assert [item.period for item in wind_results] == [0, 1, 2, 3]
    assert len(wind_results) == 4
