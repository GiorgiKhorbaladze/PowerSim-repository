import importlib.util
from pathlib import Path

import pytest
from dataclasses import replace

from powersim.qa import check_component_results


SPEC = importlib.util.spec_from_file_location("legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py")
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


def _input(shared: bool):
    return {
        "assets": [
            {"id": "g", "type": "thermal", "committable": False, "pmin": 0, "pmax": 100, "mc": 100, "vom": 0},
            {"id": "b", "type": "bess", "committable": False, "power_mw": 30, "energy_mwh": 60,
             "soc_init": 0.5, "soc_min": 0.1, "soc_max": 0.9, "eta_charge": 0.9, "eta_discharge": 0.9,
             "vom_discharge": 2, "cycle_cost_per_mwh": 1, "ramp_up_mw_min": 1, "ramp_down_mw_min": 1},
        ],
        "profiles": {"demand": [0, 0, 30, 60]},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4},
        "reserve_products": [],
        "solver_settings": {"solver": "highs", "time_limit_s": 30, "component_engine": "shared" if shared else "legacy"},
    }


def _solve(shared: bool):
    inp = _input(shared)
    return legacy.solve_all(inp, legacy.build_asset_map(inp), inp["profiles"], {})


def test_shared_bess_matches_legacy_dispatch_and_objective():
    old_rows, _, old_objective = _solve(False)
    new_rows, _, new_objective = _solve(True)
    assert new_objective == pytest.approx(old_objective, abs=0.01)
    assert [row["bess"] for row in new_rows] == [row["bess"] for row in old_rows]


def test_shared_bess_extracts_canonical_state_and_flows():
    rows, _, _ = _solve(True)
    results = [item for item in rows.component_results if item.component_kind == "bess"]
    assert len(results) == 4
    assert all(item.state_of_charge_mwh is not None for item in results)
    assert all(item.injection_mw >= 0 and item.withdrawal_mw >= 0 for item in results)
    assert all(not (item.injection_mw > 1e-7 and item.withdrawal_mw > 1e-7) for item in results)
    checks = check_component_results(results, 1.0, resolved_input=_input(True))
    assert all(check.status.value == "pass" for check in checks)
    corrupt = list(results)
    corrupt[0] = replace(corrupt[0], injection_mw=99)
    checks = check_component_results(corrupt, 1.0, resolved_input=_input(True))
    assert next(check for check in checks if check.check_id == "component.bess_bounds").status.value == "fail"

def test_bess_rolling_boundary_state_is_canonical_and_independently_checked():
    i=_input(True); i["solver_settings"].update({"rolling_window_h":2,"rolling_step_h":2})
    rows,_,_= _solve_input(i)
    results=[item for item in rows.component_results if item.component_kind=="bess"]
    assert len(results)==4
    assert results[2].previous_state_of_charge_mwh == pytest.approx(results[1].state_of_charge_mwh)
    assert results[2].previous_withdrawal_mw == pytest.approx(results[1].withdrawal_mw)
    assert results[2].previous_injection_mw == pytest.approx(results[1].injection_mw)
    assert all(check.status.value=="pass" for check in check_component_results(results,1,resolved_input=i))

def test_bess_advanced_legacy_extensions_remain_shared_legacy_parity_seams():
    legacy_input=_input(False)
    legacy_input["assets"][1].update({"depth_multiplier":1.8,"soc_deep_threshold":.2,"soc_end_target":.7,"soc_end_penalty_usd_mwh":1000})
    shared_input={**legacy_input,"solver_settings":{**legacy_input["solver_settings"],"component_engine":"shared"}}
    old,_,old_objective=_solve_input(legacy_input); new,_,new_objective=_solve_input(shared_input)
    assert new_objective == pytest.approx(old_objective,abs=.01)
    assert [row["bess"] for row in new] == [row["bess"] for row in old]

@pytest.mark.parametrize("resolution_min", [60, 15])
def test_bess_canonical_qa_detects_soc_mode_power_and_ramp_corruption(resolution_min):
    i=_input(True); i["resolution_min"]=resolution_min
    factor=60//resolution_min; i["profiles"]["demand"]=[v for v in i["profiles"]["demand"] for _ in range(factor)]
    i["study_horizon"]["horizon_hours"]=4
    rows,_,_=_solve_input(i)
    results=[item for item in rows.component_results if item.component_kind=="bess"]
    cases={
        "component.bess_soc_recurrence": replace(results[0],state_of_charge_mwh=(results[0].state_of_charge_mwh or 0)+5),
        "component.bess_mode": replace(results[0],injection_mw=1,withdrawal_mw=1),
        "component.bess_bounds": replace(results[0],injection_mw=999),
        "component.bess_charge_ramp_up": replace(results[0],withdrawal_mw=999,previous_withdrawal_mw=0),
    }
    for check_id, corrupt in cases.items():
        altered=[corrupt,*results[1:]]
        checks=check_component_results(altered,resolution_min/60,resolved_input=i)
        assert next(check for check in checks if check.check_id==check_id).status.value=="fail"

def test_each_bess_mandatory_invariant_blocks_the_production_publication_gate():
    i=_input(True); assets=legacy.build_asset_map(i); rows,elapsed,obj=legacy.solve_all(i,assets,i["profiles"],{})
    results=list(rows.component_results); index=next(n for n,x in enumerate(results) if x.component_kind=="bess")
    base=results[index]
    corruptions=(
        replace(base,state_of_charge_mwh=(base.state_of_charge_mwh or 0)+5),
        replace(base,injection_mw=1,withdrawal_mw=1),
        replace(base,injection_mw=999),
        replace(base,withdrawal_mw=999,previous_withdrawal_mw=0),
    )
    for corrupted_row in corruptions:
        altered=list(results); altered[index]=corrupted_row
        corrupted=legacy.SolvedRows(rows,solver_diagnostics=rows.solver_diagnostics,window_diagnostics=rows.window_diagnostics,component_results=altered,extraction_completed=True)
        result=legacy.build_result_store(corrupted,assets,i,elapsed,obj)
        assert result["qa"]["status"]=="fail"
        assert result["diagnostics"]["result_validity"]=="invalid"
        assert result["publication"]["publishable"] is False

def _solve_input(inp):
    return legacy.solve_all(inp,legacy.build_asset_map(inp),inp["profiles"],{})
