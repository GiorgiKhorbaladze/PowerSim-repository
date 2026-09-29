"""Stage 5B genuine joint N-1 SCUC acceptance tests."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import pytest

ROOT=Path(__file__).parents[1]
SPEC=importlib.util.spec_from_file_location("security",ROOT/"solver"/"powersim_security_shared.py")
security=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(security)
SCUC=importlib.util.spec_from_file_location("legacy_scuc",ROOT/"solver"/"powersim_scuc.py")
legacy=importlib.util.module_from_spec(SCUC); SCUC.loader.exec_module(legacy)


def _generator_case(resolution_min=60, kind="unit_outage"):
    periods=60//resolution_min
    assets=[
      {"id":"g1","type":"thermal","committable":False,"pmin":0,"pmax":10,"mc":1,"vom":0,"ramp_up":100,"ramp_down":100},
      {"id":"g2","type":"thermal","committable":False,"pmin":0,"pmax":10,"mc":20,"vom":0,"ramp_up":100,"ramp_down":100},
    ]
    if kind=="import_outage":
        assets[0]={"id":"g1","type":"import","committable":False,"pmax":10,"mc":1,"vom":0,"ramp_up":100,"ramp_down":100}
    return {"resolution_min":resolution_min,"assets":assets,"profiles":{"demand":[10.0]*periods},"study_horizon":{"horizon_hours":1},
      "reserve_products":[{"id":"up","direction":"up","requirement":10,"eligible_units":["g2"]}],
      "contingencies":[{"id":"loss","kind":kind,"elements":["g1"]}],"solver_settings":{"solver":"highs","component_engine":"shared"}}


def _line_case():
    return {"assets":[
      {"id":"g1","type":"thermal","committable":False,"pmin":0,"pmax":20,"mc":1,"vom":0,"ramp_up":100,"ramp_down":100,"bus":"a"},
      {"id":"g2","type":"thermal","committable":False,"pmin":0,"pmax":20,"mc":20,"vom":0,"ramp_up":100,"ramp_down":100,"bus":"c"}],
      "profiles":{"demand":[10.0]},"study_horizon":{"horizon_hours":1},
      "reserve_products":[{"id":"up","direction":"up","requirement":8,"eligible_units":["g2"]},{"id":"down","direction":"down","requirement":3,"eligible_units":["g1"]}],
      "buses":[{"id":"a","is_slack":True},{"id":"b"},{"id":"c"}],
      "lines":[{"id":"l1","from_bus":"a","to_bus":"b","susceptance_mw_per_rad":100,"capacity_mw":20},{"id":"l2","from_bus":"b","to_bus":"c","susceptance_mw_per_rad":100,"capacity_mw":20},{"id":"l3","from_bus":"a","to_bus":"c","susceptance_mw_per_rad":100,"capacity_mw":2}],
      "load_share_by_bus":{"a":0,"b":0,"c":1},"contingencies":[{"id":"loss_l1","kind":"line_outage","elements":["l1"]}],"solver_settings":{"solver":"highs","component_engine":"shared"}}


def test_legacy_line_outage_is_explicit_screening_noop_characterization():
    result={"hourly_system":[{"load_mw":10}],"hourly_by_unit":{}}
    report=legacy.verify(_line_case(),result)
    assert report["method"]=="post_solve_verify" and report["violation_count"]==0


@pytest.mark.parametrize("case,match", [
    (lambda: {**_generator_case(),"contingencies":[{"id":"x","kind":"bus_outage","elements":["a"]}]},"unsupported"),
    (lambda: {**_generator_case(),"contingencies":[{"id":"x","kind":"unit_outage","elements":["missing"]}]},"unknown asset"),
    (lambda: {**_generator_case(),"contingencies":[{"id":"x","kind":"unit_outage","elements":["g1","g2"]}]},"exactly one"),
    (lambda: {**_generator_case(),"contingencies":[{"id":"x","kind":"unit_outage","elements":["g1"]},{"id":"x","kind":"unit_outage","elements":["g1"]}]},"unique"),
])
def test_strict_contingency_contract(case,match):
    with pytest.raises(ValueError,match=match): security.run_security_shared(case())


@pytest.mark.parametrize("resolution_min",[60,15])
def test_generator_outage_is_joint_security_feasible_and_reserve_limited(resolution_min):
    out=security.run_security_shared(_generator_case(resolution_min))
    base=out["base"]["hourly_by_unit"]["g2"]; cont=out["contingencies"][0]["result"]["hourly_by_unit"]
    assert out["publication"]["publishable"] is True
    assert cont["g1"][0]["dispatch_mw"]==pytest.approx(0)
    assert cont["g2"][0]["dispatch_mw"]-base[0]["dispatch_mw"] <= base[0]["reserve_up"]["up"]+1e-7
    assert out["qa"]["status"]=="pass"


def test_import_outage_is_physically_applied():
    out=security.run_security_shared(_generator_case(kind="import_outage"))
    assert out["contingencies"][0]["result"]["hourly_by_unit"]["g1"][0]["dispatch_mw"]==pytest.approx(0)
    assert out["publication"]["publishable"] is True


def test_insufficient_corrective_reserve_has_no_publishable_incumbent():
    inp=_generator_case(); inp["assets"][1]["pmax"]=5; inp["reserve_products"][0]["requirement"]=5
    out=security.run_security_shared(inp)
    assert out["has_incumbent"] is False
    assert out["result_validity"]=="invalid" and out["publication"]["publishable"] is False


def test_line_outage_changes_joint_optimization_and_redistributes_flow():
    secured=security.run_security_shared(_line_case())
    base=secured["base"]; contingency=secured["contingencies"][0]["result"]
    assert base["hourly_system"][0]["line_flow"]["l1"] != 0
    assert contingency["hourly_system"][0]["line_flow"]["l1"] == pytest.approx(0)
    assert contingency["hourly_by_unit"]["g2"][0]["dispatch_mw"] > base["hourly_by_unit"]["g2"][0]["dispatch_mw"]
    assert secured["publication"]["publishable"] is True
    assert secured["qa"]["status"]=="pass"


def test_islanding_line_outage_fails_closed():
    inp=_line_case(); inp["lines"]=inp["lines"][:2]
    with pytest.raises(ValueError,match="unsupported_islanding_contingency"): security.run_security_shared(inp)


def test_corrupted_canonical_line_outage_fails_independent_security_qa():
    out=security.run_security_shared(_line_case())
    rows=[dict(row) for row in out["contingencies"][0]["canonical_rows"]]
    rows[0]["line_flow"]=dict(rows[0]["line_flow"]); rows[0]["line_flow"]["l1"]=1.0
    inp=_line_case(); assets=security.build_asset_map(inp); network=security.normalize_dc_network(inp,assets)
    report=security._qa(out["base"]["canonical_rows"],{"loss_l1":rows},[{"id":"loss_l1","kind":"line_outage","target":"l1"}],network)
    assert next(c for c in report.checks if c.check_id=="security.outage_applied").status.value=="fail"
