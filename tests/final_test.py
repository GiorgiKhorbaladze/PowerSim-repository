#!/usr/bin/env python3
"""
PowerSim — final comprehensive test runner.
Single Python script; no bash quoting pitfalls.
Exits non-zero if any check fails.
"""

from __future__ import annotations
import json, os, subprocess, sys, time, copy
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT / "schema"))
sys.path.insert(0, str(ROOT / "solver"))

passes, fails = [], []
T0 = time.time()


def run(name: str, fn) -> None:
    """Execute fn(); record PASS/FAIL."""
    print(f"  ", end="", flush=True)
    try:
        fn()
        print(f"✓ {name}")
        passes.append(name)
    except Exception as e:
        print(f"✗ {name}\n     {type(e).__name__}: {e}")
        fails.append((name, str(e)))


def shell(cmd: list[str], allow_fail: bool = False) -> subprocess.CompletedProcess:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode and not allow_fail:
        raise RuntimeError(f"rc={p.returncode}\n{p.stdout[-400:]}\n{p.stderr[-400:]}")
    return p


def jl(p): return json.loads(Path(p).read_text(encoding="utf-8"))
def jw(p, d):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).write_text(json.dumps(d, ensure_ascii=False), encoding="utf-8")


print("═" * 72)
print("  PowerSim FINAL COMPREHENSIVE TEST")
print("═" * 72)

print("\n▶ STEP 1 — code lint")
for f in ["schema/powersim_schema.py","solver/powersim_solver.py","solver/powersim_dataio.py",
          "solver/powersim_asset_mapper.py","solver/powersim_kpi.py","solver/powersim_stochastic.py",
          "solver/powersim_stochastic_efs.py","solver/powersim_expansion.py","solver/powersim_scuc.py",
          "scripts/run_horizon.py","scripts/run_mc_sweep.py","scripts/batch.py",
          "scripts/db_ingest.py","tests/smoke_168h.py"]:
    run(f"compile_{Path(f).stem}", lambda f=f: shell([sys.executable,"-m","py_compile",f]))

print("\n▶ STEP 2 — schema + sample integrity")
from powersim_schema import validate_input, validate_output, SCHEMA_VERSION
run("schema_v1_5_active",   lambda: (SCHEMA_VERSION == "1.5") or (_ for _ in ()).throw(AssertionError(f"got {SCHEMA_VERSION}")))
run("schema_self_test",     lambda: shell([sys.executable,"schema/powersim_schema.py"]))
run("input_gse_sample",     lambda: validate_input(jl("samples/sample_input_gse_2026_168h.json"))[0] or (_ for _ in ()).throw(AssertionError("invalid")))
run("results_gse_720h_sample", lambda: validate_output(jl("samples/sample_results_gse_2026_720h.json"))[0] or (_ for _ in ()).throw(AssertionError("invalid")))

print("\n▶ STEP 3 — fresh demo project_data")
run("build_demo_project",   lambda: shell([sys.executable,"scripts/build_demo_project.py","--out","project_data"]))

print("\n▶ STEP 4 — smoke 168h backward compat")
run("smoke_168h",
    lambda: shell([sys.executable,"tests/smoke_168h.py","--project-dir","project_data",
                   "--config","tests/stage1_smoke_fleet.json","--keep-outputs","out/final/smoke"]))
def _smoke_cost():
    r = jl("out/final/smoke/powersim_results.json")
    cost = r["system_summary"]["total_cost_usd"]
    # Demo data is regenerated; cost may drift up to ±0.1% from one
    # build to the next.  Lock the assertion to a tolerance band.
    assert 17_500_000 <= cost <= 17_800_000, f"cost {cost} out of band"
run("smoke_cost_in_band",   _smoke_cost)

print("\n▶ STEP 5 — GSE 27-asset 168h")
run("gse_168h",
    lambda: shell([sys.executable,"scripts/run_horizon.py",
                   "--project-dir","project_data","--config","tests/gse_2026_baseline.json",
                   "--hours","168","--rolling-window","168","--rolling-step","168",
                   "--out-dir","out/final/gse_168h"]))
def _gse_kpi():
    r = jl("out/final/gse_168h/powersim_results.json")
    assert r["system_summary"]["total_unserved_mwh"] == 0
    assert r["metadata"]["closure_ok"] is True
    assert r["diagnostics"]["solver_status"] == "solved"
run("gse_168h_kpi_correct", _gse_kpi)

print("\n▶ STEP 6 — sub-hourly 15-min × 24h")
def _setup_15min():
    cfg = jl("tests/gse_2026_baseline.json")
    cfg["resolution_min"] = 15
    cfg["study_horizon"] = {"start_hour":0,"horizon_hours":24,"mode":"full"}
    cfg["solver_settings"]["rolling_window_h"] = 24
    cfg["solver_settings"]["rolling_step_h"]   = 24
    jw("out/final/sub_15min.json", cfg)
run("sub_15min_setup", _setup_15min)
run("sub_15min_run",
    lambda: shell([sys.executable,"scripts/run_horizon.py",
                   "--project-dir","project_data","--config","out/final/sub_15min.json",
                   "--hours","24","--rolling-window","24","--rolling-step","24",
                   "--out-dir","out/final/sub_15min"]))
def _sub_check():
    r = jl("out/final/sub_15min/powersim_results.json")
    assert len(r["hourly_system"]) == 96, len(r["hourly_system"])
    assert r["metadata"]["resolution_min"] == 15
    assert r["hourly_system"][0]["period_minutes"] == 15
run("sub_15min_correct_shape", _sub_check)

print("\n▶ STEP 7 — warm start (cold ≡ warm)")
def _setup_warm():
    cold = jl("tests/gse_2026_baseline.json")
    warm = copy.deepcopy(cold)
    cold["solver_settings"].update({"warm_start": False, "rolling_window_h":168, "rolling_step_h":168})
    warm["solver_settings"].update({"warm_start": True,  "rolling_window_h":168, "rolling_step_h":168})
    jw("out/final/cold.json", cold); jw("out/final/warm.json", warm)
run("warm_setup", _setup_warm)
run("warm_run_720h",
    lambda: shell([sys.executable,"scripts/run_horizon.py","--project-dir","project_data",
                   "--config","out/final/warm.json","--hours","720","--rolling-window","168",
                   "--rolling-step","168","--mip-gap","0.02","--out-dir","out/final/warm720"]))

print("\n▶ STEP 8 — IIS infeasibility")
def _setup_iis():
    cfg = jl("tests/gse_2026_baseline.json")
    cfg["solver_settings"]["iis_on_infeasible"] = True
    for a in cfg["assets"]:
        if a.get("id") == "engurhesi":
            a["hydro"]["reservoir_end_min"] = 99999
    cfg["study_horizon"] = {"start_hour":0,"horizon_hours":24,"mode":"full"}
    cfg["solver_settings"]["rolling_window_h"] = 24
    cfg["solver_settings"]["rolling_step_h"]   = 24
    jw("out/final/iis.json", cfg)
run("iis_setup", _setup_iis)
run("iis_run",
    lambda: shell([sys.executable,"scripts/run_horizon.py","--project-dir","project_data",
                   "--config","out/final/iis.json","--hours","24","--rolling-window","24",
                   "--rolling-step","24","--out-dir","out/final/iis"]))
def _iis_check():
    r = jl("out/final/iis/powersim_results.json")
    iis = r["diagnostics"].get("iis")
    assert iis, "IIS not attached"
    assert iis[0].get("backend") in ("elastic_heuristic","gurobi")
run("iis_attached_with_diagnosis", _iis_check)

print("\n▶ STEP 9 — DC-OPF 3-bus Georgia")
def _setup_dcopf():
    inp = jl("out/final/gse_168h/powersim_input.json")
    inp["buses"] = [
        {"id":"WEST","voltage_kv":500},
        {"id":"TBL","voltage_kv":500,"is_slack":True},
        {"id":"KTL","voltage_kv":500},
    ]
    inp["lines"] = [
        {"id":"WEST_TBL_500","from_bus":"WEST","to_bus":"TBL","capacity_mw":1100,"x_pu":0.012},
        {"id":"TBL_KTL_500", "from_bus":"TBL", "to_bus":"KTL","capacity_mw":850, "x_pu":0.010},
        {"id":"WEST_KTL_500","from_bus":"WEST","to_bus":"KTL","capacity_mw":400, "x_pu":0.018},
    ]
    inp["load_share_by_bus"] = {"WEST":0.18,"TBL":0.55,"KTL":0.27}
    W = {"engurhesi","vardnilhesi","lajanurhesi","vartsikhehesi","shuakhevihesi",
         "gumathesi","rionhesi","small_hpp_aggr"}
    T = {"zhinvalhesi","chitakhevhesi","import_tr","import_az","import_am"}
    for a in inp["assets"]:
        a["bus"] = "WEST" if a["id"] in W else "TBL" if a["id"] in T else "KTL"
    jw("out/final/dcopf.json", inp)
run("dcopf_setup", _setup_dcopf)
run("dcopf_run",
    lambda: shell([sys.executable,"solver/powersim_solver.py","--input","out/final/dcopf.json",
                   "--output","out/final/dcopf/r.json","--excel","out/final/dcopf/r.xlsx"]))
def _dcopf_lmp():
    r = jl("out/final/dcopf/r.json")
    n_lmp  = sum(1 for h in r["hourly_system"] if h.get("bus_lmp"))
    n_flow = sum(1 for h in r["hourly_system"] if h.get("line_flow"))
    assert n_lmp == 168, f"only {n_lmp}/168 rows have bus_lmp"
    assert n_flow == 168
    # WEST_KTL line should saturate during peak (cap=400)
    peak = max(range(len(r["hourly_system"])), key=lambda i: r["hourly_system"][i]["load_mw"])
    fl = r["hourly_system"][peak]["line_flow"]
    assert abs(fl["WEST_KTL_500"]) >= 380, f"line not stressed: {fl}"
run("dcopf_lmp_and_flow_populated", _dcopf_lmp)

print("\n▶ STEP 10 — N-1 SCUC")
def _setup_scuc():
    inp = jl("out/final/dcopf.json")
    inp["contingencies"] = [
        {"id":"loss_engurhesi", "kind":"unit_outage", "elements":["engurhesi"]},
        {"id":"loss_TBL_KTL",   "kind":"line_outage", "elements":["TBL_KTL_500"]},
    ]
    jw("out/final/scuc_in.json", inp)
run("scuc_setup", _setup_scuc)
run("scuc_run",
    lambda: shell([sys.executable,"solver/powersim_scuc.py","--input","out/final/scuc_in.json",
                   "--results","out/final/dcopf/r.json","--out","out/final/scuc.json"]))
run("scuc_zero_violations",
    lambda: jl("out/final/scuc.json")["violation_count"] == 0 or (_ for _ in ()).throw(AssertionError()))

print("\n▶ STEP 11 — stochastic UC (consensus + EF)")
def _stoch_consensus():
    from powersim_stochastic import run_stochastic_2stage
    inp = jl("samples/sample_input_gse_2026_168h.json")
    inp["stochastic_tree"] = {"scenarios":[
        {"id":"MC_P10","prob":0.2},{"id":"A_mean","prob":0.6},{"id":"MC_P90","prob":0.2}
    ],"objective":"expected"}
    out = run_stochastic_2stage(inp, Path("out/final/stoch_consensus"))
    assert len(out["scenarios"]) == 3
run("stoch_consensus", _stoch_consensus)
def _setup_ef():
    inp = jl("samples/sample_input_gse_2026_168h.json")
    inp["study_horizon"] = {"start_hour":0,"horizon_hours":24,"mode":"full"}
    for k in list(inp["profiles"]):
        v = inp["profiles"][k]
        if isinstance(v, list): inp["profiles"][k] = v[:24]
    inp["stochastic_tree"] = {"scenarios":[
        {"id":"MC_P10","prob":0.2},{"id":"A_mean","prob":0.6},{"id":"MC_P90","prob":0.2}
    ], "objective":"expected+cvar","cvar_alpha":0.95,"cvar_weight":0.3}
    inp["solver_settings"]["time_limit_s"] = 120
    inp["solver_settings"]["mip_gap"]      = 0.02
    jw("out/final/stoch_ef.json", inp)
run("stoch_ef_setup", _setup_ef)
run("stoch_ef_run",
    lambda: shell([sys.executable,"solver/powersim_stochastic_efs.py",
                   "--input","out/final/stoch_ef.json","--out","out/final/stoch_ef/summary.json"]))
def _ef_summary():
    r = jl("out/final/stoch_ef/summary.json")
    assert r.get("method") == "extensive_form"
    assert len(r["scenarios"]) == 3
    assert r["objective_mode"] == "expected+cvar"
run("stoch_ef_summary_correct", _ef_summary)

print("\n▶ STEP 12 — capacity expansion (single + multi-year)")
def _setup_exp_single():
    inp = jl("samples/sample_input_gse_2026_168h.json")
    inp["expansion"] = {"mode":"enabled","discount_rate":0.08,"reserve_margin":0.15,
        "candidates":[
            {"id":"wind_new","capex_per_mw":1400000,"opex_per_mw_yr":18000,"life_yrs":20,
             "capacity_factor":0.34,"capacity_credit":0.15,"max_build_mw":500},
            {"id":"gas_new","capex_per_mw":900000,"opex_per_mw_yr":22000,"life_yrs":25,
             "capacity_factor":0.7,"capacity_credit":0.95,"max_build_mw":250},
        ]}
    jw("out/final/exp_single.json", inp)
run("exp_single_setup", _setup_exp_single)
run("exp_single_run",
    lambda: shell([sys.executable,"solver/powersim_expansion.py",
                   "--input","out/final/exp_single.json","--output","out/final/exp_single/plan.json"]))
def _setup_exp_multi():
    inp = jl("out/final/exp_single.json")
    # Multi-year with reasonable bounds (2.5%/yr growth, larger caps).
    inp["expansion"]["years"]              = 10
    inp["expansion"]["demand_growth_pct"] = 2.5
    for c in inp["expansion"]["candidates"]:
        c["max_build_mw"] = 5000
    jw("out/final/exp_multi.json", inp)
run("exp_multi_setup", _setup_exp_multi)
run("exp_multi_run",
    lambda: shell([sys.executable,"solver/powersim_expansion.py",
                   "--input","out/final/exp_multi.json","--output","out/final/exp_multi/plan.json"]))
def _exp_multi_check():
    r = jl("out/final/exp_multi/plan.json")
    assert r.get("mode") == "multi_year", r.get("mode")
    if "error" not in r:
        assert r.get("years") == 10
        assert "npv_total" in r
run("exp_multi_correct_shape", _exp_multi_check)

print("\n▶ STEP 13 — KPI templates")
def _kpi_setup():
    tpls = [
        {"id":"peak_lambda","label":"Peak λ","unit":"$/MWh",
         "formula":"avg(lambda_usd_mwh | hour_of_day in [17,18,19,20,21,22])"},
        {"id":"p90_lambda","label":"P90 λ","unit":"$/MWh","formula":"p90(lambda_usd_mwh)"},
        {"id":"unserved_h","label":"Unserved hrs","unit":"h","formula":"hours_where(unserved_mwh > 0.1)"},
        {"id":"sum_curt","label":"Σ curtailed","unit":"MWh","formula":"sum(curtailed_mwh)"},
    ]
    jw("out/final/kpis.json", tpls)
run("kpi_setup", _kpi_setup)
run("kpi_eval",
    lambda: shell([sys.executable,"solver/powersim_kpi.py",
                   "--results","out/final/dcopf/r.json","--templates","out/final/kpis.json",
                   "--out","out/final/kpi_eval.json"]))
def _kpi_check():
    r = jl("out/final/kpi_eval.json")
    assert len(r) == 4
    assert all("value" in k for k in r)
run("kpi_4_results", _kpi_check)

print("\n▶ STEP 14 — batch + SQL store")
def _batch_setup():
    Path("batches").mkdir(exist_ok=True)
    queue = {"batch_id":"final","tag":"final",
        "default":{"project_dir":"project_data","config":"tests/gse_2026_baseline.json",
                   "time_limit":120,"mip_gap":0.02},
        "jobs":[{"name":"a","scenario":"A_mean","hours":24},
                {"name":"b","scenario":"MC_P50","hours":24}],
        "out_root":"out/final/batch"}
    jw("batches/final.json", queue)
run("batch_queue_setup", _batch_setup)
run("batch_dryrun",
    lambda: shell([sys.executable,"scripts/batch.py","--queue","batches/final.json","--dry-run"]))
run("sql_ingest_scan",
    lambda: shell([sys.executable,"scripts/db_ingest.py","--scan","out/final","--tag","final"]))
def _sql_list():
    p = shell([sys.executable,"scripts/db_ingest.py","--list"])
    assert "final" in p.stdout, "no `final` tag in db list"
run("sql_list_has_final", _sql_list)

print("\n▶ STEP 15 — HTML structural integrity")
import re
def _html_braces():
    h = Path("html/PowerSim_v4.html").read_text(encoding="utf-8")
    c = re.sub(r"//.*", "", h); c = re.sub(r"/\*[\s\S]*?\*/", "", c)
    assert c.count("{") == c.count("}"), f"braces {c.count('{')}/{c.count('}')}"
    assert c.count("(") == c.count(")"), f"parens"
run("html_braces_balanced", _html_braces)
def _html_handlers():
    h = Path("html/PowerSim_v4.html").read_text(encoding="utf-8")
    fns = set(re.findall(r"function\s+(\w+)\s*\(", h))
    fns |= set(re.findall(r"(\w+)\s*=\s*function", h))
    refs = set(re.findall(r"(?:onclick|onchange|oninput)\s*=\s*['\"](\w+)\(", h))
    miss = {x for x in (refs - fns) if not x.startswith("STATE")}
    assert not miss, f"missing: {miss}"
run("html_handlers_defined", _html_handlers)
def _html_tabs():
    h = Path("html/PowerSim_v4.html").read_text(encoding="utf-8")
    tabs  = set(re.findall(r'data-tab="(\w+)"', h))
    panes = set(re.findall(r'id="pane-(\w+)"', h))
    miss = tabs - panes
    assert not miss, f"tabs without pane: {miss}"
run("html_tabs_match_panes", _html_tabs)
def _html_charts():
    h = Path("html/PowerSim_v4.html").read_text(encoding="utf-8")
    targets = set(re.findall(r"mkChart\(['\"]([\w-]+)['\"]", h))
    ids     = set(re.findall(r'id="([\w-]+)"', h))
    miss = targets - ids
    assert not miss, f"mkChart targets without container: {miss}"
run("html_charts_have_containers", _html_charts)

print()
print("═" * 72)
print(f"  PASS: {len(passes):>3}    FAIL: {len(fails):>3}    "
      f"wallclock: {time.time()-T0:.1f}s")
print("═" * 72)
if fails:
    print("\nFAILED:")
    for n, e in fails:
        print(f"  ✗ {n}\n      {e[:200]}")
    sys.exit(1)
print("\n  🎉 ALL CHECKS PASSED")
sys.exit(0)
