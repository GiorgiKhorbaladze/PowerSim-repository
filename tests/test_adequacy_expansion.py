from __future__ import annotations
import json, tempfile, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
for p in (ROOT/'solver', ROOT):
    if str(p) not in sys.path: sys.path.insert(0,str(p))
from powersim_adequacy import plan_adequacy_expansion, build_assets_from_expansion_plan

def inp(max_build=100):
    return {'profiles': {'demand':[120,120,120]}, 'assets':[{'id':'t','type':'thermal','pmax':100}], 'adequacy': {'lole_target_h':0,'eens_target_mwh':0,'reserve_margin_target_pct':0,'required_storage_duration_h':4}, 'expansion': {'mode':'adequacy_screening','candidates':[{'id':'bess4','type':'bess','max_build_mw':max_build,'block_mw':25,'duration_h':4,'capex_per_mw':1,'fixed_om_per_mw_yr':0,'capacity_credit':1.0}]}}

def test_expansion_closes_gap():
    p=plan_adequacy_expansion(inp())
    assert not p['infeasible']
    assert p['recommended_builds']['bess4']['mw'] >= 25
    assert p['post_expansion_adequacy']['adequacy_pass']

def test_expansion_infeasible():
    p=plan_adequacy_expansion(inp(10))
    assert p['infeasible'] is True
    assert p['binding_targets']

def test_handoff_and_cli():
    p=plan_adequacy_expansion(inp())
    assets,w=build_assets_from_expansion_plan(inp()['assets'], p, inp()['expansion']['candidates'])
    assert any(a['id']=='adequacy_bess4' and a['type']=='bess' for a in assets)
    with tempfile.TemporaryDirectory() as td:
        f=Path(td)/'input.json'; out=Path(td)/'out'; f.write_text(json.dumps(inp()), encoding='utf-8')
        subprocess.check_call([sys.executable, str(ROOT/'scripts/run_adequacy.py'), '--input', str(f), '--out-dir', str(out), '--mode', 'deterministic_derated', '--write-expanded-input'])
        expanded=json.loads((out/'expanded_input.json').read_text(encoding='utf-8'))
        assert any(a['id']=='adequacy_bess4' for a in expanded['assets'])

if __name__ == '__main__':
    test_expansion_closes_gap(); test_expansion_infeasible(); test_handoff_and_cli(); print('adequacy expansion tests passed')
