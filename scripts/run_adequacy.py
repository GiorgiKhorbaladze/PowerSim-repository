#!/usr/bin/env python3
from __future__ import annotations
import argparse, copy, json, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT/'solver', ROOT/'schema'):
    if str(p) not in sys.path: sys.path.insert(0,str(p))
from powersim_adequacy import run_adequacy, plan_adequacy_expansion, build_assets_from_expansion_plan, write_json

def main(argv=None):
    ap=argparse.ArgumentParser(description='Run PowerSim Stage 1 adequacy screening')
    ap.add_argument('--input', required=True); ap.add_argument('--out-dir', default='out/adequacy')
    ap.add_argument('--mode', choices=['deterministic_derated','monte_carlo'], default=None)
    ap.add_argument('--samples', type=int, default=None); ap.add_argument('--seed', type=int, default=None)
    ap.add_argument('--write-expanded-input', action='store_true')
    ns=ap.parse_args(argv)
    inp=json.loads(Path(ns.input).read_text(encoding='utf-8'))
    if not isinstance(inp.get('assets'), list) or not (inp.get('profiles') or {}).get('demand'):
        raise SystemExit('input must include assets[] and profiles.demand[]')
    out=Path(ns.out_dir); out.mkdir(parents=True, exist_ok=True)
    summary=run_adequacy(inp, ns.mode, ns.samples, ns.seed); write_json(out/'adequacy_summary.json', summary)
    plan=None
    ex=inp.get('expansion') or {}
    if ex.get('candidates') and ex.get('mode') in (None, 'adequacy_screening'):
        plan=plan_adequacy_expansion(inp, ns.mode, ns.samples, ns.seed); write_json(out/'adequacy_expansion_plan.json', plan)
    if ns.write_expanded_input:
        expanded=copy.deepcopy(inp)
        if plan:
            expanded['assets'], warns = build_assets_from_expansion_plan(inp.get('assets') or [], plan, ex.get('candidates') or [])
            expanded.setdefault('diagnostics', {})['adequacy_expansion_warnings']=warns
        expanded.setdefault('diagnostics', {})['adequacy']=summary
        if (expanded.get('expansion') or {}).get('mode') == 'adequacy_screening':
            expanded.pop('expansion', None)
        write_json(out/'expanded_input.json', expanded)
    print(json.dumps(summary, indent=2))
if __name__=='__main__': main()
