"""BESS sizing LP for Georgia 2030.

Chronological 8760-hour capacity co-optimization for BESS power (MW) and
energy (MWh).  Inputs are MW hourly series; outputs report MWh/GWh metrics.
"""
from __future__ import annotations

import argparse, csv, io, json, math, zipfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable

import pyomo.environ as pyo

DEFAULT_REG_ENERGY_GWH = [296, 276, 491, 429, 830, 890, 1050, 766, 498, 367, 319, 367]
MONTH_HOURS_2030 = [744,672,744,720,744,720,744,744,720,744,720,744]

@dataclass
class BessSizingParams:
    mode: str = "reliability"  # reliability | economic | fixed
    c_p_usd_mw_yr: float = 90000.0
    c_e_usd_mwh_yr: float = 35000.0
    eta_c: float = 0.95
    eta_d: float = 0.95
    voll_usd_mwh: float = 3000.0
    vom_usd_mwh: float = 1.0
    export_value_usd_mwh: float = 0.0
    eens_target_pct: float = 0.10
    dur_min_h: float = 1.0
    dur_max_h: float = 8.0
    export_limit_mw: float = 800.0
    reg_energy_gwh: tuple[float, ...] = tuple(DEFAULT_REG_ENERGY_GWH)
    reg_pmax_mw: tuple[float, ...] | None = None
    fixed_p_mw: float | None = None
    fixed_e_mwh: float | None = None


def _read_csv_text(text: str) -> list[float]:
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=';,\t,')
    rows = csv.DictReader(io.StringIO(text), dialect=dialect)
    vals = []
    for row in rows:
        raw = row.get('Value') or row.get('value') or list(row.values())[-1]
        vals.append(float(str(raw).strip().replace(' ', '').replace(',', '.')))
    return vals

class PlexosZipLoader:
    """Loads available-generation profiles from the provided PLEXOS zip.

    Uses original profile CSVs (PV * Average, Wind * Average, RoR * Medium), not
    `Model BEST Solution/...Generation.csv` dispatch results.
    """
    def __init__(self, zip_path: str | Path, root: str = 'plexos model/BESS Research 800-1400/'):
        self.zip_path = Path(zip_path); self.root = root
        self.zf = zipfile.ZipFile(self.zip_path)
        self.names = self.zf.namelist()

    def _series(self, suffix: str) -> list[float]:
        name = self.root + suffix
        return _read_csv_text(self.zf.read(name).decode('utf-8-sig', errors='replace'))

    def _sum_matching(self, prefixes: Iterable[str], scenario_words: Iterable[str]) -> list[float]:
        picks = [n for n in self.names if n.startswith(self.root) and n.endswith('.csv')
                 and any(Path(n).name.startswith(p) for p in prefixes)
                 and any(w in Path(n).stem for w in scenario_words)]
        if not picks:
            return [0.0]*8760
        acc = None
        for n in picks:
            s = _read_csv_text(self.zf.read(n).decode('utf-8-sig', errors='replace'))
            acc = s if acc is None else [a+b for a,b in zip(acc, s)]
        return acc or [0.0]*8760

    def load(self) -> dict[str, list[float]]:
        data = {
            'load': self._series('Load.csv'),
            'export': self._series('Export.csv'),
            'pv': self._sum_matching(['PV '], ['Average']),
            'wind': self._sum_matching(['Wind '], ['Average']),
            'ror': self._sum_matching(['RoR '], ['Medium']),
            'tpp': self._sum_matching(['TPP', 'IMP'], ['']),
        }
        for k, v in data.items():
            if len(v) != 8760:
                raise ValueError(f'{k} has {len(v)} hours, expected 8760')
        return data


def solve_bess_sizing(data: dict[str, list[float]], params: BessSizingParams) -> dict:
    if params.reg_pmax_mw is None:
        raise ValueError('Regulated hydro monthly Pmax is required; refusing to assume arbitrary RegPmax.')
    T = range(len(data['load']))
    if len(data['load']) != 8760: raise ValueError('BESS sizing requires a full 8760-hour chronological year')
    month_of_t=[]
    for m,h in enumerate(MONTH_HOURS_2030): month_of_t += [m]*h
    D=data['load']; X=[min(x, params.export_limit_mw) for x in data['export']]
    avail=[data.get(k,[0]*8760) for k in ('pv','wind','ror','tpp')]
    must=[sum(vals[t] for vals in avail) for t in T]

    m=pyo.ConcreteModel(); m.T=pyo.RangeSet(0,8759); m.M=pyo.RangeSet(0,11)
    m.P=pyo.Var(domain=pyo.NonNegativeReals); m.E=pyo.Var(domain=pyo.NonNegativeReals)
    m.ch=pyo.Var(m.T, domain=pyo.NonNegativeReals); m.dis=pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.soc=pyo.Var(m.T, domain=pyo.NonNegativeReals); m.reg=pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.exp=pyo.Var(m.T, domain=pyo.NonNegativeReals); m.ens=pyo.Var(m.T, domain=pyo.NonNegativeReals); m.spill=pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.balance=pyo.Constraint(m.T, rule=lambda mm,t: must[t]+mm.reg[t]+mm.dis[t]-mm.ch[t] == D[t]+mm.exp[t]-mm.ens[t]+mm.spill[t])
    m.export_cap=pyo.Constraint(m.T, rule=lambda mm,t: mm.exp[t] <= X[t])
    m.ch_cap=pyo.Constraint(m.T, rule=lambda mm,t: mm.ch[t] <= mm.P); m.dis_cap=pyo.Constraint(m.T, rule=lambda mm,t: mm.dis[t] <= mm.P)
    m.soc_cap=pyo.Constraint(m.T, rule=lambda mm,t: mm.soc[t] <= mm.E)
    m.soc_dyn=pyo.Constraint(m.T, rule=lambda mm,t: mm.soc[t] == mm.soc[8759 if t==0 else t-1] + params.eta_c*mm.ch[t] - mm.dis[t]/params.eta_d)
    m.dur_min=pyo.Constraint(expr=m.E >= params.dur_min_h*m.P); m.dur_max=pyo.Constraint(expr=m.E <= params.dur_max_h*m.P)
    m.reg_pmax=pyo.Constraint(m.T, rule=lambda mm,t: mm.reg[t] <= params.reg_pmax_mw[month_of_t[t]])
    m.reg_energy=pyo.Constraint(m.M, rule=lambda mm,mo: sum(mm.reg[t] for t in T if month_of_t[t]==mo) <= params.reg_energy_gwh[mo]*1000.0)
    if params.mode == 'fixed':
        m.fixp=pyo.Constraint(expr=m.P == float(params.fixed_p_mw or 0)); m.fixe=pyo.Constraint(expr=m.E == float(params.fixed_e_mwh or 0))
    if params.mode == 'reliability':
        m.eens_target=pyo.Constraint(expr=sum(m.ens[t] for t in m.T) <= params.eens_target_pct/100.0*sum(D))
        obj = params.c_p_usd_mw_yr*m.P + params.c_e_usd_mwh_yr*m.E + 1e-3*sum(m.ch[t]+m.dis[t]+m.spill[t]+(X[t]-m.exp[t]) for t in m.T)
    else:
        obj = params.c_p_usd_mw_yr*m.P + params.c_e_usd_mwh_yr*m.E + sum(params.vom_usd_mwh*m.dis[t]+params.voll_usd_mwh*m.ens[t]-params.export_value_usd_mwh*m.exp[t] for t in m.T)
    m.obj=pyo.Objective(expr=obj, sense=pyo.minimize)
    solver = pyo.SolverFactory('appsi_highs')
    res = solver.solve(m)
    status = str(res.solver.termination_condition)
    if status.lower() not in ('optimal','locallyoptimal'):
        raise RuntimeError(f'HiGHS did not find optimal solution: {status}')
    vals=lambda v: [pyo.value(v[t]) for t in m.T]
    ens=vals(m.ens); spill=vals(m.spill); exp=vals(m.exp); ch=vals(m.ch); dis=vals(m.dis); soc=vals(m.soc)
    p=float(pyo.value(m.P)); e=float(pyo.value(m.E)); ens_mwh=sum(ens)
    return {'status':'Optimal','pbess_mw':p,'ebess_mwh':e,'duration_h': e/p if p>1e-9 else 0.0,
            'eens_gwh':ens_mwh/1000,'eens_pct':100*ens_mwh/sum(D),'lolh_h':sum(1 for x in ens if x>1e-6),
            'export_ens_gwh':sum(max(0, data['export'][t]-exp[t]) for t in T)/1000,
            'curtailment_gwh':sum(spill)/1000,'cycles_per_year':sum(dis)/(e or math.inf),
            'annualized_capex_usd':params.c_p_usd_mw_yr*p+params.c_e_usd_mwh_yr*e,
            'energy_closure_pct':100*(sum(must)+sum(pyo.value(m.reg[t]) for t in m.T)-sum(spill)-(sum(D)-ens_mwh+sum(exp)))/(sum(D) or 1),
            'series': {'charge_mw':ch,'discharge_mw':dis,'soc_mwh':soc,'ens_mw':ens,'spill_mw':spill,'export_mw':exp}}


def main():
    ap=argparse.ArgumentParser(description='Georgia 2030 BESS sizing LP')
    ap.add_argument('--plexos-zip', default='plexos model.zip'); ap.add_argument('--mode', choices=['reliability','economic','fixed'], default='reliability')
    ap.add_argument('--reg-pmax-mw', required=True, help='Comma-separated 12 monthly regulated hydro Pmax values in MW')
    ap.add_argument('--fixed-p-mw', type=float); ap.add_argument('--fixed-e-mwh', type=float); ap.add_argument('--out', default='out/bess_sizing_results.json')
    args=ap.parse_args(); pmax=tuple(float(x) for x in args.reg_pmax_mw.split(','))
    if len(pmax)!=12: raise SystemExit('--reg-pmax-mw must contain 12 values')
    params=BessSizingParams(mode=args.mode, reg_pmax_mw=pmax, fixed_p_mw=args.fixed_p_mw, fixed_e_mwh=args.fixed_e_mwh)
    out=solve_bess_sizing(PlexosZipLoader(args.plexos_zip).load(), params)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True); Path(args.out).write_text(json.dumps({'params':asdict(params),'results':out}, indent=2), encoding='utf-8')
    print(json.dumps({k:v for k,v in out.items() if k!='series'}, indent=2))
if __name__ == '__main__': main()
