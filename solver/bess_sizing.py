"""BESS sizing LP for Georgia 2030.

Chronological 8760-hour capacity adequacy LP, 1-hour resolution.
Inputs in MW; reports output in GWh.

Key corrections vs. original:
- PV, Wind, RoR profiles are 0-100 % of installed capacity; scaled by capacity_MW/100.
- TPP/Import profile is already in MW (Tpp-Imp.csv).
- Loader refuses to run if any capacity mapping is incomplete.
- Ksani BESS (200 MW / 200 MWh) is treated as already-built; only additional BESS is sized.
- Export ENS never enters internal EENS.
- Spill/curtailment never counted as ENS.
"""
from __future__ import annotations

import argparse, csv, io, json, math, zipfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable

import pyomo.environ as pyo

# ── Calendar ──────────────────────────────────────────────────────────────────
MONTH_HOURS_2030 = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]

# ── Regulated HPP monthly limits (from PLEXOS XML) ────────────────────────────
REG_ENERGY_GWH = (296.0, 276.0, 491.0, 429.0, 830.0, 890.0, 1050.0, 766.0,
                  498.0, 367.0, 319.0, 367.0)
REG_PMAX_MW = (1864.16, 1788.74, 1766.33, 1570.60, 1886.27, 2069.68,
               2123.39, 2121.14, 2091.98, 2066.43, 1980.07, 1954.02)

# ── Ksani reference BESS (already-built 2030) ────────────────────────────────
KSANI_P_MW = 200.0
KSANI_E_MWH = 200.0

# ── Cost structure (Ksani = reference C per MW for 1-hour system) ─────────────
COST_1H_PER_MW_YR = 125_000.0   # C  ($/MW/year)
COST_2H_PER_MW_YR = 225_000.0   # 1.8 × C
COST_4H_PER_MW_YR = 405_000.0   # 3.24 × C
DURATION_COST = {1.0: COST_1H_PER_MW_YR, 2.0: COST_2H_PER_MW_YR, 4.0: COST_4H_PER_MW_YR}

# ── Capacity-profile map (from PLEXOS XML tag table, verified vs. solution) ───
# All profiles are 0-100 % → available_MW = capacity_MW × profile_pct / 100
_ROR_CAP = {
    'RoR Kodori Medium.csv':          21.39,
    'RoR Rioni-Oni Medium.csv':       217.44,
    'RoR Rioni-Alpana Medium.csv':    254.89,
    'RoR Tekhuri Medium.csv':         192.81,
    'RoR Kvirila Medium.csv':          49.67,
    'RoR Supsa Medium.csv':           136.70,
    'RoR Adjaristskali Medium.csv':    54.23,
    'RoR Liakhvi Medium.csv':         165.87,
    'RoR Paravani Medium.csv':        221.01,
    'RoR Tusheti Alazani Medium.csv': 139.28,
    'RoR Mtiuleti Aragvi Medium.csv':  34.21,
    'RoR Alazani Birkiani Medium.csv': 67.44,
    'RoR Mashavera Medium.csv':       150.19,
    'RoR Alazani Shakriani Medium.csv': 27.42,
    'RoR Chorokhi Medium.csv':        100.10,
    'RoR Samgori Medium.csv':          39.41,
}
_WIND_CAP = {
    'Wind Kolkheti Average.csv':    482.20,
    'Wind Likhis Kedi Average.csv': 125.00,
    'Wind Shida Kartli Average.csv': 1171.35,
    'Wind Tbilisi Average.csv':      30.00,
    'Wind Javakheti Average.csv':   100.00,
}
_PV_CAP = {
    'PV Samegrelo Average.csv':          20.00,
    'PV Imereti Average.csv':            90.60,
    'PV Samtskhe-Javakheti Average.csv': 23.92,
    'PV Shida Kartli Average.csv':      113.62,
    'PV Kvemo Kartli Average.csv':     1113.37,
    'PV Tbilisi Average.csv':            30.00,
    'PV Sagarejo Average.csv':           17.60,
    'PV Telavi Average.csv':             48.45,
    'PV Dedoplistskaro Average.csv':     38.50,
}
_TPP_FNAME = 'Tpp-Imp.csv'   # already in MW

# ── QA expected ranges ─────────────────────────────────────────────────────────
_QA_RANGES = {
    'load':   (17_000, 19_500, 'GWh'),
    'export': ( 4_000,  5_000, 'GWh'),
    'tpp':    (   150,    220, 'GWh'),
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CSV reader
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _read_csv_text(text: str) -> list[float]:
    """Parse a semicolon-separated CSV with European decimal comma."""
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=';\t,')
    rows = csv.DictReader(io.StringIO(text), dialect=dialect)
    vals: list[float] = []
    for row in rows:
        raw = row.get('Value') or row.get('value') or list(row.values())[-1]
        vals.append(float(str(raw).strip().replace(' ', '').replace(',', '.')))
    return vals


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Data loader
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PlexosZipLoader:
    """Load 8760-hour profiles from the PLEXOS zip.

    PV / Wind / RoR profile CSVs contain values as 0-100 (% of installed
    capacity).  Available MW = installed_capacity_MW × profile_pct / 100.

    TPP/Import (Tpp-Imp.csv) values are already in MW.

    Raises ValueError if any profile file is missing or the mapping is
    incomplete.
    """

    def __init__(
        self,
        zip_path: str | Path,
        root: str = 'plexos model/BESS Research 800-1400/',
    ) -> None:
        self.zip_path = Path(zip_path)
        self.root = root
        self.zf = zipfile.ZipFile(self.zip_path)
        self._names_set = set(self.zf.namelist())

    def _read(self, fname: str) -> list[float]:
        key = self.root + fname
        if key not in self._names_set:
            raise ValueError(f'Profile file not found in zip: {fname}')
        text = self.zf.read(key).decode('utf-8-sig', errors='replace')
        vals = _read_csv_text(text)
        if len(vals) != 8760:
            raise ValueError(f'{fname} has {len(vals)} rows, expected 8760')
        return vals

    def _scale_sum(self, cap_map: dict[str, float]) -> list[float]:
        """Sum profile × (capacity/100) across all entries in cap_map."""
        total = [0.0] * 8760
        for fname, cap_mw in cap_map.items():
            pct = self._read(fname)
            for t in range(8760):
                total[t] += cap_mw * pct[t] / 100.0
        return total

    # ── public API ────────────────────────────────────────────────────────────

    def load(self, verbose: bool = True) -> dict[str, list[float]]:
        """Return dict with keys: load, export, pv, wind, ror, tpp.

        All values are in MW (hourly).
        Prints a QA table and raises if values are implausible.
        """
        data: dict[str, list[float]] = {
            'load':   self._read('Load.csv'),
            'export': self._read('Export.csv'),
            'pv':     self._scale_sum(_PV_CAP),
            'wind':   self._scale_sum(_WIND_CAP),
            'ror':    self._scale_sum(_ROR_CAP),
            'tpp':    self._read(_TPP_FNAME),
        }

        if verbose:
            self._print_qa(data)
        self._validate_qa(data)
        return data

    def reg_limits(self) -> tuple[tuple[float, ...], tuple[float, ...]]:
        """Return (monthly_energy_GWh, monthly_pmax_MW) for regulated HPP."""
        return REG_ENERGY_GWH, REG_PMAX_MW

    # ── QA ───────────────────────────────────────────────────────────────────

    def _print_qa(self, data: dict[str, list[float]]) -> None:
        print('\n=== Loader QA ===')
        print(f"{'Category':<15} {'GWh':>10} {'Peak MW':>10} {'Min MW':>10}")
        print('-' * 50)
        for k, v in data.items():
            gwh = sum(v) / 1000
            print(f'{k:<15} {gwh:>10.1f} {max(v):>10.1f} {min(v):>10.1f}')
        print('\nRegulated HPP monthly energy limits (GWh):')
        print('  ' + ', '.join(f'{e:.1f}' for e in REG_ENERGY_GWH))
        print('Regulated HPP monthly Pmax (MW):')
        print('  ' + ', '.join(f'{p:.1f}' for p in REG_PMAX_MW))
        print()

    def _validate_qa(self, data: dict[str, list[float]]) -> None:
        errs: list[str] = []
        for k, (lo, hi, unit) in _QA_RANGES.items():
            gwh = sum(data[k]) / 1000
            if not (lo <= gwh <= hi):
                errs.append(
                    f'{k}: {gwh:.1f} GWh outside expected range [{lo}, {hi}] GWh'
                )
        if errs:
            raise ValueError('Loader QA failed:\n' + '\n'.join(errs))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Sizing parameters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class BessSizingParams:
    """Parameters for one BESS sizing / validation run."""
    mode: str = 'reliability'   # reliability | fixed
    eens_target_pct: float = 0.10
    add_duration_h: float = 1.0       # duration for ADDITIONAL BESS only
    export_firm: bool = False          # True → export treated as must-serve
    export_limit_mw: float = 800.0    # cap on export demand profile
    fixed_p_mw: float | None = None   # total BESS MW (fixed mode)
    fixed_e_mwh: float | None = None  # total BESS MWh (fixed mode)
    eta_c: float = 0.95
    eta_d: float = 0.95
    ksani_p_mw: float = KSANI_P_MW
    ksani_e_mwh: float = KSANI_E_MWH


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Solver
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def solve_bess_sizing(
    data: dict[str, list[float]],
    params: BessSizingParams,
    reg_energy_gwh: tuple[float, ...] = REG_ENERGY_GWH,
    reg_pmax_mw: tuple[float, ...]   = REG_PMAX_MW,
) -> dict:
    """Run the 8760-hour chronological BESS sizing LP.

    Returns a result dict including scalar KPIs and hourly series.

    Strict accounting:
    - Internal EENS  = sum(ens_internal) / sum(load)  only.
    - Export ENS     = reported separately, never enters EENS denominator.
    - Spill/curtailment are separate and never ENS.
    - Energy closure is reported.
    """
    if len(data['load']) != 8760:
        raise ValueError('BESS sizing requires a full 8760-hour year')

    # month-of-hour index
    month_of_t: list[int] = []
    for m, h in enumerate(MONTH_HOURS_2030):
        month_of_t += [m] * h

    D = data['load']
    X = [min(x, params.export_limit_mw) for x in data['export']]
    must = [
        sum(data.get(k, [0.0] * 8760)[t] for k in ('pv', 'wind', 'ror', 'tpp'))
        for t in range(8760)
    ]
    T = range(8760)

    Kp = float(params.ksani_p_mw)
    Ke = float(params.ksani_e_mwh)
    dur = float(params.add_duration_h)

    # ── Build Pyomo model ────────────────────────────────────────────────────
    m = pyo.ConcreteModel()
    m.T = pyo.RangeSet(0, 8759)
    m.M = pyo.RangeSet(0, 11)

    # BESS sizing variables (TOTAL = Ksani + additional)
    m.P = pyo.Var(domain=pyo.NonNegativeReals)   # total BESS power MW
    m.E = pyo.Var(domain=pyo.NonNegativeReals)   # total BESS energy MWh

    # Ksani minimum
    m.ksani_p = pyo.Constraint(expr=m.P >= Kp)
    m.ksani_e = pyo.Constraint(expr=m.E >= Ke)

    # Duration of ADDITIONAL BESS only: (E - Ke) = dur × (P - Kp)
    m.add_dur = pyo.Constraint(expr=m.E - Ke == dur * (m.P - Kp))

    # Operational variables
    m.ch  = pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.dis = pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.soc = pyo.Var(m.T, domain=pyo.NonNegativeReals)
    m.reg = pyo.Var(m.T, domain=pyo.NonNegativeReals)

    # Objective coefficients: internal ENS has highest priority (largest penalty in
    # fixed/dispatch mode); export ENS has lower priority than internal load.
    # In reliability mode the EENS constraint governs ens_int; dispatch terms only
    # provide numerical regularisation.
    _VOLL_INT = 1.0          # per-MWh weight for internal ENS in fixed mode
    _VOLL_EXP = 1e-3         # per-MWh weight for export ENS / curtailment
    _EPS      = 1e-6         # regularisation for ch/dis/spill

    if params.export_firm:
        # Separate ENS for internal load and export; balance uses X[t] as fixed
        m.ens_int = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.ens_exp = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.spill   = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.balance = pyo.Constraint(
            m.T,
            rule=lambda mm, t: (
                must[t] + mm.reg[t] + mm.dis[t] - mm.ch[t]
                == D[t] + X[t] - mm.ens_int[t] - mm.ens_exp[t] + mm.spill[t]
            ),
        )
        m.ens_int_cap = pyo.Constraint(m.T, rule=lambda mm, t: mm.ens_int[t] <= D[t])
        m.ens_exp_cap = pyo.Constraint(m.T, rule=lambda mm, t: mm.ens_exp[t] <= X[t])
        # EENS target on internal load only
        if params.mode == 'reliability':
            m.eens_target = pyo.Constraint(
                expr=sum(m.ens_int[t] for t in m.T)
                <= params.eens_target_pct / 100.0 * sum(D)
            )
        # Objectives:
        # - fixed mode: dispatch optimisation → minimise ENS (internal priority)
        # - reliability mode: sizing minimisation → cost of additional BESS
        if params.mode == 'fixed':
            m.obj = pyo.Objective(
                expr=_VOLL_INT * sum(m.ens_int[t] for t in m.T)
                + _VOLL_EXP * sum(m.ens_exp[t] for t in m.T)
                + _EPS * sum(m.ch[t] + m.dis[t] + m.spill[t] for t in m.T),
                sense=pyo.minimize,
            )
        else:
            m.obj = pyo.Objective(
                expr=(m.P - Kp)
                + _VOLL_EXP * sum(m.ens_exp[t] for t in m.T)
                + _EPS * sum(m.ch[t] + m.dis[t] + m.spill[t] for t in m.T),
                sense=pyo.minimize,
            )
    else:
        # Export curtailable: exp is a free variable 0..X[t]
        m.exp   = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.ens   = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.spill = pyo.Var(m.T, domain=pyo.NonNegativeReals)
        m.balance = pyo.Constraint(
            m.T,
            rule=lambda mm, t: (
                must[t] + mm.reg[t] + mm.dis[t] - mm.ch[t]
                == D[t] + mm.exp[t] - mm.ens[t] + mm.spill[t]
            ),
        )
        m.export_cap = pyo.Constraint(m.T, rule=lambda mm, t: mm.exp[t] <= X[t])
        if params.mode == 'reliability':
            m.eens_target = pyo.Constraint(
                expr=sum(m.ens[t] for t in m.T)
                <= params.eens_target_pct / 100.0 * sum(D)
            )
        if params.mode == 'fixed':
            m.obj = pyo.Objective(
                expr=_VOLL_INT * sum(m.ens[t] for t in m.T)
                + _VOLL_EXP * sum(X[t] - m.exp[t] for t in m.T)
                + _EPS * sum(m.ch[t] + m.dis[t] + m.spill[t] for t in m.T),
                sense=pyo.minimize,
            )
        else:
            m.obj = pyo.Objective(
                expr=(m.P - Kp)
                + _EPS * sum(
                    m.ch[t] + m.dis[t] + m.spill[t] + (X[t] - m.exp[t])
                    for t in m.T
                ),
                sense=pyo.minimize,
            )

    # Fixed mode overrides
    if params.mode == 'fixed':
        fp = float(params.fixed_p_mw or 0)
        fe = float(params.fixed_e_mwh or 0)
        m.fix_p = pyo.Constraint(expr=m.P == fp)
        m.fix_e = pyo.Constraint(expr=m.E == fe)
        # Remove duration constraint (might conflict with fixed values)
        m.del_component(m.add_dur)

    # BESS operational constraints
    m.ch_cap  = pyo.Constraint(m.T, rule=lambda mm, t: mm.ch[t] <= mm.P)
    m.dis_cap = pyo.Constraint(m.T, rule=lambda mm, t: mm.dis[t] <= mm.P)
    m.soc_cap = pyo.Constraint(m.T, rule=lambda mm, t: mm.soc[t] <= mm.E)
    m.soc_dyn = pyo.Constraint(
        m.T,
        rule=lambda mm, t: mm.soc[t]
        == (mm.soc[8759 if t == 0 else t - 1]
            + params.eta_c * mm.ch[t]
            - mm.dis[t] / params.eta_d),
    )

    # Regulated HPP constraints
    m.reg_pmax   = pyo.Constraint(
        m.T, rule=lambda mm, t: mm.reg[t] <= reg_pmax_mw[month_of_t[t]]
    )
    m.reg_energy = pyo.Constraint(
        m.M,
        rule=lambda mm, mo: sum(mm.reg[t] for t in T if month_of_t[t] == mo)
        <= reg_energy_gwh[mo] * 1000.0,
    )

    # ── Solve ────────────────────────────────────────────────────────────────
    solver = pyo.SolverFactory('appsi_highs')
    res = solver.solve(m)
    status = str(res.solver.termination_condition)
    if status.lower() not in ('optimal', 'locallyoptimal'):
        raise RuntimeError(f'HiGHS solver: {status}')

    # ── Extract results ──────────────────────────────────────────────────────
    def _v(var): return [pyo.value(var[t]) for t in m.T]

    p_total = float(pyo.value(m.P))
    e_total = float(pyo.value(m.E))
    p_add   = max(0.0, p_total - Kp)
    e_add   = max(0.0, e_total - Ke)

    ch_mw  = _v(m.ch)
    dis_mw = _v(m.dis)
    soc_mwh = _v(m.soc)
    reg_mw = _v(m.reg)
    spill_mw = _v(m.spill)

    if params.export_firm:
        ens_int_mw = _v(m.ens_int)
        ens_exp_mw = _v(m.ens_exp)
        exp_mw = [X[t] - ens_exp_mw[t] for t in T]
    else:
        ens_int_mw = _v(m.ens)
        ens_exp_mw = [max(0.0, X[t] - pyo.value(m.exp[t])) for t in T]
        exp_mw     = _v(m.exp)

    ens_int_mwh = sum(ens_int_mw)
    ens_exp_mwh = sum(ens_exp_mw)
    total_load  = sum(D)

    cost_per_mw_yr = DURATION_COST.get(dur, COST_1H_PER_MW_YR)
    add_capex_yr   = cost_per_mw_yr * p_add
    ksani_capex_yr = COST_1H_PER_MW_YR * Kp

    # Energy closure: total in = total out + losses + stored_change
    reg_total = sum(reg_mw)
    gen_in    = sum(must) + reg_total + sum(dis_mw)
    bess_loss = sum(ch_mw) * (1 - params.eta_c) + sum(dis_mw) * (1 / params.eta_d - 1)
    demand_served = total_load - ens_int_mwh + sum(exp_mw)
    closure_mwh = gen_in - sum(ch_mw) - demand_served - sum(spill_mw) - bess_loss
    closure_pct = 100 * closure_mwh / (total_load or 1)

    return {
        # scalars
        'status': 'Optimal',
        'pbess_total_mw':  p_total,
        'ebess_total_mwh': e_total,
        'pbess_add_mw':    p_add,
        'ebess_add_mwh':   e_add,
        'add_duration_h':  dur,
        'ksani_p_mw':      Kp,
        'ksani_e_mwh':     Ke,
        'eens_gwh':        ens_int_mwh / 1000,
        'eens_pct':        100 * ens_int_mwh / total_load,
        'lolh_h':          sum(1 for x in ens_int_mw if x > 1e-6),
        'export_ens_gwh':  ens_exp_mwh / 1000,
        'curtailment_gwh': sum(spill_mw) / 1000,
        'pv_gwh':          sum(data.get('pv', [0]*8760)) / 1000,
        'wind_gwh':        sum(data.get('wind', [0]*8760)) / 1000,
        'ror_gwh':         sum(data.get('ror', [0]*8760)) / 1000,
        'tpp_gwh':         sum(data.get('tpp', [0]*8760)) / 1000,
        'reg_gwh':         reg_total / 1000,
        'bess_charge_gwh': sum(ch_mw) / 1000,
        'bess_discharge_gwh': sum(dis_mw) / 1000,
        'cycles_per_year': sum(dis_mw) / (e_total or math.inf),
        'add_capex_annualized_usd':   add_capex_yr,
        'ksani_capex_annualized_usd': ksani_capex_yr,
        'total_capex_annualized_usd': add_capex_yr + ksani_capex_yr,
        'energy_closure_pct': closure_pct,
        # hourly series
        'series': {
            'load_mw':        D,
            'export_demand_mw': X,
            'export_served_mw': exp_mw,
            'pv_mw':          list(data.get('pv', [0]*8760)),
            'wind_mw':        list(data.get('wind', [0]*8760)),
            'ror_mw':         list(data.get('ror', [0]*8760)),
            'tpp_mw':         list(data.get('tpp', [0]*8760)),
            'reg_mw':         reg_mw,
            'charge_mw':      ch_mw,
            'discharge_mw':   dis_mw,
            'soc_mwh':        soc_mwh,
            'ens_internal_mw': ens_int_mw,
            'ens_export_mw':  ens_exp_mw,
            'spill_mw':       spill_mw,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CLI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    ap = argparse.ArgumentParser(description='Georgia 2030 BESS sizing LP')
    ap.add_argument('--plexos-zip', default='plexos model.zip')
    ap.add_argument('--mode', choices=['reliability', 'fixed'], default='reliability')
    ap.add_argument('--eens-target-pct', type=float, default=0.10)
    ap.add_argument('--add-duration-h', type=float, choices=[1.0, 2.0, 4.0], default=1.0)
    ap.add_argument('--export-firm', action='store_true')
    ap.add_argument('--fixed-p-mw', type=float)
    ap.add_argument('--fixed-e-mwh', type=float)
    ap.add_argument('--out', default='out/bess_sizing_results.json')
    args = ap.parse_args()

    loader = PlexosZipLoader(args.plexos_zip)
    data = loader.load(verbose=True)
    reg_energy, reg_pmax = loader.reg_limits()

    params = BessSizingParams(
        mode=args.mode,
        eens_target_pct=args.eens_target_pct,
        add_duration_h=args.add_duration_h,
        export_firm=args.export_firm,
        fixed_p_mw=args.fixed_p_mw,
        fixed_e_mwh=args.fixed_e_mwh,
    )

    result = solve_bess_sizing(data, params, reg_energy, reg_pmax)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out = {'params': asdict(params), 'results': {k: v for k, v in result.items() if k != 'series'}}
    Path(args.out).write_text(json.dumps(out, indent=2), encoding='utf-8')
    print(json.dumps({k: v for k, v in result.items() if k != 'series'}, indent=2))


if __name__ == '__main__':
    main()
