"""Georgia 2030 BESS scenario runner.

Runs the full matrix:
  Export treatment:   curtailable | firm
  EENS targets:       0.10% | 0.05% | 0.03%
  Additional duration: 1h | 2h | 4h

Also runs zero-BESS baseline and fixed 800/1400 validation case.
Saves results to JSON and prints a summary table.
"""
from __future__ import annotations

import argparse, json, sys, time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from solver.bess_sizing import (
    BessSizingParams,
    PlexosZipLoader,
    REG_ENERGY_GWH,
    REG_PMAX_MW,
    KSANI_P_MW,
    KSANI_E_MWH,
    solve_bess_sizing,
)

EENS_TARGETS = [0.10, 0.05, 0.03]
DURATIONS    = [1.0, 2.0, 4.0]

SCENARIO_MATRIX: list[dict[str, Any]] = []
# Baseline: zero total BESS (Ksani-only)
SCENARIO_MATRIX.append({
    'label': 'ksani_only',
    'description': 'Ksani 200/200 only, no additional BESS',
    'mode': 'fixed',
    'fixed_p_mw': KSANI_P_MW,
    'fixed_e_mwh': KSANI_E_MWH,
    'export_firm': False,
    'add_duration_h': 1.0,
})
# Fixed 800/1400 validation (total BESS)
SCENARIO_MATRIX.append({
    'label': 'fixed_800_1400',
    'description': 'Fixed 800 MW / 1400 MWh (validation)',
    'mode': 'fixed',
    'fixed_p_mw': 800.0,
    'fixed_e_mwh': 1400.0,
    'export_firm': False,
    'add_duration_h': 1.75,  # informational only; not a reliability target
})
# Reliability scenarios
for export_firm in [False, True]:
    for target in EENS_TARGETS:
        for dur in DURATIONS:
            exp_label = 'firm' if export_firm else 'curt'
            SCENARIO_MATRIX.append({
                'label': f'exp_{exp_label}_eens{target:.2f}pct_{int(dur)}h',
                'description': (
                    f'Export {"firm" if export_firm else "curtailable"}, '
                    f'EENS≤{target:.2f}%, add BESS {int(dur)}h'
                ),
                'mode': 'reliability',
                'eens_target_pct': target,
                'export_firm': export_firm,
                'add_duration_h': dur,
                'fixed_p_mw': None,
                'fixed_e_mwh': None,
            })


def _run_one(
    scen: dict[str, Any],
    data: dict[str, list[float]],
    reg_energy: tuple,
    reg_pmax: tuple,
) -> dict[str, Any]:
    params = BessSizingParams(
        mode=scen['mode'],
        eens_target_pct=scen.get('eens_target_pct', 0.10),
        add_duration_h=scen.get('add_duration_h', 1.0),
        export_firm=scen.get('export_firm', False),
        fixed_p_mw=scen.get('fixed_p_mw'),
        fixed_e_mwh=scen.get('fixed_e_mwh'),
    )
    t0 = time.time()
    try:
        result = solve_bess_sizing(data, params, reg_energy, reg_pmax)
        elapsed = time.time() - t0
        result['elapsed_s'] = round(elapsed, 1)
        result['scenario'] = scen
        # Drop hourly series from the scenario output (kept separately if needed)
        result_no_series = {k: v for k, v in result.items() if k != 'series'}
        return result_no_series
    except Exception as exc:
        return {'status': 'Error', 'error': str(exc), 'scenario': scen}


def _print_table(results: list[dict[str, Any]]) -> None:
    header = (
        f"{'Label':<35} {'PaddMW':>8} {'EaddMWh':>8} "
        f"{'EENS%':>7} {'LOLH':>6} {'ExpENS':>7} "
        f"{'Curt':>6} {'AddMUSD/yr':>11}"
    )
    print('\n' + '=' * len(header))
    print('Georgia 2030 BESS Sizing – Scenario Results')
    print('=' * len(header))
    print(header)
    print('-' * len(header))
    for r in results:
        if r.get('status') == 'Error':
            print(f"{r['scenario']['label']:<35} ERROR: {r.get('error','')[:60]}")
            continue
        lbl  = r['scenario']['label']
        padd = r.get('pbess_add_mw', 0)
        eadd = r.get('ebess_add_mwh', 0)
        eens = r.get('eens_pct', 0)
        lolh = r.get('lolh_h', 0)
        xens = r.get('export_ens_gwh', 0)
        curt = r.get('curtailment_gwh', 0)
        capx = r.get('add_capex_annualized_usd', 0) / 1e6
        print(
            f'{lbl:<35} {padd:>8.0f} {eadd:>8.0f} '
            f'{eens:>7.3f} {lolh:>6.0f} {xens:>7.1f} '
            f'{curt:>6.1f} {capx:>11.1f}'
        )
    print('=' * len(header) + '\n')


def run_all(
    zip_path: str,
    out_dir: str = 'out',
    verbose: bool = True,
) -> list[dict[str, Any]]:
    """Run the full scenario matrix and return results list."""
    loader = PlexosZipLoader(zip_path)
    data = loader.load(verbose=verbose)
    reg_energy, reg_pmax = loader.reg_limits()

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    total = len(SCENARIO_MATRIX)
    for i, scen in enumerate(SCENARIO_MATRIX, 1):
        print(f'[{i}/{total}] {scen["label"]} …', end=' ', flush=True)
        r = _run_one(scen, data, reg_energy, reg_pmax)
        status = r.get('status', 'Error')
        eens   = r.get('eens_pct', float('nan'))
        padd   = r.get('pbess_add_mw', float('nan'))
        print(f'{status}  EENS={eens:.3f}%  Padd={padd:.0f} MW')
        results.append(r)

    _print_table(results)

    # Save JSON (without hourly series; series are added by bess_report.py)
    json_path = out_path / 'bess_scenarios.json'
    json_path.write_text(json.dumps(results, indent=2, default=str), encoding='utf-8')
    print(f'Results saved → {json_path}')
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description='Georgia 2030 BESS scenario runner')
    ap.add_argument('--plexos-zip', default='plexos model.zip')
    ap.add_argument('--out', default='out')
    ap.add_argument('--quiet', action='store_true')
    args = ap.parse_args()
    run_all(args.plexos_zip, args.out, verbose=not args.quiet)


if __name__ == '__main__':
    main()
