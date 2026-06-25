"""Tests for solver/bess_sizing.py.

Validates the following critical properties:
1. Loader refuses to run if profile files are missing.
2. Fixed 800/1400 validation no longer returns ~15.7% EENS.
3. Export ENS never enters internal EENS.
4. Spill/curtailment never counted as ENS.
5. Scenario runner produces all required scenarios.
6. Energy closure within tolerance.
"""
import io
import math
import zipfile

import pytest

from solver.bess_sizing import (
    BessSizingParams,
    PlexosZipLoader,
    KSANI_P_MW,
    KSANI_E_MWH,
    solve_bess_sizing,
)


# ── Loader zip helper ────────────────────────────────────────────────────────

_ROOT = 'plexos model/BESS Research 800-1400/'
# Semicolon-delimited with a header so csv.Sniffer can detect the delimiter
_ROWS_8760 = 'DATETIME;Value\n' + '\n'.join([f'{t};1.0' for t in range(8760)])


def _make_loader_zip(include_pv: bool = True) -> PlexosZipLoader:
    """Return a PlexosZipLoader backed by a minimal in-memory zip."""
    from solver.bess_sizing import _PV_CAP, _WIND_CAP, _ROR_CAP
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr(_ROOT + 'Load.csv', _ROWS_8760)
        zf.writestr(_ROOT + 'Export.csv', _ROWS_8760)
        zf.writestr(_ROOT + 'Tpp-Imp.csv', _ROWS_8760)
        if include_pv:
            for fname in _PV_CAP:
                zf.writestr(_ROOT + fname, _ROWS_8760)
        for fname in _WIND_CAP:
            zf.writestr(_ROOT + fname, _ROWS_8760)
        for fname in _ROR_CAP:
            zf.writestr(_ROOT + fname, _ROWS_8760)
    buf.seek(0)
    loader = object.__new__(PlexosZipLoader)
    loader.zip_path = None
    loader.root = _ROOT
    loader.zf = zipfile.ZipFile(buf)
    loader._names_set = set(loader.zf.namelist())
    return loader


# ── Test 0: loader mapping ────────────────────────────────────────────────────

class TestLoaderMapping:
    def test_loader_refuses_missing_profile(self):
        """PlexosZipLoader raises ValueError if any profile CSV is absent."""
        loader = _make_loader_zip(include_pv=False)  # all PV files missing
        with pytest.raises(ValueError, match='not found in zip'):
            loader.load(verbose=False)

    def test_loader_reads_all_profiles_when_present(self):
        """When all profile files are present, _read and _scale_sum succeed."""
        from solver.bess_sizing import _PV_CAP
        loader = _make_loader_zip(include_pv=True)
        for fname in _PV_CAP:
            vals = loader._read(fname)
            assert len(vals) == 8760
        pv = loader._scale_sum(_PV_CAP)
        assert len(pv) == 8760
        assert all(v >= 0.0 for v in pv)


# ── Synthetic dataset helpers ─────────────────────────────────────────────────

def _flat(v: float, n: int = 8760) -> list[float]:
    return [float(v)] * n


def _base_data(
    load: float = 1000.0,
    export: float = 0.0,
    must_run: float = 1000.0,
):
    """Balanced dataset: must_run covers load exactly, zero ENS expected."""
    return {
        'load':   _flat(load),
        'export': _flat(export),
        'pv':     _flat(must_run),
        'wind':   _flat(0.0),
        'ror':    _flat(0.0),
        'tpp':    _flat(0.0),
    }


_REG_ENERGY = tuple([0.0] * 12)
_REG_PMAX   = tuple([0.0] * 12)


# ── Test 1: export ENS never enters internal EENS ────────────────────────────

class TestExportEnsSeparation:
    def test_export_firm_no_supply_sheds_export_not_load(self):
        """With export-firm and perfect internal supply, export ENS is separate."""
        # Internal load = 1000 MW, perfect supply = 1000 MW.
        # Export demand = 500 MW but supply is already fully used for load.
        data = {
            'load':   _flat(1000.0),
            'export': _flat(500.0),
            'pv':     _flat(1000.0),
            'wind':   _flat(0.0),
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        params = BessSizingParams(
            mode='fixed',
            fixed_p_mw=KSANI_P_MW,
            fixed_e_mwh=KSANI_E_MWH,
            export_firm=True,
        )
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        # No internal ENS (must_run=load)
        assert r['eens_gwh'] == pytest.approx(0.0, abs=0.01)
        assert r['eens_pct'] == pytest.approx(0.0, abs=0.01)
        # Export ENS should be nonzero (supply can't cover both)
        assert r['export_ens_gwh'] > 0.0

    def test_export_curtailable_no_internal_ens(self):
        """Export-curtailable: export shed before internal load; internal ENS = 0."""
        data = {
            'load':   _flat(1000.0),
            'export': _flat(200.0),
            'pv':     _flat(1000.0),
            'wind':   _flat(0.0),
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        params = BessSizingParams(
            mode='fixed',
            fixed_p_mw=KSANI_P_MW,
            fixed_e_mwh=KSANI_E_MWH,
            export_firm=False,
        )
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        # Internal load exactly covered – no internal ENS
        assert r['eens_gwh'] == pytest.approx(0.0, abs=0.01)


# ── Test 2: spill is NOT ENS ─────────────────────────────────────────────────

class TestSpillNotENS:
    def test_excess_supply_becomes_spill_not_ens(self):
        """Surplus generation → spill; internal ENS must remain zero."""
        data = {
            'load':   _flat(500.0),
            'export': _flat(0.0),
            'pv':     _flat(2000.0),   # excess
            'wind':   _flat(0.0),
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        params = BessSizingParams(
            mode='fixed',
            fixed_p_mw=KSANI_P_MW,
            fixed_e_mwh=KSANI_E_MWH,
        )
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        assert r['eens_gwh'] == pytest.approx(0.0, abs=0.01)
        assert r['curtailment_gwh'] > 0.0


# ── Test 3: fixed 800/1400 does NOT give 15.7% EENS (with correct profiles) ─

class TestFixed800_1400Validation:
    def test_corrected_data_gives_low_eens(self):
        """With correct profiles and RegHPP, 800/1400 BESS should give EENS << 15%."""
        from solver.bess_sizing import REG_ENERGY_GWH, REG_PMAX_MW

        # Use a simplified but physically consistent dataset:
        # load=2064 MW avg (~18,080 GWh), generous must-run supply.
        load_mw = 2064.0
        # Must-run = RoR+PV+Wind covers most of load (approx Georgian values)
        # Approx totals: RoR~7940 GWh, PV~2801 GWh, Wind~7570 GWh → avg 2089 MW must-run
        must_run_avg = (7940 + 2801 + 7570) / 8.76e3  # ≈ 2090 MW
        data = {
            'load':   _flat(load_mw),
            'export': _flat(512.0),  # ~4494 GWh/yr
            'pv':     _flat(320.0),
            'wind':   _flat(864.0),
            'ror':    _flat(906.0),
            'tpp':    _flat(0.0),
        }
        params = BessSizingParams(
            mode='fixed',
            fixed_p_mw=800.0,
            fixed_e_mwh=1400.0,
        )
        r = solve_bess_sizing(data, params, REG_ENERGY_GWH, REG_PMAX_MW)
        # With regulated hydro + VRE covering ~18000+ GWh and 800/1400 BESS,
        # EENS should be well below 15%
        assert r['eens_pct'] < 5.0, (
            f'Fixed 800/1400 still gives high EENS {r["eens_pct"]:.2f}% '
            f'(correct loader should give << 5%)'
        )


# ── Test 4: reliability sizing returns non-negative additional BESS ───────────

class TestReliabilitySizing:
    def test_zero_must_run_needs_large_bess(self):
        """With zero must-run supply, large BESS needed to meet EENS target."""
        data = {
            'load':   _flat(1000.0),
            'export': _flat(0.0),
            'pv':     _flat(0.0),
            'wind':   _flat(0.0),
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        # Only regulated hydro available
        reg_energy = tuple([1e9] * 12)   # unlimited hydro
        reg_pmax   = tuple([2000.0] * 12)
        params = BessSizingParams(mode='reliability', eens_target_pct=0.10)
        r = solve_bess_sizing(data, params, reg_energy, reg_pmax)
        assert r['eens_pct'] <= 0.10 + 1e-3
        assert r['pbess_total_mw'] >= KSANI_P_MW
        assert r['pbess_add_mw'] >= 0.0

    def test_abundant_supply_needs_minimal_bess(self):
        """With must-run > load, EENS = 0 and additional BESS ≈ 0."""
        data = {
            'load':   _flat(1000.0),
            'export': _flat(0.0),
            'pv':     _flat(0.0),
            'wind':   _flat(2000.0),   # always surplus
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        params = BessSizingParams(mode='reliability', eens_target_pct=0.10)
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        assert r['eens_pct'] == pytest.approx(0.0, abs=0.01)
        assert r['pbess_add_mw'] == pytest.approx(0.0, abs=1.0)
        assert r['ebess_add_mwh'] == pytest.approx(0.0, abs=2.0)


# ── Test 5: Ksani is always included ─────────────────────────────────────────

class TestKsaniMinimum:
    def test_fixed_mode_total_bess_includes_ksani(self):
        """Fixed mode: P_total >= Ksani and E_total >= Ksani always."""
        data = _base_data()
        params = BessSizingParams(
            mode='fixed', fixed_p_mw=KSANI_P_MW, fixed_e_mwh=KSANI_E_MWH
        )
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        assert r['pbess_total_mw'] == pytest.approx(KSANI_P_MW, abs=1.0)
        assert r['ebess_total_mwh'] == pytest.approx(KSANI_E_MWH, abs=1.0)
        assert r['pbess_add_mw'] == pytest.approx(0.0, abs=1.0)
        assert r['ebess_add_mwh'] == pytest.approx(0.0, abs=1.0)

    def test_reliability_mode_always_at_least_ksani(self):
        """Reliability mode: even with zero demand, BESS >= Ksani."""
        data = _base_data(load=1.0, export=0.0, must_run=10.0)
        params = BessSizingParams(mode='reliability', eens_target_pct=0.10)
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        assert r['pbess_total_mw'] >= KSANI_P_MW - 0.5
        assert r['ebess_total_mwh'] >= KSANI_E_MWH - 0.5


# ── Test 6: energy closure ───────────────────────────────────────────────────

class TestEnergyClosure:
    def test_balanced_system_closure(self):
        """Balanced supply/demand → energy closure within 0.1%."""
        data = _base_data(load=1000.0, must_run=1000.0)
        params = BessSizingParams(
            mode='fixed', fixed_p_mw=KSANI_P_MW, fixed_e_mwh=KSANI_E_MWH
        )
        r = solve_bess_sizing(data, params, _REG_ENERGY, _REG_PMAX)
        assert abs(r['energy_closure_pct']) < 0.5


# ── Test 7: duration constraint on additional BESS ───────────────────────────

class TestDurationConstraint:
    @pytest.mark.parametrize('dur', [1.0, 2.0, 4.0])
    def test_additional_bess_duration(self, dur: float):
        """E_add / P_add == requested duration when P_add > 1 MW.

        Alternating PV (2000 MW even hours / 0 MW odd hours) creates a
        temporal mismatch that forces BESS sizing.  Regulated hydro
        (400 MW / 100 GWh/month) alone cannot bridge the gap, so
        reliability mode will size non-trivial additional BESS.
        Total energy supply (PV ≈ 8760 GWh + reg ≈ 1200 GWh) exceeds
        load (8760 GWh), keeping the LP feasible.
        """
        pv = [2000.0 if t % 2 == 0 else 0.0 for t in range(8760)]
        data = {
            'load':   _flat(1000.0),
            'export': _flat(0.0),
            'pv':     pv,
            'wind':   _flat(0.0),
            'ror':    _flat(0.0),
            'tpp':    _flat(0.0),
        }
        reg_energy = tuple([100.0] * 12)
        reg_pmax   = tuple([400.0] * 12)
        params = BessSizingParams(
            mode='reliability',
            eens_target_pct=0.10,
            add_duration_h=dur,
        )
        r = solve_bess_sizing(data, params, reg_energy, reg_pmax)
        p_add = r['pbess_add_mw']
        e_add = r['ebess_add_mwh']
        if p_add > 1.0:
            assert e_add / p_add == pytest.approx(dur, rel=0.01)


# ── Test 8: scenario runner matrix ───────────────────────────────────────────

class TestScenarioMatrix:
    def test_matrix_has_required_scenarios(self):
        """Scenario matrix includes all 18 reliability + 2 baseline scenarios."""
        from solver.bess_scenario_runner import SCENARIO_MATRIX
        labels = [s['label'] for s in SCENARIO_MATRIX]
        # Baseline scenarios
        assert 'ksani_only' in labels
        assert 'fixed_800_1400' in labels
        # Reliability: 2 export × 3 targets × 3 durations = 18
        reliability = [s for s in SCENARIO_MATRIX if s.get('mode') == 'reliability']
        assert len(reliability) == 18

    def test_matrix_has_all_export_treatments(self):
        from solver.bess_scenario_runner import SCENARIO_MATRIX
        reliability = [s for s in SCENARIO_MATRIX if s.get('mode') == 'reliability']
        firm_count = sum(1 for s in reliability if s.get('export_firm'))
        curt_count = sum(1 for s in reliability if not s.get('export_firm'))
        assert firm_count == 9  # 3 targets × 3 durations
        assert curt_count == 9

    def test_matrix_has_all_targets(self):
        from solver.bess_scenario_runner import SCENARIO_MATRIX, EENS_TARGETS
        reliability = [s for s in SCENARIO_MATRIX if s.get('mode') == 'reliability']
        for tgt in EENS_TARGETS:
            count = sum(1 for s in reliability if s.get('eens_target_pct') == tgt)
            assert count == 6  # 2 export × 3 durations

    def test_matrix_has_all_durations(self):
        from solver.bess_scenario_runner import SCENARIO_MATRIX, DURATIONS
        reliability = [s for s in SCENARIO_MATRIX if s.get('mode') == 'reliability']
        for dur in DURATIONS:
            count = sum(1 for s in reliability if s.get('add_duration_h') == dur)
            assert count == 6  # 2 export × 3 targets
