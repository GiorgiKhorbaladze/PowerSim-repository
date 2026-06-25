import pytest

from solver.bess_sizing import BessSizingParams, solve_bess_sizing


def _flat(v):
    return [float(v)] * 8760


def test_requires_regulated_hydro_pmax():
    data = {"load": _flat(10), "export": _flat(0), "pv": _flat(0), "wind": _flat(0), "ror": _flat(0), "tpp": _flat(0)}
    with pytest.raises(ValueError, match="Regulated hydro monthly Pmax"):
        solve_bess_sizing(data, BessSizingParams(mode="fixed", fixed_p_mw=0, fixed_e_mwh=0))


def test_fixed_zero_bess_keeps_export_out_of_eens():
    data = {"load": _flat(10), "export": _flat(5), "pv": _flat(0), "wind": _flat(0), "ror": _flat(0), "tpp": _flat(10)}
    params = BessSizingParams(mode="fixed", fixed_p_mw=0, fixed_e_mwh=0, reg_pmax_mw=tuple([0]*12))
    result = solve_bess_sizing(data, params)
    assert result["pbess_mw"] == pytest.approx(0)
    assert result["ebess_mwh"] == pytest.approx(0)
    assert result["eens_gwh"] == pytest.approx(0)
    assert result["export_ens_gwh"] == pytest.approx(43.8)
