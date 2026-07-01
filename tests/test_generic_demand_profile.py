from pathlib import Path
import sys
import tempfile

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from solver.powersim_dataio import load_demand_profile_file


def test_load_multiyear_excel_selects_requested_year_column():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "load 2026-2030.xlsx"
        df = pd.DataFrame({
            "DateTime": pd.date_range("2026-01-01", periods=8760, freq="h"),
            "Load 2026 MW": [2026.0] * 8760,
            "Load 2027 MW": [2027.0] * 8760,
            "Load 2030 MW": [2030.0] * 8760,
        })
        df.to_excel(p, index=False)

        vals, path, meta = load_demand_profile_file(p, year=2030)

        assert path == p
        assert len(vals) == 8760
        assert vals[0] == 2030.0
        assert meta["column"] == "Load 2030 MW"
        assert meta["year"] == 2030


def test_load_csv_allows_explicit_column_name():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "loads.csv"
        df = pd.DataFrame({
            "DateTime": pd.date_range("2026-01-01", periods=8760, freq="h").strftime("%Y-%m-%d %H:%M"),
            "2026": [10.0] * 8760,
            "2028": [28.0] * 8760,
        })
        df.to_csv(p, index=False)

        vals, _, meta = load_demand_profile_file(p, year=2026, column="2028")

        assert vals == [28.0] * 8760
        assert meta["column"] == "2028"


if __name__ == "__main__":
    test_load_multiyear_excel_selects_requested_year_column()
    test_load_csv_allows_explicit_column_name()
    print("generic demand profile tests passed")
