"""Deterministic regression for 15-minute ramp-rate scaling."""

from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "schema", ROOT / "solver"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_solver import build_asset_map, solve_window  # noqa: E402

RESOLUTION_MIN = 15
DT = RESOLUTION_MIN / 60
RAMP_MW_PER_H = 40.0
PERIOD_RAMP_MW = RAMP_MW_PER_H * DT


def test_subhourly_ramp_down_uses_period_dt():
    assets = build_asset_map({
        "assets": [
            {
                "id": "thermal",
                "type": "thermal",
                "committable": False,
                "pmin": 0,
                "pmax": 200,
                "mc": 10,
                "heat_rate": 0,
                "fuel_price": 0,
                "vom": 0,
                "startup_cost": 0,
                "no_load_cost": 0,
                "ramp_up": RAMP_MW_PER_H,
                "ramp_down": RAMP_MW_PER_H,
                "min_up": 0,
                "min_down": 0,
                "for_rate": 0,
            },
            {
                "id": "sink_bess",
                "type": "bess",
                "power_mw": 100,
                "energy_mwh": 100,
                "soc_init": 0,
                "soc_min": 0,
                "soc_max": 1,
                "eta_charge": 1,
                "eta_discharge": 1,
                # Make discharge unattractive; BESS only absorbs surplus
                # forced by the corrected ramp-down limit.
                "vom_discharge": 10000,
            },
        ]
    })
    hourly, _state, _solve_time, _obj = solve_window(
        assets=assets,
        demand_w=[90.0, 100.0, 60.0],
        profiles_w={},
        reserve_prods=[],
        gas_limits={"mode": "none"},
        init_state={},
        solver_cfg={"solver_backend": "highs", "time_limit_s": 60, "mip_gap": 0.0},
        dt=DT,
    )

    dispatch = [row["dispatch"]["thermal"] for row in hourly]
    charge = [row["bess"]["sink_bess"]["charge_mw"] for row in hourly]

    assert dispatch == [90.0, 100.0, 90.0]
    assert charge == [0.0, 0.0, 30.0]
    assert math.isclose(dispatch[1] - dispatch[0], PERIOD_RAMP_MW, abs_tol=1e-6)
    assert math.isclose(dispatch[1] - dispatch[2], PERIOD_RAMP_MW, abs_tol=1e-6)


def main() -> int:
    test_subhourly_ramp_down_uses_period_dt()
    print("Sub-hourly ramp regression tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
