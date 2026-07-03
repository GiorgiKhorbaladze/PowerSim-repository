from __future__ import annotations

import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.powersim_ai_tools import compare_results, load_input_summary, propose_add_bess, propose_edit_asset, run_solver, summarize_results

INPUT = {
    "metadata": {"study_year": 2026},
    "study_horizon": {"horizon_hours": 24, "start_hour": 0},
    "assets": [{"id": "G1", "type": "thermal", "pmax_mw": 100}],
    "profiles": {"demand": [10, 20, 30]},
    "reserve_products": [{"id": "spin"}],
    "gas_constraints": {"mode": "none"},
    "profile_bundle": {"scenario_id": "A_mean"},
}
RESULTS = {"system_summary": {"total_cost": 100, "objective_cost": 110, "gas": 5, "unserved_mwh": 1, "curtailment_mwh": 2}, "hourly_system": [{"lambda": 10}, {"lambda": 20}], "by_unit_summary": [{"id": "G1", "generation_mwh": 50, "cost": 90}]}

class AIToolsTest(unittest.TestCase):
    def test_load_input_summary(self):
        s = load_input_summary(INPUT)
        self.assertEqual(s["study_year"], 2026)
        self.assertEqual(s["assets_by_type"]["thermal"], 1)
        self.assertEqual(s["demand_peak"], 30)

    def test_summarize_results(self):
        s = summarize_results(RESULTS)
        self.assertEqual(s["total_cost"], 100)
        self.assertEqual(s["avg_lambda"], 15)

    def test_compare_results(self):
        cand = copy.deepcopy(RESULTS)
        cand["system_summary"]["total_cost"] = 150
        self.assertEqual(compare_results(RESULTS, cand)["deltas"]["total_cost"], 50)

    def test_propose_add_bess_returns_patched_copy(self):
        original = copy.deepcopy(INPUT)
        out = propose_add_bess(INPUT, "B1", 10, 40)
        self.assertEqual(INPUT, original)
        self.assertEqual(len(out["patched_input"]["assets"]), 2)

    def test_propose_edit_asset_validates_path(self):
        out = propose_edit_asset(INPUT, "G1", "limits.pmax_mw", 80)
        self.assertEqual(out["patched_input"]["assets"][0]["limits"]["pmax_mw"], 80)
        with self.assertRaises(ValueError):
            propose_edit_asset(INPUT, "missing", "pmax_mw", 80)
        with self.assertRaises(ValueError):
            propose_edit_asset(INPUT, "G1", "metadata.secret", "x")

    def test_run_solver_requires_confirmation(self):
        self.assertTrue(run_solver("samples/sample_input_168h.json", ".runtime/sessions/t/out")["requires_confirmation"])

if __name__ == "__main__":
    unittest.main()
