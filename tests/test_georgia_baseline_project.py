"""Sanitized Georgia planning-baseline contract coverage."""
from __future__ import annotations

import json
from pathlib import Path

from powersim.contracts import ProjectContract, resolve_scenario


ROOT = Path(__file__).resolve().parents[1]


def test_georgia_2026_baseline_is_typed_and_uses_committed_demand():
    project=ProjectContract.model_validate_json((ROOT/"samples/projects/georgia_2026_baseline.json").read_text(encoding="utf-8"))
    source=json.loads((ROOT/"data/gse_load_2026_2030.json").read_text(encoding="utf-8"))
    assert project.id == "georgia-2026-baseline"
    assert project.metadata["classification"] == "sanitized aggregated planning demonstration"
    assert len(project.profiles[0].values) == 8760
    assert list(project.profiles[0].values) == source["load_mw_by_year"]["2026"]
    assert [scenario.id for scenario in project.scenarios] == ["georgia-2027","georgia-2028","georgia-2029","georgia-2030"]
    for year in range(2027,2031):
        resolved=resolve_scenario(project, f"georgia-{year}")
        assert resolved.time.study_year == year
        assert list(resolved.profiles[0].values) == source["load_mw_by_year"][str(year)]
