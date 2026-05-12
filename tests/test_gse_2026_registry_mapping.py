"""Regression checks for the sanitized GSE 2026 installed-capacity summary."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "solver"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_asset_mapper import _CATEGORY_TO_TYPE  # noqa: E402

FIXTURE = ROOT / "tests" / "gse_2026_installed_capacity_registry_summary.json"


def _fixture() -> dict:
    with FIXTURE.open(encoding="utf-8") as f:
        return json.load(f)


def test_sanitized_registry_summary_totals_match_categories():
    data = _fixture()
    summary = data["summary"]

    assert sum(row["count"] for row in summary.values()) == data["totals"]["count"]
    assert math.isclose(
        sum(row["installed_capacity_mw"] for row in summary.values()),
        data["totals"]["installed_capacity_mw"],
        rel_tol=0.0,
        abs_tol=1e-6,
    )


def test_capacity_mapper_uses_registry_category_mapping():
    data = _fixture()

    for category, expected_type in data["mapping"].items():
        assert _CATEGORY_TO_TYPE[category][0] == expected_type

    assert _CATEGORY_TO_TYPE["Hydro - Seasonal"][0] == "hydro_ror"
    assert _CATEGORY_TO_TYPE["Hydro - Seasonal"][1]["_subclass"] == "seasonal_assumed_ror"
