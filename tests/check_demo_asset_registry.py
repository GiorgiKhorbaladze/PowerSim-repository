"""Validate the reconciled 2026 Georgia demo installed-capacity registry.

The registry is intentionally JSON-only: do not commit Giorgi's original
Excel/image/source workbook.  This check verifies the public demo registry can
be audited back to the canonical installed-capacity display total.
"""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "samples" / "demo_asset_registry_2026.json"

EXPECTED_ASSET_COUNT = 131
EXPECTED_DISPLAY_TOTAL_MW = 4770.194
EXPECTED_CATEGORY_COUNTS = {
    "Hydro - Reservoir": 7,
    "Hydro - Seasonal": 24,
    "Hydro - Small": 85,
    "Solar": 6,
    "Thermal": 7,
    "Wind": 2,
}
EXPECTED_RECONCILED_SUBTOTALS_MW = {
    "Hydro - Reservoir": 1993.12,
    "Hydro - Seasonal": 1214.21,
    "Hydro - Small": 331.2635,
    "Solar": 10.75,
    "Thermal": 1181.4,
    "Wind": 39.45,
}
EXPECTED_TYPE_MAPPING = {
    "Hydro - Reservoir": "hydro_reg",
    "Hydro - Seasonal": "hydro_ror",
    "Hydro - Small": "hydro_ror",
    "Solar": "solar",
    "Thermal": "thermal",
    "Wind": "wind",
}


def load_registry() -> dict:
    with REGISTRY_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def category_totals(assets: list[dict]) -> tuple[dict[str, int], dict[str, float]]:
    counts: dict[str, int] = defaultdict(int)
    totals: dict[str, float] = defaultdict(float)
    for asset in assets:
        category = asset.get("source_category")
        counts[category] += 1
        totals[category] += float(asset.get("installed_capacity_mw", 0))
    return dict(counts), {k: round(v, 4) for k, v in totals.items()}


def _row_capacity_total(assets: list[dict]) -> float:
    return round(sum(float(a["installed_capacity_mw"]) for a in assets), 4)


def _assert_registry_valid(registry: dict) -> list[str]:
    metadata = registry.get("metadata") or {}
    assets = registry.get("assets") or []
    notes: list[str] = []

    assert metadata.get("source_total_mw_display") == EXPECTED_DISPLAY_TOTAL_MW
    assert metadata.get("source_basis") == "Giorgi-provided 2026 installed capacity table"
    assert len(assets) == EXPECTED_ASSET_COUNT
    assert metadata.get("asset_count") == EXPECTED_ASSET_COUNT

    ids = [asset.get("asset_id") for asset in assets]
    duplicates = sorted(asset_id for asset_id, count in Counter(ids).items() if count > 1)
    assert not duplicates, f"duplicate asset_id values: {duplicates}"
    assert all(ids), "all assets must include asset_id"

    invalid_capacity = [
        asset.get("asset_id")
        for asset in assets
        if not isinstance(asset.get("installed_capacity_mw"), (int, float))
        or asset.get("installed_capacity_mw") <= 0
    ]
    assert not invalid_capacity, f"missing/non-positive capacities: {invalid_capacity}"

    counts, totals = category_totals(assets)
    assert counts == EXPECTED_CATEGORY_COUNTS
    assert metadata.get("powersim_type_mapping") == EXPECTED_TYPE_MAPPING
    for asset in assets:
        category = asset["source_category"]
        assert asset["powersim_type"] == EXPECTED_TYPE_MAPPING[category]

    reconciled = metadata.get("reconciled_subtotals_mw") or {}
    for category, expected in EXPECTED_RECONCILED_SUBTOTALS_MW.items():
        assert math.isclose(totals[category], expected, rel_tol=0, abs_tol=1e-6), (
            category,
            totals[category],
            expected,
        )
        assert math.isclose(float(reconciled[category]), expected, rel_tol=0, abs_tol=1e-6)

    hydro_total = sum(totals[c] for c in ("Hydro - Reservoir", "Hydro - Seasonal", "Hydro - Small"))
    assert math.isclose(hydro_total, float(reconciled["Total Hydro"]), rel_tol=0, abs_tol=1e-3)
    assert math.isclose(float(reconciled["Total Hydro"]), 3538.594, rel_tol=0, abs_tol=1e-6)

    row_total = _row_capacity_total(assets)
    assert math.isclose(row_total, float(metadata["row_capacity_total_mw"]), rel_tol=0, abs_tol=1e-6)
    assert round(row_total, 3) == EXPECTED_DISPLAY_TOTAL_MW

    reconciliation_note = metadata.get("reconciliation_note", "")
    if not math.isclose(row_total, EXPECTED_DISPLAY_TOTAL_MW, rel_tol=0, abs_tol=1e-9):
        assert reconciliation_note, "reconciliation_note is required when row sum differs from display total"
        notes.append(
            f"Reconciliation: row sum {row_total:.4f} MW rounds to display total "
            f"{EXPECTED_DISPLAY_TOTAL_MW:.3f} MW. {reconciliation_note}"
        )

    warnings = "\n".join(metadata.get("warnings") or [])
    assert "მტკვარიჰესი" in warnings and "Hydro - Seasonal" in warnings
    assert any(
        asset["asset_id"] == "hydro_seasonal_024"
        and asset["name_ge"] == "მტკვარიჰესი"
        and asset["source_category"] == "Hydro - Seasonal"
        for asset in assets
    )
    assert "გამარჯვების მზის საგდური" in warnings

    return notes


def test_demo_asset_registry_is_reconciled_to_authoritative_total():
    _assert_registry_valid(load_registry())


def main() -> int:
    registry = load_registry()
    notes = _assert_registry_valid(registry)
    print("Demo asset registry checks passed.")
    print(f"  Registry: {REGISTRY_PATH.relative_to(ROOT)}")
    print(f"  Asset count: {len(registry['assets'])}")
    print(f"  System installed capacity display total: {EXPECTED_DISPLAY_TOTAL_MW:.3f} MW")
    for note in notes:
        print(f"  {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
