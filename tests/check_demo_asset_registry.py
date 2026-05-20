"""Validate the 2026 PowerSim demo installed-capacity registry."""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "samples" / "demo_asset_registry_2026.json"

EXPECTED_TOTAL = Decimal("4770.1935")
EXPECTED_SUMMARY = {
    "Hydro - Reservoir": {
        "count": 7,
        "installed_capacity_mw": Decimal("1993.12"),
        "powersim_type": "hydro_reg",
    },
    "Hydro - Seasonal": {
        "count": 24,
        "installed_capacity_mw": Decimal("1214.21"),
        "powersim_type": "hydro_ror",
    },
    "Hydro - Small": {
        "count": 85,
        "installed_capacity_mw": Decimal("331.2635"),
        "powersim_type": "hydro_ror",
    },
    "Solar": {
        "count": 6,
        "installed_capacity_mw": Decimal("10.75"),
        "powersim_type": "solar",
    },
    "Thermal": {
        "count": 7,
        "installed_capacity_mw": Decimal("1181.4"),
        "powersim_type": "thermal",
    },
    "Wind": {
        "count": 2,
        "installed_capacity_mw": Decimal("39.45"),
        "powersim_type": "wind",
    },
}


def _capacity(value: object) -> Decimal:
    return Decimal(str(value))


def validate_demo_asset_registry() -> list[str]:
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    assets = registry.get("assets", [])
    errors: list[str] = []

    if len(assets) != 131:
        errors.append(f"expected 131 assets, found {len(assets)}")

    asset_ids = [asset.get("asset_id") for asset in assets]
    duplicate_ids = sorted(
        asset_id for asset_id, count in Counter(asset_ids).items() if count > 1
    )
    if duplicate_ids:
        errors.append(f"duplicate asset_id values: {duplicate_ids}")

    missing_capacity = [
        asset.get("asset_id")
        for asset in assets
        if "installed_capacity_mw" not in asset
    ]
    if missing_capacity:
        errors.append(f"missing installed_capacity_mw values: {missing_capacity}")

    negative_capacity = [
        asset.get("asset_id")
        for asset in assets
        if "installed_capacity_mw" in asset
        and _capacity(asset["installed_capacity_mw"]) < Decimal("0")
    ]
    if negative_capacity:
        errors.append(f"negative installed_capacity_mw values: {negative_capacity}")

    total = sum(
        (_capacity(asset["installed_capacity_mw"]) for asset in assets), Decimal("0")
    )
    if total != EXPECTED_TOTAL:
        errors.append(f"expected total capacity {EXPECTED_TOTAL} MW, found {total} MW")

    by_category: dict[str, dict[str, object]] = defaultdict(
        lambda: {"count": 0, "installed_capacity_mw": Decimal("0"), "types": set()}
    )
    for asset in assets:
        category = asset.get("source_category")
        summary = by_category[str(category)]
        summary["count"] = int(summary["count"]) + 1
        summary["installed_capacity_mw"] = summary["installed_capacity_mw"] + _capacity(
            asset["installed_capacity_mw"]
        )
        summary["types"].add(asset.get("powersim_type"))

    if set(by_category) != set(EXPECTED_SUMMARY):
        errors.append(
            "category set mismatch: "
            f"expected {sorted(EXPECTED_SUMMARY)}, found {sorted(by_category)}"
        )

    for category, expected in EXPECTED_SUMMARY.items():
        actual = by_category.get(category)
        if actual is None:
            continue
        if actual["count"] != expected["count"]:
            errors.append(
                f"{category}: expected count {expected['count']}, found {actual['count']}"
            )
        if actual["installed_capacity_mw"] != expected["installed_capacity_mw"]:
            errors.append(
                f"{category}: expected capacity {expected['installed_capacity_mw']} MW, "
                f"found {actual['installed_capacity_mw']} MW"
            )
        if actual["types"] != {expected["powersim_type"]}:
            errors.append(
                f"{category}: expected PowerSim type {expected['powersim_type']!r}, "
                f"found {sorted(actual['types'])}"
            )

    return errors


def test_demo_asset_registry_contract() -> None:
    assert validate_demo_asset_registry() == []


def main() -> int:
    errors = validate_demo_asset_registry()
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print(f"{REGISTRY_PATH.relative_to(ROOT)} validation passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
