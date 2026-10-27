"""Locate bundled non-Python resources in a checkout, wheel or frozen build.

Resources are looked up in this order:

1. a PyInstaller bundle (``sys._MEIPASS``),
2. the source checkout (``<repo>/<relative path>``),
3. the wheel's data-files location (``<prefix>/powersim/<relative path>``).
"""
from __future__ import annotations

import sys
import sysconfig
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: Sample projects the application can load into a workspace.
SAMPLE_PROJECTS: dict[str, dict[str, str]] = {
    "georgia_2026_baseline": {
        "path": "samples/projects/georgia_2026_baseline.json",
        "name": "Georgia 2026 baseline (sanitized, aggregated)",
        "description": ("8,760-hour deterministic UC/ED demonstration with 2027-2030 demand scenarios. "
                        "Two aggregated dispatch resources; not official GSE operational data."),
    },
    "application_compact_demo": {
        "path": "samples/application_compact_demo.json",
        "name": "Compact 4-hour demonstration",
        "description": "Two-resource, 4-period deterministic case for a quick end-to-end check.",
    },
}

REFERENCE_CATALOGUE = "data/reference/plant_parameters"


def resource_path(relative: str) -> Path:
    """Return the first existing location of a bundled file or directory."""
    candidates = []
    frozen = getattr(sys, "_MEIPASS", None)
    if frozen:
        candidates.append(Path(frozen) / relative)
    candidates.append(REPOSITORY_ROOT / relative)
    candidates.append(Path(sysconfig.get_path("data")) / "powersim" / relative)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]
