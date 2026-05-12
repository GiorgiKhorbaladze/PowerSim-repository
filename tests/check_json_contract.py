"""
PowerSim JSON contract regression checks.

Fast, deterministic checks for CI that do not run the optimizer:
  * committed sample input/result JSON still validates against Python schema,
  * the HTML output validator accepts committed sample result versions, and
  * the HTML SCHEMA_VERSION constant matches the Python SCHEMA_VERSION exactly
    (catches the v1.2 vs v1.4 class of silent round-trip breakage).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "schema", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_schema import (  # noqa: E402
    ACCEPTED_SCHEMA_PRIOR,
    SCHEMA_VERSION,
    validate_input,
    validate_output,
)


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _metadata_schema_version(path: Path) -> str | None:
    data = _load_json(path)
    meta = data.get("metadata") if isinstance(data, dict) else None
    return meta.get("schema_version") if isinstance(meta, dict) else None


def check_sample_inputs() -> list[str]:
    errors: list[str] = []
    for path in sorted((ROOT / "samples").glob("sample_input*.json")):
        ok, errs, _warns = validate_input(_load_json(path))
        if not ok:
            errors.append(f"{path.relative_to(ROOT)} failed validate_input: {errs}")
    return errors


def check_sample_outputs() -> list[str]:
    errors: list[str] = []
    for path in sorted((ROOT / "samples").glob("sample_results*.json")):
        ok, errs, _warns = validate_output(_load_json(path))
        if not ok:
            errors.append(f"{path.relative_to(ROOT)} failed validate_output: {errs}")
    return errors


def check_html_schema_sync() -> list[str]:
    """
    Fail if the HTML SCHEMA_VERSION constant diverges from the Python one.
    This is the primary guard against the 'v1.2 vs v1.4' class of silent
    round-trip breakage where the solver emits a version the HTML rejects.
    """
    errors: list[str] = []
    html_path = ROOT / "html" / "PowerSim_v4.html"
    text = html_path.read_text(encoding="utf-8")

    version_match = re.search(r"const\s+SCHEMA_VERSION\s*=\s*['\"]([^'\"]+)['\"]", text)
    if not version_match:
        return ["html/PowerSim_v4.html missing const SCHEMA_VERSION"]
    html_schema_version = version_match.group(1)

    if html_schema_version != SCHEMA_VERSION:
        errors.append(
            f"HTML SCHEMA_VERSION '{html_schema_version}' != "
            f"Python SCHEMA_VERSION '{SCHEMA_VERSION}' — "
            "solver output will be rejected by HTML importResults()"
        )

    accepted_match = re.search(
        r"const\s+ACCEPTED_OUTPUT_SCHEMAS\s*=\s*new\s+Set\(\s*\[([^\]]*)\]\s*\)",
        text,
        flags=re.S,
    )
    if not accepted_match:
        errors.append("html/PowerSim_v4.html missing ACCEPTED_OUTPUT_SCHEMAS")
        return errors

    html_accepted = set(re.findall(r"['\"]([^'\"]+)['\"]", accepted_match.group(1)))

    # Python SCHEMA_VERSION must be accepted (either as the primary or in the set).
    if SCHEMA_VERSION not in html_accepted and SCHEMA_VERSION != html_schema_version:
        errors.append(
            f"HTML ACCEPTED_OUTPUT_SCHEMAS does not include Python "
            f"SCHEMA_VERSION '{SCHEMA_VERSION}'"
        )

    # All committed sample result versions must be accepted too.
    sample_versions = {
        sv
        for path in (ROOT / "samples").glob("sample_results*.json")
        if (sv := _metadata_schema_version(path))
    }
    missing_samples = sample_versions - html_accepted - {html_schema_version}
    if missing_samples:
        errors.append(
            "HTML output validator rejects committed sample result schema versions: "
            f"{sorted(missing_samples)}"
        )

    return errors


def main() -> int:
    errors = []
    errors.extend(check_sample_inputs())
    errors.extend(check_sample_outputs())
    errors.extend(check_html_schema_sync())

    if errors:
        print("JSON contract checks FAILED:")
        for err in errors:
            print(f"  - {err}")
        return 1

    print("JSON contract checks passed.")
    print(f"  Python schema version: {SCHEMA_VERSION}")
    print(f"  Python accepted prior versions: {sorted(ACCEPTED_SCHEMA_PRIOR)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
