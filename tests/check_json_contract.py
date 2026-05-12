"""
PowerSim JSON contract regression checks.

Fast, deterministic checks for CI that do not run the optimizer:
  * committed sample input/result JSON still validates against Python schema,
  * HTML and Python active schema constants stay in lockstep, and
  * the HTML output validator accepts committed sample result versions.
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

HTML_PATH = ROOT / "html" / "PowerSim_v4.html"


def _load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _metadata_schema_version(path: Path) -> str | None:
    data = _load_json(path)
    meta = data.get("metadata") if isinstance(data, dict) else None
    return meta.get("schema_version") if isinstance(meta, dict) else None


def _html_schema_contract() -> tuple[str | None, set[str], list[str]]:
    """Extract the HTML schema constants that gate import/export compatibility."""
    errors: list[str] = []
    text = HTML_PATH.read_text(encoding="utf-8")

    version_match = re.search(
        r"const\s+SCHEMA_VERSION\s*=\s*['\"]([^'\"]+)['\"]", text
    )
    html_schema_version = version_match.group(1) if version_match else None
    if html_schema_version is None:
        errors.append("html/PowerSim_v4.html missing const SCHEMA_VERSION")

    accepted_match = re.search(
        r"const\s+ACCEPTED_OUTPUT_SCHEMAS\s*=\s*new\s+Set\(\s*\[([^\]]*)\]\s*\)",
        text,
        flags=re.S,
    )
    html_accepted = (
        set(re.findall(r"['\"]([^'\"]+)['\"]", accepted_match.group(1)))
        if accepted_match
        else set()
    )
    if not accepted_match:
        errors.append("html/PowerSim_v4.html missing ACCEPTED_OUTPUT_SCHEMAS")

    return html_schema_version, html_accepted, errors


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


def check_html_python_schema_sync() -> list[str]:
    """Fail CI when the browser export schema drifts from the Python schema."""
    html_schema_version, html_accepted, errors = _html_schema_contract()
    if errors:
        return errors

    if html_schema_version != SCHEMA_VERSION:
        errors.append(
            "HTML SCHEMA_VERSION does not match Python SCHEMA_VERSION: "
            f"html={html_schema_version!r}, python={SCHEMA_VERSION!r}"
        )

    expected_accepted = set(ACCEPTED_SCHEMA_PRIOR) | {SCHEMA_VERSION}
    if html_accepted != expected_accepted:
        errors.append(
            "HTML ACCEPTED_OUTPUT_SCHEMAS does not match "
            "Python active/prior schema set: "
            f"html={sorted(html_accepted)}, expected={sorted(expected_accepted)}"
        )

    return errors


def check_html_sample_result_versions() -> list[str]:
    errors: list[str] = []
    _html_schema_version, html_accepted, extract_errors = _html_schema_contract()
    if extract_errors:
        return extract_errors

    sample_versions = {
        sv
        for path in (ROOT / "samples").glob("sample_results*.json")
        if (sv := _metadata_schema_version(path))
    }
    missing_samples = sample_versions - html_accepted
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
    errors.extend(check_html_python_schema_sync())
    errors.extend(check_html_sample_result_versions())

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
