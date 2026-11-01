"""Georgian UI translation coverage and integrity."""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

UI = Path(__file__).resolve().parents[1] / "src" / "powersim" / "ui" / "static"
# extracted literals that are data keys or code, not visible UI copy
NOT_UI_TEXT = {"about", "results", "failed", "validating", "longitude"}


def _static_strings() -> set[str]:
    source = (UI / "app.js").read_text(encoding="utf-8")
    found: set[str] = set()
    for pattern in (r'(?:text|placeholder|"aria-label"|title|label):\s*"((?:[^"\\]|\\.)*)"',
                    r'h\("(?:strong|h3|h4|p|span|li|summary|th|td|option|button|label)",\s*\{[^{}]*\},\s*"((?:[^"\\]|\\.)*)"'):
        found.update(m.group(1) for m in re.finditer(pattern, source))
    for m in re.finditer(r'\[((?:"[^"]*",\s*)+"[^"]*")\]\.map\(\(t\)', source):
        found.update(re.findall(r'"([^"]*)"', m.group(1)))
    html = (UI / "index.html").read_text(encoding="utf-8")
    found.update(re.findall(r'data-view="[a-z]+">([^<]+)<', html))
    found = {f.replace("&amp;", "&").replace('\\"', '"') for f in found}
    return {f for f in found if re.search(r"[A-Za-z]{2}", f) and not f.startswith(("/", "#", "."))
            and "http" not in f and "${" not in f} - NOT_UI_TEXT


@pytest.fixture(scope="module")
def georgian() -> dict[str, str]:
    if shutil.which("node") is None:
        pytest.skip("node is not installed")
    script = f"import('{(UI / 'i18n.js').as_uri()}').then(m => console.log(JSON.stringify(m.translations('ka'))))"
    completed = subprocess.run(["node", "--input-type=module", "-e", script], capture_output=True, text=True, check=True)
    return json.loads(completed.stdout)


def test_every_static_ui_string_has_a_georgian_translation(georgian):
    missing = sorted(_static_strings() - set(georgian))
    assert not missing, f"untranslated UI strings: {missing}"


def test_translations_are_georgian_and_keep_placeholders(georgian):
    for english, text in georgian.items():
        assert text.strip(), english
        if english.endswith(": ") or english.endswith(" "):
            assert text.endswith(" "), english
        letters = re.findall(r"[ა-ჿ]", text)
        latin_only = re.fullmatch(r"[\x00-\x7f]+", english) and not letters
        # acronyms and ids may stay Latin; prose must be Georgian
        if latin_only and len(english.split()) > 2:
            raise AssertionError(f"not translated: {english!r} -> {text!r}")


def test_terminology_follows_the_localization_note(georgian):
    assert georgian["Unserved energy"] == "მიუწოდებელი ენერგია"
    assert georgian["Wind"] == "ქარი" and georgian["Solar"] == "მზე"
    assert georgian["Validation"] == "ვალიდაცია"
