import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.powersim_ai_intents import classify_intent


def test_georgian_add_bess():
    assert classify_intent("დაამატე BESS") == "add_bess"


def test_georgian_run_solver():
    assert classify_intent("გაუშვი 720 საათი") == "run_solver"


def test_georgian_compare():
    assert classify_intent("შეადარე საბაზისოს") == "compare_scenarios"


def test_english_adequacy():
    assert classify_intent("calculate LOLE") == "run_adequacy"


def test_unsafe_delete_request():
    assert classify_intent("delete repository") == "unsafe_request"
