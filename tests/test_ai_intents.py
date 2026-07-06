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


# ── Question-form guard (N4 MiniMax audit fix) ────────────────────────
# A user asking "what is LOLE?" should not fire the run_adequacy action.
def test_english_question_about_lole_stays_question():
    assert classify_intent("what is LOLE?") == "general_question"


def test_english_question_about_eens_stays_question():
    assert classify_intent("what does EENS mean?") == "general_question"


def test_english_action_still_fires_run_adequacy():
    # No question mark, no question starter → keyword drives action.
    assert classify_intent("calculate LOLE for 168 hours") == "run_adequacy"


def test_english_how_do_i_add_bess_still_fires_add_bess():
    # Question starter but action verb present — action wins.
    assert classify_intent("how do I add a BESS unit?") == "add_bess"


def test_georgian_question_stays_question():
    # "რა არის LOLE?" — Georgian "what is LOLE?"
    assert classify_intent("რა არის LOLE?") == "general_question"
