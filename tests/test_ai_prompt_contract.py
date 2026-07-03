import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.powersim_ai_prompt import POWERSIM_AI_SYSTEM_PROMPT


def test_system_prompt_contract():
    p = POWERSIM_AI_SYSTEM_PROMPT.lower()
    assert "confirmation" in p and "running a solver" in p
    assert "secrets" in p and "api keys" in p
    assert "arbitrary shell" in p and "git" in p
    assert "georgian" in p and "english" in p and "ka" in p and "en" in p
