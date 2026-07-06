"""Deterministic intent routing helper for PowerSim AI chat."""
from __future__ import annotations

import re
from backend.powersim_ai_safety import reject_unsafe_request

INTENTS = {
    "explain_results", "compare_scenarios", "add_bess", "edit_hydro_assumption",
    "edit_reserve_requirement", "set_horizon", "run_solver", "run_adequacy",
    "export_report", "general_question", "unsafe_request",
}

_RULES = [
    ("add_bess", [r"\bbess\b", r"battery", r"ბესს", r"დაამატე.*bess"]),
    ("run_adequacy", [r"\blole\b", r"\beens\b", r"adequacy", r"ადეკვატ", r"დათვალე.*lole"]),
    ("compare_scenarios", [r"compare", r"baseline", r"შეადარე", r"საბაზის"]),
    ("edit_hydro_assumption", [r"water value", r"hydro", r"reservoir", r"წყლის ღირებულ", r"წყალსაცავი", r"ენგურ"]),
    ("edit_reserve_requirement", [r"reserve", r"რეზერვ"]),
    ("set_horizon", [r"\b\d+\s*(hours|hrs|h)\b", r"\b\d+\s*საათ"]),
    ("run_solver", [r"run\s+\d*\s*(hours|hrs|h)?", r"გაუშვი", r"solver", r"uc/ed"]),
    ("export_report", [r"report", r"export", r"ანგარიში"]),
    ("explain_results", [r"why", r"explain", r"curtailment", r"ens", r"რატომ", r"ახსენ", r"დაღვრა"]),
]


_QUESTION_MARK_RE = re.compile(r"\?")
# Question-form starters (English + Georgian) — presence + no action verb
# means the user is asking about a concept, not requesting an action.
_QUESTION_STARTERS_RE = re.compile(
    r"^\s*(what|why|how|when|where|who|which|is\s+|are\s+|does\s+|do\s+|can\s+|"
    r"რა\s+|როგორ|რატომ|რომელი|რომელი\s+)",
    re.IGNORECASE,
)
# Action verbs that override the question guard — "how do I add BESS?"
# is an action request even though it starts with "how".
_ACTION_VERBS_RE = re.compile(
    r"\b(add|run|edit|change|set|update|create|remove|delete|compare|export|"
    r"დაამატე|გაუშვი|შეადარე|შეცვალე|შექმენი|წაშალე)\b",
    re.IGNORECASE,
)


def classify_intent(message: str) -> str:
    """Classify chat text into one supported intent using deterministic rules."""
    if reject_unsafe_request(message):
        return "unsafe_request"
    text = (message or "").lower()
    matched = []
    for intent, patterns in _RULES:
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
            matched.append(intent)
    # Question-form guard: "what is LOLE?" should be a general question,
    # not run_adequacy. If the text is question-form (ends in `?` or
    # starts with a question word) AND has no action verb, return
    # general_question — the user is asking about a concept, not
    # requesting a run/edit. Prevents the classifier from firing an
    # action just because a domain keyword appears in the question.
    is_question = bool(_QUESTION_MARK_RE.search(text)
                       or _QUESTION_STARTERS_RE.match(text))
    has_action = bool(_ACTION_VERBS_RE.search(text))
    if is_question and not has_action:
        return "general_question"
    if "add_bess" in matched:
        return "add_bess"
    if "run_adequacy" in matched:
        return "run_adequacy"
    if "compare_scenarios" in matched:
        return "compare_scenarios"
    if "run_solver" in matched:
        return "run_solver"
    return matched[0] if matched else "general_question"


def route_intent(message: str) -> dict:
    """Return a small routing payload for backend/frontend orchestration."""
    intent = classify_intent(message)
    return {"intent": intent, "unsafe": intent == "unsafe_request"}
