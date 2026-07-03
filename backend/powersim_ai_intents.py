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


def classify_intent(message: str) -> str:
    """Classify chat text into one supported intent using deterministic rules."""
    if reject_unsafe_request(message):
        return "unsafe_request"
    text = (message or "").lower()
    matched = []
    for intent, patterns in _RULES:
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
            matched.append(intent)
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
