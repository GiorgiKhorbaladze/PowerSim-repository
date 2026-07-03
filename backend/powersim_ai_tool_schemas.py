"""Importable tool schema registry for PowerSim AI backend orchestration."""

READ_ONLY = "read_only"
PROPOSE_CHANGE = "propose_change"
REQUIRES_CONFIRMATION = "requires_confirmation"

TOOL_SCHEMAS = [
    {
        "name": "load_input_summary",
        "description": "Load a read-only summary of the active PowerSim input JSON.",
        "safety_classification": READ_ONLY,
        "parameters": {"type": "object", "properties": {"input_id": {"type": "string"}}, "required": ["input_id"]},
        "examples": [{"input_id": "active"}],
    },
    {
        "name": "summarize_results",
        "description": "Summarize UC/ED, dispatch, BESS, hydro, reserve, gas, and ENS result metrics.",
        "safety_classification": READ_ONLY,
        "parameters": {"type": "object", "properties": {"result_id": {"type": "string"}}, "required": ["result_id"]},
        "examples": [{"result_id": "latest"}],
    },
    {
        "name": "compare_results",
        "description": "Compare two result sets or scenarios without modifying inputs.",
        "safety_classification": READ_ONLY,
        "parameters": {"type": "object", "properties": {"base_result_id": {"type": "string"}, "candidate_result_id": {"type": "string"}}, "required": ["base_result_id", "candidate_result_id"]},
        "examples": [{"base_result_id": "baseline", "candidate_result_id": "bess_500mw"}],
    },
    {
        "name": "propose_add_bess",
        "description": "Prepare a proposed BESS addition for user review; does not write files.",
        "safety_classification": PROPOSE_CHANGE,
        "parameters": {"type": "object", "properties": {"mw": {"type": "number"}, "mwh": {"type": "number"}, "bus": {"type": "string"}}, "required": ["mw", "mwh"]},
        "examples": [{"mw": 500, "mwh": 2000, "bus": "Tbilisi"}],
    },
    {
        "name": "propose_edit_asset",
        "description": "Prepare a proposed asset/input edit for confirmation; does not write files.",
        "safety_classification": PROPOSE_CHANGE,
        "parameters": {"type": "object", "properties": {"asset_id": {"type": "string"}, "field": {"type": "string"}, "value": {}}, "required": ["asset_id", "field", "value"]},
        "examples": [{"asset_id": "enguri", "field": "water_value", "value": 50}],
    },
    {
        "name": "propose_set_horizon",
        "description": "Prepare a proposed simulation horizon change for confirmation.",
        "safety_classification": PROPOSE_CHANGE,
        "parameters": {"type": "object", "properties": {"hours": {"type": "integer", "minimum": 1}}, "required": ["hours"]},
        "examples": [{"hours": 720}],
    },
    {
        "name": "run_solver",
        "description": "Run the UC/ED solver after explicit user confirmation.",
        "safety_classification": REQUIRES_CONFIRMATION,
        "parameters": {"type": "object", "properties": {"input_id": {"type": "string"}, "hours": {"type": "integer"}}, "required": ["input_id"]},
        "examples": [{"input_id": "active", "hours": 720}],
    },
    {
        "name": "run_adequacy",
        "description": "Run adequacy metrics such as LOLE and EENS after confirmation.",
        "safety_classification": REQUIRES_CONFIRMATION,
        "parameters": {"type": "object", "properties": {"input_id": {"type": "string"}, "samples": {"type": "integer"}}, "required": ["input_id"]},
        "examples": [{"input_id": "active", "samples": 1000}],
    },
    {
        "name": "export_report_summary",
        "description": "Export a report summary file after explicit user confirmation.",
        "safety_classification": REQUIRES_CONFIRMATION,
        "parameters": {"type": "object", "properties": {"result_id": {"type": "string"}, "format": {"type": "string", "enum": ["md", "json", "pdf"]}}, "required": ["result_id", "format"]},
        "examples": [{"result_id": "latest", "format": "md"}],
    },
]

TOOL_SCHEMA_BY_NAME = {tool["name"]: tool for tool in TOOL_SCHEMAS}
