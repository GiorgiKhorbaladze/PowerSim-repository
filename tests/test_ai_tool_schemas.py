import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.powersim_ai_tool_schemas import TOOL_SCHEMA_BY_NAME


def test_all_required_tool_schemas_present():
    required = {
        "load_input_summary", "summarize_results", "compare_results", "propose_add_bess",
        "propose_edit_asset", "propose_set_horizon", "run_solver", "run_adequacy",
        "export_report_summary",
    }
    assert required <= set(TOOL_SCHEMA_BY_NAME)


def test_safety_classifications():
    for name in ["run_solver", "run_adequacy", "export_report_summary"]:
        assert TOOL_SCHEMA_BY_NAME[name]["safety_classification"] == "requires_confirmation"
    for name in ["load_input_summary", "summarize_results", "compare_results"]:
        assert TOOL_SCHEMA_BY_NAME[name]["safety_classification"] == "read_only"


def test_parameters_contain_required_keys():
    for tool in TOOL_SCHEMA_BY_NAME.values():
        assert "parameters" in tool and "required" in tool["parameters"]
        assert "name" in tool and "description" in tool and "examples" in tool
