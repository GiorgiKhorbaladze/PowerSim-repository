from __future__ import annotations
import argparse
import json
from pathlib import Path
from pydantic import ValidationError
from powersim.contracts.project import ProjectContract, resolve_scenario
from powersim.validation.contract_validation import pydantic_issues, validate_contract
from powersim.validation.legacy_adapter import adapt_legacy_input
from powersim.version import CONTRACT_VERSION, PRODUCT_VERSION


def _validate(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if "time_index" in data or (data.get("metadata") or {}).get("schema_version") in {"1.0","1.1","1.2","1.3","1.4","1.5"}:
            result = adapt_legacy_input(data)
            issues = result.issues
        else:
            project = ProjectContract.model_validate(data)
            issues = validate_contract(project)
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"valid": False, "issues": [{"code":"input_read_error","severity":"error","path":"","message":str(exc),"context":{}}]}))
        return 2
    except ValidationError as exc:
        issues = pydantic_issues(exc)
    output = {"valid": not any(i.severity.value == "error" for i in issues),
              "contract_version": CONTRACT_VERSION,
              "issues": [i.model_dump(mode="json") for i in issues]}
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0 if output["valid"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="powersim")
    parser.add_argument("--version", action="version", version=f"PowerSim {PRODUCT_VERSION}")
    sub = parser.add_subparsers(dest="command")
    validate = sub.add_parser("validate", help="validate a current or legacy input JSON")
    validate.add_argument("input", type=Path)
    args = parser.parse_args(argv)
    if args.command == "validate":
        return _validate(args.input)
    parser.print_help()
    return 0

