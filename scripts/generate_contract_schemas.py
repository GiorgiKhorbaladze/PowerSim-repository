"""Generate the checked-in PowerSim v1 JSON Schemas from authoritative models."""
from __future__ import annotations
import json
from pathlib import Path
from powersim.contracts import ProjectContract, QAReport, ResolvedInputContract, ResultEnvelope, RunContract, SolverDiagnostics

MODELS = {
    "project": ProjectContract,
    "resolved-input": ResolvedInputContract,
    "solver-diagnostics": SolverDiagnostics,
    "result-envelope": ResultEnvelope,
    "qa-report": QAReport,
    "run": RunContract,
}

def main() -> None:
    output = Path("schemas/v1")
    output.mkdir(parents=True, exist_ok=True)
    for name, model in MODELS.items():
        (output / f"{name}.schema.json").write_text(
            json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()

