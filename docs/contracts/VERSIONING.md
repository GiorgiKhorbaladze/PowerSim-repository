# Versioning policy

PowerSim has deliberately separate version axes:

* **Product version:** `src/powersim/version.py` is authoritative. Development identifies itself as **PowerSim 1.0.0-dev**; the PEP 440 distribution spelling is `1.0.0.dev0`. This is not a PowerSim 1.0 FINAL claim.
* **Contract version:** `1.0.0` identifies the new JSON/Python contract. It follows semantic versioning. A major version may break meaning or shape, a minor version may add backward-compatible fields, and a patch corrects non-breaking defects.
* **Workflow/model version:** records the equation/workflow implementation separately. Stage 1 uses `legacy-v4`; this does not make v4.0 the package version.

Readers fail closed on unknown contract major versions. Breaking contract changes require a new major version, an explicit reviewed migration, regenerated schemas, fixtures, and consumer contract tests. Minor changes must preserve old valid documents. Migrations never silently discard fields with possible semantic importance.

Legacy schema `1.0` through `1.5`, solver 1.x, and the historical `PowerSim v4.0` label remain readable only through the compatibility boundary. They do not determine package or current contract versions.

