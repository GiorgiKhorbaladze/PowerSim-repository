# Application architecture

```text
ChatGPT Sites / PowerSim UI
            ↕ HTTPS + authenticated commands/events
PowerSim API / backend
            ↕ application services + durable job queue
PowerSim Python package / isolated workers
            ↕ solver adapter
HiGHS / Gurobi
```

Sites is a replaceable application/control surface. It creates commands, displays sanitized data and subscribes to progress; it neither retains authoritative operational data nor executes mathematical code. The API is equally usable by another web client, CLI or automation.

## User journey

The application supports create/open project; edit/import assets, profiles, network, reserves and scenarios; validate with field-addressable issues; create a run from an immutable snapshot; monitor queue/build/solve/QA/report phases; cancel; view normalized solver status/gap and logs; inspect dispatch/KPIs/QA witnesses; compare compatible scenarios; and export JSON, tables and reports. Ordinary use requires no terminal or manual JSON relay. JSON remains available for reproducibility, bulk automation and offline exchange.

## Domain and API resources

`Project` owns metadata and references; `ProjectVersion` is immutable; `Scenario` is a versioned overlay; `ValidationReport` binds to the resolved snapshot hash; `Run` binds snapshot, workflow and settings; `SolveAttempt`, `RunEvent`, `Artifact`, `ResultSet`, `QAReport` and `Comparison` are immutable/audited resources. Mutating commands use idempotency keys and optimistic concurrency. Long operations return IDs immediately; polling and server-sent events/WebSockets expose monotonic event sequence numbers.

Run states are `draft → validating → queued → preparing → solving → validating_results → reporting → completed`, with terminal `failed|cancelled`. A completed run may still have warnings, but never invalid mandatory QA. Solver status is nested and never collapsed into run state.

## Deployment and confidentiality

Supported topology ranges from one-machine offline (local UI/API/worker/SQLite/files) to separated UI gateway, API, queue, workers, database and artifact store. A worker pulls only authorized snapshot/artifact references and writes results without routing raw profiles through Sites. Sites receives opaque project/run IDs and purpose-limited summaries. Deployments configure data locality, retention/deletion, encryption in transit/at rest, secrets vaulting, authentication/RBAC, audit events, signed download URLs, upload scanning/limits and log redaction.

No prompt, analytics event or frontend persistence may contain confidential input by default. AI assistance proposes explainable edits and summaries; all mutations are schema-validated, previewed and confirmed. The non-AI UI and offline core remain fully functional.

## Failure behavior

API restarts do not lose jobs; worker leases expire/recover; duplicate commands do not duplicate runs; partial artifacts are marked; cancellation is best-effort and explicit; reconnect replays events. Errors use stable codes, safe messages, correlation IDs and private diagnostic detail. Compatibility is negotiated by API/contract version; frontend/server mismatches fail clearly.

## Application acceptance

Contract tests bind UI/API/Python models. Integration tests use a real small solver case. Browser E2E covers project creation/import, validation, run/progress reconnect, failure/cancel, result/QA view, comparison and export. Security tests cover unauthorized object access, path traversal, malicious upload, secret/PII redaction and confirmation boundaries.
