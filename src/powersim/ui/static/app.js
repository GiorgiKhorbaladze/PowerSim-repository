// PowerSim 1.0 browser application.
//
// The UI is a thin client of the same-origin application API under /api.
// It never computes model results: validation, run lifecycle, QA,
// publication gating and reference-data review all happen server-side.
// Untrusted text is always inserted with textContent, never as HTML.

const TERMINAL = new Set(["completed", "failed", "cancelled"]);
const CANCELLABLE = new Set(["draft", "validating", "queued"]);
const ASSET_KINDS = ["thermal", "import", "hydro_ror", "hydro_reg", "pumped_hydro", "solar", "wind", "bess", "dr"];
const REFERENCE_SOURCE = "powersim.reference_catalogue";
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const OVERRIDE_FIELDS = [
  ["id", "Asset id"], ["kind", "Component kind"], ["name", "Name"],
  ["capacity_max_mw", "Max capacity (MW)"], ["capacity_min_mw", "Min capacity (MW)"], ["fuel_type", "Fuel type"],
];
const KPI_FIELDS = [
  ["total_energy_mwh", "Energy served", "MWh"], ["total_objective_cost_usd", "Objective cost", "USD"],
  ["total_cost_usd", "System cost", "USD"], ["avg_lambda_usd_mwh", "Average price", "USD/MWh"],
  ["peak_load_mw", "Peak load", "MW"], ["total_unserved_mwh", "Unserved energy", "MWh"],
  ["total_curtailed_mwh", "Curtailment", "MWh"], ["total_gas_mm3", "Gas use", "Mm3"],
  ["total_co2_t", "CO2", "t"],
];

const S = {
  view: "overview", system: null, samples: [], projects: [], runs: [],
  project: null, savedJson: null, validation: null, editingAsset: null, documentOpen: false,
  selectedRunId: null, resultsRunId: null, summaries: new Map(), lastRunError: null, poller: null,
  compare: null,
  ref: { summary: null, table: "stations", q: "", data: null, selected: new Map(), review: null,
         overrides: {}, quality: null, detail: null, qualitySeverity: "error", importResult: null },
};

// ---------------------------------------------------------------- helpers
function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key === "text") node.textContent = value;
    else if (key.startsWith("on") && typeof value === "function") node.addEventListener(key.slice(2), value);
    else if (key === "dataset") Object.assign(node.dataset, value);
    else if (value === true) node.setAttribute(key, "");
    else node.setAttribute(key, String(value));
  }
  append(node, children);
  return node;
}
function append(node, children) {
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}
const $ = (selector, root = document) => root.querySelector(selector);
function num(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value !== "number") return String(value);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(value);
}
function badge(text, kind) { return h("span", { class: `badge badge-${kind}`, text }); }
function statusKind(status) {
  return { completed: "ok", failed: "err", cancelled: "neutral", draft: "neutral" }[status] || "info";
}
function severityKind(severity) {
  return { error: "err", warning: "warn", info: "info", resolved: "ok" }[severity] || "neutral";
}
function clear(node) { while (node.firstChild) node.removeChild(node.firstChild); return node; }
function toast(message, kind = "info") {
  const box = clear($("#toast"));
  box.append(h("div", { class: `toast-${kind}`, text: message }));
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => clear(box), 6000);
}
function workingJson() { return S.project ? JSON.stringify(S.project) : null; }
function isDirty() { return S.project !== null && workingJson() !== S.savedJson; }
function currentValidation() {
  return S.validation && S.validation.forJson === workingJson() ? S.validation.result : null;
}
function shortHash(value) { return value ? String(value).replace("sha256:", "").slice(0, 12) : "-"; }

class ApiFailure extends Error {
  constructor(status, detail) {
    super(detail.message || `request failed (${status})`);
    this.status = status; this.code = detail.code; this.issues = detail.issues || [];
  }
}
async function api(path, { method = "GET", body } = {}) {
  const init = { method, cache: "no-store", headers: {} };
  if (body !== undefined) { init.headers["Content-Type"] = "application/json"; init.body = JSON.stringify(body); }
  let response;
  try { response = await fetch(`/api${path}`, init); }
  catch (error) { throw new ApiFailure(0, { code: "backend_unreachable", message: "The PowerSim backend is not reachable." }); }
  const text = await response.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { detail: text }; }
  if (!response.ok) {
    let detail = data && data.detail;
    if (Array.isArray(detail)) {
      detail = { code: "request_invalid", message: "The request was rejected by the API.",
                 issues: detail.map((d) => ({ severity: "error", code: d.type, path: (d.loc || []).join("."), message: d.msg })) };
    } else if (!detail || typeof detail !== "object") {
      detail = { code: `http_${response.status}`, message: typeof detail === "string" ? detail : response.statusText };
    }
    throw new ApiFailure(response.status, detail);
  }
  return data;
}

function issuesTable(issues, { id } = {}) {
  if (!issues || !issues.length) return h("p", { class: "muted", text: "No issues." });
  const rows = issues.map((issue) => h("tr", {},
    h("td", {}, badge(issue.severity, severityKind(issue.severity))),
    h("td", {}, h("code", { text: issue.path || issue.field || "-" })),
    h("td", {}, h("code", { text: issue.code })),
    h("td", { text: issue.message }),
    h("td", { class: "help", text: issue.remediation || issue.resolution || "" })));
  return h("div", { class: "table-wrap" }, h("table", { id },
    h("thead", {}, h("tr", {}, ["Severity", "Path", "Code", "Message", "Remediation"].map((t) => h("th", { scope: "col", text: t })))),
    h("tbody", {}, rows)));
}
function errorPanel(error, title) {
  const panel = h("div", { class: "error-panel", role: "alert" },
    h("strong", { text: title || "Request failed" }), h("p", { text: error.message }));
  if (error.code) panel.append(h("p", { class: "help" }, "Code: ", h("code", { text: error.code })));
  if (error.issues && error.issues.length) panel.append(issuesTable(error.issues));
  return panel;
}
function needProject(section, heading) {
  append(section, [h("h2", { id: `h-${S.view}`, text: heading }),
    h("div", { class: "empty" }, h("p", { text: "No project is open." }),
      h("button", { type: "button", onclick: () => show("overview"), text: "Open or create a project" }))]);
}
function download(path) {
  const link = h("a", { href: `/api${path}`, download: "", hidden: true });
  document.body.append(link); link.click(); link.remove();
}

// ---------------------------------------------------------------- status
function updateStatus() {
  const status = $("#project-status");
  let text = "No project open", kind = "neutral";
  if (S.project) {
    const validation = currentValidation();
    const dirty = isDirty();
    if (validation && !validation.contract_valid) { text = "Validation failed"; kind = "err"; }
    else if (dirty) { text = validation ? (validation.run_ready ? "Draft - valid, unsaved" : "Draft - not run-ready, unsaved") : "Draft - unsaved changes"; kind = "warn"; }
    else if (validation) { text = validation.run_ready ? "Saved - valid, run-ready" : "Saved - not run-ready"; kind = validation.run_ready ? "ok" : "warn"; }
    else { text = "Saved - not validated"; kind = "info"; }
  }
  status.textContent = text;
  status.className = `badge badge-${kind}`;
  $("#nav-project").textContent = S.project ? `Project: ${S.project.metadata?.name || S.project.id}` : "No project open";
  const actions = $("#project-actions");
  actions.hidden = !S.project || ["overview", "about", "reference"].includes(S.view);
  $("#dirty-indicator").textContent = S.project ? (isDirty() ? "Unsaved changes" : "All changes saved") : "";
  const exportLink = $("#export-project");
  if (S.project && !isDirty()) {
    exportLink.href = `/api/projects/${encodeURIComponent(S.project.id)}/export`;
    exportLink.removeAttribute("aria-disabled"); exportLink.title = "Download the saved project document";
  } else {
    exportLink.removeAttribute("href"); exportLink.setAttribute("aria-disabled", "true");
    exportLink.title = "Save the project before exporting it";
  }
  if (S.system) {
    const ws = S.system.workspace;
    $("#workspace-status").textContent = `${ws.name} - ${ws.projects} project(s), ${ws.runs} run(s)`;
  }
}

async function refreshSystem() {
  try {
    S.system = await api("/system");
    const highs = S.system.solvers.highs;
    const ready = S.system.executor.configured && highs;
    const backend = $("#backend-status");
    backend.textContent = ready ? `Backend ready - HiGHS ${highs}` : (highs ? "No executor configured" : "Solver unavailable (HiGHS missing)");
    backend.className = `badge badge-${ready ? "ok" : "err"}`;
  } catch (error) {
    const backend = $("#backend-status");
    backend.textContent = "Backend unreachable"; backend.className = "badge badge-err";
  }
  updateStatus();
}
async function refreshProjects() {
  S.projects = await api("/projects");
  S.samples = await api("/samples");
}
async function refreshRuns() {
  const path = S.project && S.savedJson ? `/projects/${encodeURIComponent(S.project.id)}/runs` : "/runs";
  try { S.runs = await api(path); } catch (error) { S.runs = []; }
  const active = S.runs.some((run) => !TERMINAL.has(run.status));
  if (active && !S.poller) S.poller = setInterval(pollRuns, 1000);
  if (!active && S.poller) { clearInterval(S.poller); S.poller = null; }
}
async function pollRuns() {
  const before = new Map(S.runs.map((run) => [run.id, run.status]));
  await refreshRuns();
  const changed = S.runs.some((run) => before.get(run.id) !== run.status);
  if (!changed) return;
  for (const run of S.runs) if (TERMINAL.has(run.status) && before.get(run.id) !== run.status) S.summaries.delete(run.id);
  await refreshSystem();
  if (["runs", "results", "compare"].includes(S.view)) render();
}

// ---------------------------------------------------------------- project lifecycle
async function openProject(projectId) {
  const data = await api(`/projects/${encodeURIComponent(projectId)}`);
  S.project = data.project;
  S.savedJson = JSON.stringify(S.project);
  S.validation = null; S.editingAsset = null; S.documentOpen = false;
  S.selectedRunId = null; S.resultsRunId = null; S.compare = null; S.lastRunError = null;
  await refreshRuns();
  await validateProject({ quiet: true });
}
async function validateProject({ quiet = false } = {}) {
  if (!S.project) return null;
  const forJson = workingJson();
  const result = await api("/projects/validate", { method: "POST", body: S.project });
  S.validation = { forJson, result };
  if (!quiet) toast(result.contract_valid ? (result.run_ready ? "Validation passed: the project is run-ready." : `Validation found ${result.errors} blocking issue(s) for runs.`) : `Validation failed with ${result.errors} error(s).`, result.run_ready ? "ok" : "warn");
  updateStatus();
  return result;
}
async function saveProject() {
  if (!S.project) return;
  const forJson = workingJson();
  try {
    const saved = await api("/projects/save", { method: "POST", body: S.project });
    S.savedJson = forJson;
    S.validation = { forJson, result: { contract_valid: true, run_ready: saved.run_ready, issues: saved.issues,
      errors: saved.issues.filter((i) => i.severity === "error").length,
      warnings: saved.issues.filter((i) => i.severity === "warning").length } };
    toast(`Project saved (revision ${saved.revision})${saved.run_ready ? "" : " - not run-ready, see Validation"}.`, saved.run_ready ? "ok" : "warn");
    await refreshProjects(); await refreshSystem(); await refreshRuns();
  } catch (error) {
    if (error.issues && error.issues.length) {
      S.validation = { forJson, result: { contract_valid: false, run_ready: false, issues: error.issues,
        errors: error.issues.length, warnings: 0 } };
    }
    toast(`Save rejected: ${error.message}`, "err");
    S.view = "validation";
  }
  render();
}
function markChanged() { updateStatus(); }

// ---------------------------------------------------------------- views
const VIEWS = {
  overview: renderOverview, project: renderProject, assets: renderAssets, scenarios: renderScenarios,
  validation: renderValidation, runs: renderRuns, results: renderResults, compare: renderCompare,
  reference: renderReference, about: renderAbout,
};
function show(view) {
  S.view = view;
  if (location.hash !== `#${view}`) history.replaceState(null, "", `#${view}`);
  render();
  $("#main").focus();
}
function render() {
  for (const [name] of Object.entries(VIEWS)) {
    const section = $(`#view-${name}`);
    section.hidden = name !== S.view;
  }
  for (const button of document.querySelectorAll(".sidenav button")) {
    if (button.dataset.view === S.view) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  }
  const section = clear($(`#view-${S.view}`));
  try { VIEWS[S.view](section); }
  catch (error) { section.append(errorPanel(error, "This view could not be rendered")); console.warn(error); }
  updateStatus();
}

// ----- overview
function renderOverview(section) {
  section.append(h("h2", { id: "h-overview", text: "Overview" }),
    h("p", { class: "muted", text: "Load the sanitized Georgia demonstration or create a project from a typed project document. Validate and save it, launch a run, then review QA-gated results." }));
  const grid = h("div", { class: "grid-2" });
  // open project
  const openCard = h("div", { class: "card" }, h("h3", { text: "Open a saved project" }));
  if (!S.projects.length) openCard.append(h("p", { class: "empty", text: "This workspace has no saved projects yet." }));
  else {
    const select = h("select", { id: "project-select", "aria-label": "Saved projects" },
      S.projects.map((p) => h("option", { value: p.id, selected: S.project && S.project.id === p.id, text: `${p.name} (${p.workflow})` })));
    openCard.append(h("div", { class: "actions" }, select,
      h("button", { type: "button", id: "open-project", class: "primary", text: "Open",
        onclick: async () => {
          if (isDirty() && !confirm("Discard unsaved changes to the current project?")) return;
          try { await openProject(select.value); show("project"); }
          catch (error) { openCard.append(errorPanel(error, "The project could not be opened")); }
        } })));
  }
  grid.append(openCard);
  // samples
  const sampleCard = h("div", { class: "card" }, h("h3", { text: "Sample projects" }));
  for (const sample of S.samples) {
    const sampleProjectId = sample.project_id;
    const exists = S.projects.some((p) => p.id === sampleProjectId);
    const message = h("div", {});
    sampleCard.append(h("div", { class: "stack" },
      h("p", {}, h("strong", { text: sample.name })), h("p", { class: "help", text: sample.description }),
      h("div", { class: "actions" },
        h("button", { type: "button", id: `load-sample-${sample.id}`, disabled: !sample.available,
          title: sample.available ? null : "The sample file is not installed",
          text: exists ? "Reload sample (overwrite)" : "Load sample",
          onclick: async () => {
            if (exists && !confirm("Overwrite the saved copy of this sample in the workspace?")) return;
            if (isDirty() && !confirm("Discard unsaved changes to the current project?")) return;
            try {
              const saved = await api(`/samples/${sample.id}/load${exists ? "?overwrite=true" : ""}`, { method: "POST" });
              await refreshProjects(); await refreshSystem(); await openProject(saved.project_id);
              toast(`Loaded ${sample.name}.`, "ok"); show("project");
            } catch (error) { clear(message).append(errorPanel(error, "The sample could not be loaded")); }
          } }),
        exists ? h("button", { type: "button", text: "Open saved copy", onclick: async () => {
          if (isDirty() && !confirm("Discard unsaved changes to the current project?")) return;
          await openProject(sampleProjectId); show("project"); } }) : null),
      message, h("hr", { class: "muted" })));
  }
  grid.append(sampleCard);
  section.append(grid);
  // new project from document
  const diagnostics = h("div", { id: "new-project-diagnostics", "aria-live": "polite" });
  const textarea = h("textarea", { id: "new-project-json", rows: 12, spellcheck: "false",
    "aria-describedby": "new-project-help", placeholder: "Paste a PowerSim project JSON document" });
  const parseDocument = () => {
    try {
      const parsed = JSON.parse(textarea.value);
      textarea.removeAttribute("aria-invalid");
      return parsed;
    } catch (error) {
      textarea.setAttribute("aria-invalid", "true");
      clear(diagnostics).append(h("p", { class: "field-error", role: "alert", text: `The document is not valid JSON: ${error.message}` }));
      return undefined;
    }
  };
  const fileInput = h("input", { type: "file", id: "new-project-file", accept: ".json,application/json",
    onchange: async () => { const file = fileInput.files[0]; if (file) textarea.value = await file.text(); } });
  section.append(h("div", { class: "card" },
    h("h3", { text: "Create a project from a typed project document" }),
    h("p", { id: "new-project-help", class: "help", text: "The document must follow the PowerSim project contract (schemas/v1). It is validated on the server before it is saved." }),
    h("label", { for: "new-project-file" }, "Project JSON file", fileInput),
    h("label", { for: "new-project-json" }, "Project document", textarea),
    h("div", { class: "actions" },
      h("button", { type: "button", id: "insert-template", text: "Insert minimal template",
        onclick: () => { textarea.value = JSON.stringify(templateProject(), null, 2); } }),
      h("button", { type: "button", id: "validate-new-project", text: "Validate document",
        onclick: async () => {
          const parsed = parseDocument(); if (parsed === undefined) return;
          const result = await api("/projects/validate", { method: "POST", body: parsed });
          clear(diagnostics).append(h("p", {}, result.contract_valid ? badge(result.run_ready ? "Valid - run-ready" : "Valid - not run-ready", result.run_ready ? "ok" : "warn") : badge("Validation failed", "err")), issuesTable(result.issues));
        } }),
      h("button", { type: "button", id: "create-project", class: "primary", text: "Create project",
        onclick: async () => {
          const parsed = parseDocument(); if (parsed === undefined) return;
          if (S.projects.some((p) => p.id === parsed.id) && !confirm(`A project with id '${parsed.id}' exists. Overwrite it?`)) return;
          try {
            const saved = await api("/projects/save", { method: "POST", body: parsed });
            await refreshProjects(); await refreshSystem(); await openProject(saved.project_id);
            toast(`Project ${saved.project_id} created.`, "ok"); show("project");
          } catch (error) { clear(diagnostics).append(errorPanel(error, "The project was not created")); }
        } })),
    diagnostics));
  // workflows
  if (S.system) {
    section.append(h("div", { class: "card" }, h("h3", { text: "Supported workflows" }),
      h("ul", {}, S.system.workflows.map((w) => h("li", {}, h("strong", { text: w.label }), ` - ${w.description}`))),
      h("p", { class: "warn-panel" }, h("strong", { text: "Not supported: " }), S.system.unsupported.join(", "), ".")));
  }
}
function templateProject() {
  return {
    id: "my-study", version: { revision: 1 }, workflow: "deterministic_uc",
    metadata: { name: "My study" }, units: { currency: "USD" },
    time: { timezone: "UTC", study_year: 2026, resolution_minutes: 60, interval_duration_hours: 1,
            start: "2026-01-01T00:00:00Z", periods: 4, calendar_policy: "explicit_periods" },
    assets: [
      { id: "thermal", kind: "thermal", capacity_min_mw: 0, capacity_max_mw: 30,
        legacy_extensions: { committable: false, heat_rate: 0, fuel_price: 0, vom: 10, ramp_up: 30, ramp_down: 30 } },
      { id: "import", kind: "import", capacity_max_mw: 30, legacy_extensions: { vom: 100 } },
    ],
    profiles: [{ id: "demand", unit: "MW", values: [20, 22, 25, 21] }],
    legacy_payload: { resolution_min: 60, study_horizon: { start_hour: 0, horizon_hours: 4 } },
  };
}

// ----- project
function renderProject(section) {
  if (!S.project) return needProject(section, "Project");
  const p = S.project;
  section.append(h("h2", { id: "h-project", text: `Project: ${p.metadata?.name || p.id}` }));
  const nameInput = h("input", { id: "project-name", value: p.metadata?.name || "",
    oninput: () => { p.metadata = { ...(p.metadata || {}), name: nameInput.value }; markChanged(); } });
  const workflowSelect = h("select", { id: "project-workflow",
    onchange: () => { p.workflow = workflowSelect.value; workflowHelp.textContent = describeWorkflow(p.workflow); markChanged(); } },
    (S.system?.workflows || [{ id: p.workflow, label: p.workflow }]).map((w) => h("option", { value: w.id, selected: w.id === p.workflow, text: w.label })));
  const workflowHelp = h("span", { class: "help", text: describeWorkflow(p.workflow) });
  const currency = h("input", { id: "project-currency", value: p.units?.currency || "",
    oninput: () => { p.units = { ...p.units, currency: currency.value }; markChanged(); } });
  const settings = p.solver_settings || {};
  const solverField = (id, key, label, help, check) => {
    const error = h("span", { class: "field-error", id: `${id}-error` });
    const input = h("input", { id, type: "number", step: "any", value: settings[key] ?? "", "aria-describedby": `${id}-help ${id}-error`,
      oninput: () => {
        const raw = input.value.trim();
        const value = raw === "" ? null : Number(raw);
        const problem = value === null ? null : check(value);
        error.textContent = problem || "";
        if (problem) { input.setAttribute("aria-invalid", "true"); return; }
        input.removeAttribute("aria-invalid");
        const next = { ...(p.solver_settings || {}) };
        if (value === null) delete next[key]; else next[key] = value;
        p.solver_settings = next; markChanged();
      } });
    return h("label", { for: id }, label, input, h("span", { class: "help", id: `${id}-help`, text: help }), error);
  };
  section.append(h("div", { class: "card" }, h("h3", { text: "Study definition" }),
    h("div", { class: "form-grid" },
      h("label", { for: "project-id" }, "Project id", h("input", { id: "project-id", value: p.id, readonly: true }),
        h("span", { class: "help", text: "The id is the project identity; change it in the project document." })),
      h("label", { for: "project-name" }, "Name", nameInput),
      h("label", { for: "project-workflow" }, "Workflow", workflowSelect, workflowHelp),
      h("label", { for: "project-currency" }, "Currency", currency))));
  section.append(h("div", { class: "card" }, h("h3", { text: "Time axis" }),
    h("dl", { class: "kv" }, [
      ["Timezone", p.time.timezone], ["Study year", p.time.study_year], ["Start", p.time.start],
      ["Periods", num(p.time.periods, 0)], ["Resolution", `${p.time.resolution_minutes} minutes`], ["Calendar policy", p.time.calendar_policy],
    ].map(([k, v]) => [h("dt", { text: k }), h("dd", { text: String(v) })])),
    h("p", { class: "help", text: "The time axis must stay consistent with every profile; edit it in the project document." })));
  section.append(h("div", { class: "card" }, h("h3", { text: "Solver settings" }),
    h("p", { class: "help", text: "These typed settings are passed to the solver. Blank fields use the solver defaults. Compatibility settings inside legacy_payload are not used by the application executor." }),
    h("div", { class: "form-grid" },
      solverField("solver-time-limit", "time_limit_s", "Time limit per solve (s)", "Positive number of seconds.", (v) => (v > 0 && Number.isFinite(v) ? null : "Enter a positive number.")),
      solverField("solver-mip-gap", "mip_gap", "Relative MIP gap", "Between 0 and 1, for example 0.005.", (v) => (v >= 0 && v < 1 ? null : "Enter a value from 0 up to 1.")))));
  const meta = { ...(p.metadata || {}) };
  const imports = meta.reference_imports || [];
  delete meta.reference_imports; delete meta.name;
  const metaCard = h("div", { class: "card" }, h("h3", { text: "Metadata and provenance" }));
  const rows = Object.entries(meta);
  if (!rows.length) metaCard.append(h("p", { class: "muted", text: "No additional metadata." }));
  else metaCard.append(h("dl", { class: "kv" }, rows.map(([k, v]) => [h("dt", { text: k }),
    h("dd", {}, Array.isArray(v) ? h("ul", {}, v.map((x) => h("li", { text: typeof x === "string" ? x : JSON.stringify(x) })))
      : (typeof v === "object" ? h("code", { text: JSON.stringify(v) }) : String(v)))])));
  if (imports.length) {
    metaCard.append(h("h4", { text: "Reference-data imports" }), h("div", { class: "table-wrap" }, h("table", {},
      h("thead", {}, h("tr", {}, ["Imported at", "Records", "Assets", "Workbook SHA-256", "Overrides", "Imported by"].map((t) => h("th", { text: t })))),
      h("tbody", {}, imports.map((i) => h("tr", {}, h("td", { text: i.imported_at }), h("td", { text: (i.records || []).join(", ") }),
        h("td", { text: (i.asset_ids || []).join(", ") }), h("td", {}, h("code", { text: shortHash(i.source_sha256) })),
        h("td", { text: String(Object.keys(i.overrides || {}).length) }), h("td", { text: i.imported_by || "-" })))))));
  }
  section.append(metaCard);
  // advanced document
  const docArea = h("textarea", { id: "project-document", rows: 18, spellcheck: "false", "aria-describedby": "document-help" });
  const docError = h("div", { id: "document-error", "aria-live": "polite" });
  const details = h("details", { class: "card", open: S.documentOpen,
    ontoggle: () => { S.documentOpen = details.open; if (details.open) docArea.value = JSON.stringify(S.project, null, 2); } },
    h("summary", { text: "Project document (advanced JSON editor)" }),
    h("p", { id: "document-help", class: "help", text: "Edit any part of the typed project. Applying replaces the working copy; nothing is saved until you choose Save project." }),
    docArea,
    h("div", { class: "actions" }, h("button", { type: "button", id: "apply-document", text: "Apply document to working copy",
      onclick: () => {
        try {
          const parsed = JSON.parse(docArea.value);
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("the document must be a JSON object");
          S.project = parsed; docArea.removeAttribute("aria-invalid"); clear(docError);
          toast("Document applied to the working copy. Validate and save to keep it.", "info"); render();
        } catch (error) {
          docArea.setAttribute("aria-invalid", "true");
          clear(docError).append(h("p", { class: "field-error", role: "alert", text: `Not applied: ${error.message}` }));
        }
      } })), docError);
  if (S.documentOpen) docArea.value = JSON.stringify(S.project, null, 2);
  section.append(details);
}
function describeWorkflow(id) { return S.system?.workflows.find((w) => w.id === id)?.description || ""; }

// ----- assets
function assetSource(asset) {
  const prov = asset.provenance;
  if (!prov) return h("span", { class: "muted", text: "Project" });
  if (prov.source === REFERENCE_SOURCE) {
    const reviewed = prov.details?.review_status === "user_reviewed";
    return h("span", {}, "Reference catalogue ", badge(reviewed ? "reviewed" : "requires review", reviewed ? "ok" : "warn"));
  }
  return h("span", { text: prov.source });
}
function renderAssets(section) {
  if (!S.project) return needProject(section, "Assets");
  const assets = S.project.assets || [];
  section.append(h("h2", { id: "h-assets", text: "Assets" }),
    h("p", { class: "muted", text: "Disabled assets stay in the project but are excluded from runs. Reference-catalogue assets are imported disabled and must be reviewed before they can be enabled in a run." }),
    h("div", { class: "actions" }, h("button", { type: "button", id: "add-asset", text: "Add asset", onclick: () => {
      let n = assets.length + 1; while (assets.some((a) => a.id === `asset-${n}`)) n += 1;
      S.editingAsset = { index: -1, draft: { id: `asset-${n}`, kind: "thermal", enabled: true, capacity_min_mw: 0, capacity_max_mw: 100, legacy_extensions: { vom: 50 } } };
      render(); $("#asset-id")?.focus();
    } })));
  if (!assets.length) section.append(h("p", { class: "empty", text: "This project has no assets." }));
  else section.append(h("div", { class: "table-wrap" }, h("table", { id: "assets-table" },
    h("thead", {}, h("tr", {}, ["Enabled", "Id", "Name", "Kind", "Min MW", "Max MW", "Bus", "Profiles", "Source", ""].map((t) => h("th", { scope: "col", text: t })))),
    h("tbody", {}, assets.map((asset, index) => h("tr", { class: S.editingAsset?.index === index ? "selected" : null },
      h("td", {}, asset.enabled === false ? badge("disabled", "neutral") : badge("enabled", "ok")),
      h("td", {}, h("code", { text: asset.id })), h("td", { text: asset.legacy_extensions?.name || "" }),
      h("td", { text: asset.kind }), h("td", { class: "num", text: num(asset.capacity_min_mw) }),
      h("td", { class: "num", text: num(asset.capacity_max_mw) }), h("td", { text: asset.bus || "-" }),
      h("td", { text: (asset.profile_references || []).join(", ") || "-" }), h("td", {}, assetSource(asset)),
      h("td", {}, h("button", { type: "button", class: "link", id: `edit-asset-${index}`, "aria-label": `Edit asset ${asset.id}`, text: "Edit",
        onclick: () => { S.editingAsset = { index, draft: JSON.parse(JSON.stringify(asset)) }; render(); $("#asset-id")?.focus(); } }))))))));
  if (S.editingAsset) section.append(assetEditor());
  section.append(workbookCard());
}
async function uploadWorkbook(file, apply) {
  const params = new URLSearchParams({ apply: String(apply), file_name: file.name });
  const reviewer = $("#workbook-reviewer")?.value.trim();
  if (reviewer) params.set("imported_by", reviewer);
  let response;
  try {
    response = await fetch(`/api/projects/${encodeURIComponent(S.project.id)}/parameter-workbook?${params}`, {
      method: "POST", cache: "no-store", headers: { "Content-Type": "application/octet-stream" }, body: await file.arrayBuffer() });
  } catch (error) { throw new ApiFailure(0, { code: "backend_unreachable", message: "The PowerSim backend is not reachable." }); }
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = data && typeof data.detail === "object" && !Array.isArray(data.detail) ? data.detail : { code: `http_${response.status}`, message: response.statusText };
    throw new ApiFailure(response.status, detail);
  }
  return data;
}
function workbookCard() {
  const card = h("div", { class: "card", id: "workbook-card" }, h("h3", { text: "Bulk edit parameters with Excel" }),
    h("p", { class: "help", text: "Download a workbook with every asset and profile, fill in approved values (fuel, heat rate, fuel price, VOM, capacities, availability/inflow profiles, review status), and upload it. Empty cells leave values unchanged. You see every change before it is applied." }));
  const blocker = !S.savedJson ? "Save the project first." : (isDirty() ? "Save your changes before using the workbook: it is generated from the saved project." : null);
  const out = h("div", { id: "workbook-result", "aria-live": "polite" });
  const fileInput = h("input", { type: "file", id: "workbook-file", accept: ".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", disabled: Boolean(blocker) });
  const reviewer = h("input", { id: "workbook-reviewer", placeholder: "Your name or initials (optional)" });
  card.append(
    h("div", { class: "actions" },
      h("button", { type: "button", id: "download-workbook", disabled: Boolean(blocker), text: "Download Excel template",
        onclick: () => download(`/projects/${encodeURIComponent(S.project.id)}/parameter-workbook`) }),
      blocker ? h("span", { class: "field-error", text: blocker }) : null),
    h("div", { class: "form-grid" }, h("label", { for: "workbook-file" }, "Filled workbook (.xlsx)", fileInput),
      h("label", { for: "workbook-reviewer" }, "Imported by", reviewer)),
    h("div", { class: "actions" }, h("button", { type: "button", id: "check-workbook", disabled: Boolean(blocker), text: "Check workbook",
      onclick: async () => {
        const file = fileInput.files[0];
        if (!file) { clear(out).append(h("p", { class: "field-error", role: "alert", text: "Choose the filled .xlsx file first." })); return; }
        clear(out).append(h("p", { class: "muted", text: "Checking..." }));
        try { renderWorkbookReport(clear(out), await uploadWorkbook(file, false), file); }
        catch (error) { clear(out).append(errorPanel(error, "The workbook could not be checked")); }
      } })),
    out);
  return card;
}
function renderWorkbookReport(out, report, file) {
  const ok = report.errors === 0;
  out.append(h("div", { class: ok ? (report.applicable ? "info-panel" : "warn-panel") : "error-panel", role: "status" },
    h("strong", { text: ok ? (report.applicable ? `${report.changes.length} change(s) and ${report.profiles.length} profile update(s) ready to apply.` : "The workbook contains no changes.") : `${report.errors} error(s) must be fixed in the workbook.` }),
    h("p", { text: `${report.warnings} warning(s). File ${shortHash(report.file_sha256)}.` })));
  if (report.issues.length) {
    out.append(h("div", { class: "table-wrap" }, h("table", { id: "workbook-issues" },
      h("thead", {}, h("tr", {}, ["Severity", "Cell", "Code", "Message"].map((t) => h("th", { text: t })))),
      h("tbody", {}, report.issues.map((i) => h("tr", {}, h("td", {}, badge(i.severity, severityKind(i.severity))),
        h("td", {}, h("code", { text: i.path || "-" })), h("td", {}, h("code", { text: i.code })), h("td", { text: i.message })))))));
  }
  if (report.changes.length) {
    out.append(h("h4", { text: "Changes" }), h("div", { class: "table-wrap tall" }, h("table", { id: "workbook-changes" },
      h("thead", {}, h("tr", {}, ["Asset", "Field", "Current", "New", "Row"].map((t) => h("th", { text: t })))),
      h("tbody", {}, report.changes.map((c) => h("tr", {}, h("td", {}, h("code", { text: c.asset_id })), h("td", { text: c.field }),
        h("td", { text: c.old ?? "-" }), h("td", { text: c.new ?? "-" }), h("td", { class: "num", text: c.row })))))));
  }
  if (report.profiles.length) {
    out.append(h("h4", { text: "Profiles" }), h("ul", {}, report.profiles.map((p) => h("li", { text: `${p.id}: ${p.action} (${p.values} values)` }))));
  }
  if (report.applicable) {
    out.append(h("div", { class: "actions" }, h("button", { type: "button", class: "primary", id: "apply-workbook", text: "Apply changes to project",
      onclick: async () => {
        try {
          const result = await uploadWorkbook(file, true);
          await openProject(result.project_id); await refreshProjects(); await refreshSystem();
          toast(`Applied ${result.changes.length} change(s); project revision ${result.revision}${result.run_ready ? "" : " - not run-ready, see Validation"}.`, result.run_ready ? "ok" : "warn");
          render();
        } catch (error) { out.append(errorPanel(error, "The workbook was not applied")); }
      } }), h("span", { class: "help", text: "Saves a new project revision and records the file hash and every change." })));
  }
}
function assetEditor() {
  const { index, draft } = S.editingAsset;
  const ext = draft.legacy_extensions || {};
  const errors = h("div", { id: "asset-errors", "aria-live": "assertive" });
  const input = (id, label, value, attrs = {}) => h("label", { for: id }, label, h("input", { id, value: value ?? "", ...attrs }));
  const profiles = (S.project.profiles || []).map((pr) => pr.id);
  const isReference = draft.provenance?.source === REFERENCE_SOURCE;
  const card = h("div", { class: "card", id: "asset-editor" },
    h("h3", { text: index === -1 ? "New asset" : `Edit asset ${draft.id}` }),
    h("div", { class: "form-grid" },
      input("asset-id", "Asset id", draft.id, { required: true }),
      h("label", { for: "asset-kind" }, "Kind", h("select", { id: "asset-kind" },
        Array.from(new Set([...ASSET_KINDS, draft.kind])).map((k) => h("option", { value: k, selected: k === draft.kind, text: k })))),
      input("asset-name", "Name", ext.name),
      input("asset-min", "Min capacity (MW)", draft.capacity_min_mw, { type: "number", step: "any", min: 0 }),
      input("asset-max", "Max capacity (MW)", draft.capacity_max_mw, { type: "number", step: "any", min: 0 }),
      input("asset-bus", "Bus", draft.bus),
      h("label", { for: "asset-profiles" }, "Profile references", h("select", { id: "asset-profiles", multiple: true, size: Math.min(4, Math.max(2, profiles.length)) },
        profiles.map((id) => h("option", { value: id, selected: (draft.profile_references || []).includes(id), text: id }))),
        h("span", { class: "help", text: "The first profile is used as availability profile." })),
      input("asset-vom", "Variable O&M (currency/MWh)", ext.vom, { type: "number", step: "any", min: 0 }),
      input("asset-heat-rate", "Heat rate (GJ/MWh)", ext.heat_rate, { type: "number", step: "any", min: 0 }),
      input("asset-fuel-price", "Fuel price (USD/MMBtu)", ext.fuel_price, { type: "number", step: "any", min: 0 }),
      input("asset-fuel-type", "Fuel type", ext.fuel_type),
      h("label", { class: "inline", for: "asset-enabled" }, h("input", { id: "asset-enabled", type: "checkbox", checked: draft.enabled !== false }), "Enabled in runs")),
    errors);
  if (isReference) card.append(referenceProvenancePanel(draft));
  card.append(h("div", { class: "actions" },
    h("button", { type: "button", class: "primary", id: "apply-asset", text: index === -1 ? "Add to project" : "Apply changes", onclick: () => applyAsset(errors) }),
    h("button", { type: "button", text: "Cancel", onclick: () => { S.editingAsset = null; render(); } }),
    index >= 0 ? h("button", { type: "button", class: "danger", id: "remove-asset", text: "Remove asset",
      onclick: () => { if (!confirm(`Remove asset ${draft.id}?`)) return; S.project.assets.splice(index, 1); S.editingAsset = null; render(); } }) : null));
  return card;
}
function referenceProvenancePanel(draft) {
  const details = draft.provenance.details || {};
  const reviewed = details.review_status === "user_reviewed";
  const note = h("textarea", { id: "review-note", rows: 3, placeholder: "What did you check, against which approved source?" });
  const noteError = h("span", { class: "field-error", id: "review-note-error" });
  const metadata = details.reference_metadata || {};
  return h("div", { class: "stack" },
    h("div", { class: reviewed ? "ok-panel" : "warn-panel" },
      h("strong", { text: reviewed ? "Reviewed reference values" : "Reference values require user review" }),
      h("p", { text: reviewed ? `Reviewed ${details.reviewed_at || ""}: ${details.review_note || ""}` : "These values were transcribed from the reference workbook. They are not approved study data. Check them against an approved source before enabling this asset." })),
    h("dl", { class: "kv" },
      [["Workbook", details.source_workbook], ["Sheet / row", `${details.source_sheet} / row ${details.source_row}`],
       ["Record", details.source_record_id], ["Workbook SHA-256", shortHash(details.source_sha256)],
       ["Imported", `${details.imported_at || "-"}${details.imported_by ? ` by ${details.imported_by}` : ""}`],
       ["Importer", details.importer_version]].map(([k, v]) => [h("dt", { text: k }), h("dd", { text: String(v ?? "-") })])),
    h("h4", { text: "Field origins" }),
    h("div", { class: "table-wrap" }, h("table", {},
      h("thead", {}, h("tr", {}, ["Field", "Status", "Origin", "Reason / note"].map((t) => h("th", { text: t })))),
      h("tbody", {}, Object.entries(details.field_origins || {}).map(([field, o]) => h("tr", {},
        h("td", {}, h("code", { text: field })), h("td", { text: o.status }), h("td", { text: o.origin }),
        h("td", { text: o.reason || o.note || "" })))))),
    h("details", {}, h("summary", { text: "Attached reference metadata (not used by the solver)" }),
      h("dl", { class: "kv" }, Object.entries(metadata).map(([k, v]) => [h("dt", { text: k }),
        h("dd", {}, typeof v === "object" ? h("code", { text: JSON.stringify(v) }) : String(v))]))),
    reviewed
      ? h("button", { type: "button", id: "unmark-reviewed", text: "Mark as requiring review again", onclick: () => {
          draft.provenance = { ...draft.provenance, details: { ...details, review_status: "requires_user_review" } }; render(); } })
      : h("div", {}, h("label", { for: "review-note" }, "Review note (required)", note, noteError),
          h("button", { type: "button", id: "mark-reviewed", text: "Mark parameters reviewed", onclick: () => {
            if (!note.value.trim()) { noteError.textContent = "Describe the review before marking it complete."; note.setAttribute("aria-invalid", "true"); note.focus(); return; }
            draft.provenance = { ...draft.provenance, details: { ...details, review_status: "user_reviewed",
              reviewed_at: new Date().toISOString(), review_note: note.value.trim() } };
            toast("Marked reviewed in the editor. Apply changes and save to keep it.", "info"); render();
          } })));
}
function applyAsset(errors) {
  const { index, draft } = S.editingAsset;
  const problems = [];
  const id = $("#asset-id").value.trim();
  if (!ID_PATTERN.test(id)) problems.push("Asset id must start with a letter or digit and use letters, digits, '.', '_' or '-'.");
  if (S.project.assets.some((a, i) => a.id === id && i !== index)) problems.push(`Asset id '${id}' is already used.`);
  const numeric = (selector, label) => {
    const raw = $(selector).value.trim();
    if (raw === "") return null;
    const value = Number(raw);
    if (!Number.isFinite(value) || value < 0) { problems.push(`${label} must be a non-negative number.`); return null; }
    return value;
  };
  const min = numeric("#asset-min", "Min capacity"), max = numeric("#asset-max", "Max capacity");
  if (min !== null && max !== null && min > max) problems.push("Min capacity cannot exceed max capacity.");
  const vom = numeric("#asset-vom", "Variable O&M"), heatRate = numeric("#asset-heat-rate", "Heat rate"), fuelPrice = numeric("#asset-fuel-price", "Fuel price");
  if (problems.length) {
    clear(errors).append(h("div", { class: "error-panel", role: "alert" }, h("strong", { text: "The asset was not applied" }), h("ul", {}, problems.map((p) => h("li", { text: p })))));
    return;
  }
  const kind = $("#asset-kind").value;
  const ext = { ...(draft.legacy_extensions || {}) };
  const setExt = (key, value) => { if (value === null || value === "") delete ext[key]; else ext[key] = value; };
  setExt("name", $("#asset-name").value.trim()); setExt("vom", vom); setExt("heat_rate", heatRate);
  setExt("fuel_price", fuelPrice); setExt("fuel_type", $("#asset-fuel-type").value.trim());
  // keep legacy compatibility keys consistent with the typed fields
  if ("id" in ext) ext.id = id;
  if ("type" in ext) ext.type = kind;
  if ("pmin" in ext) ext.pmin = min ?? 0;
  if ("pmax" in ext) ext.pmax = max;
  const bus = $("#asset-bus").value.trim();
  const asset = { ...draft, id, kind, enabled: $("#asset-enabled").checked, capacity_min_mw: min, capacity_max_mw: max,
    bus: bus || null, profile_references: Array.from($("#asset-profiles").selectedOptions).map((o) => o.value), legacy_extensions: ext };
  if (index === -1) S.project.assets = [...(S.project.assets || []), asset];
  else S.project.assets[index] = asset;
  S.editingAsset = null;
  toast(`Asset ${id} updated in the working copy. Validate and save to keep it.`, "info");
  render();
}

// ----- scenarios
function overlaySummary(overlay) {
  const parts = [];
  for (const [key, value] of Object.entries(overlay || {})) {
    if (key === "profiles" && Array.isArray(value)) parts.push(`profiles: ${value.map((p) => `${p.id} (${(p.values || []).length} values)`).join(", ")}`);
    else if (value && typeof value === "object") parts.push(`${key}: ${Object.keys(value).join(", ")}`);
    else parts.push(`${key}: ${JSON.stringify(value)}`);
  }
  return parts.length ? parts.join("; ") : "No overrides (identical to base case)";
}
function renderScenarios(section) {
  if (!S.project) return needProject(section, "Scenarios");
  const scenarios = S.project.scenarios || [];
  section.append(h("h2", { id: "h-scenarios", text: "Scenarios" }),
    h("p", { class: "muted", text: "A scenario is a deterministic overlay on the base project. Overlays cannot change project identity or version." }));
  if (!scenarios.length) section.append(h("p", { class: "empty", text: "No scenarios. Runs use the base case." }));
  else section.append(h("div", { class: "table-wrap" }, h("table", { id: "scenarios-table" },
    h("thead", {}, h("tr", {}, ["Id", "Name", "Overlay", ""].map((t) => h("th", { scope: "col", text: t })))),
    h("tbody", {}, scenarios.map((scenario, index) => {
      const name = h("input", { value: scenario.name || "", "aria-label": `Name of scenario ${scenario.id}`,
        oninput: () => { scenario.name = name.value || null; markChanged(); } });
      return h("tr", {}, h("td", {}, h("code", { text: scenario.id })), h("td", {}, name),
        h("td", { class: "help", text: overlaySummary(scenario.overlay) }),
        h("td", {}, h("button", { type: "button", class: "link", text: "Delete", "aria-label": `Delete scenario ${scenario.id}`,
          onclick: () => { if (!confirm(`Delete scenario ${scenario.id}?`)) return; scenarios.splice(index, 1); render(); } })));
    })))));
  const idInput = h("input", { id: "scenario-id", placeholder: "e.g. high-demand" });
  const nameInput = h("input", { id: "scenario-name" });
  const overlay = h("textarea", { id: "scenario-overlay", rows: 6, spellcheck: "false", "aria-describedby": "scenario-overlay-help" });
  overlay.value = "{}";
  const errors = h("div", { "aria-live": "assertive" });
  section.append(h("div", { class: "card" }, h("h3", { text: "Add a scenario" }),
    h("div", { class: "form-grid" }, h("label", { for: "scenario-id" }, "Scenario id", idInput), h("label", { for: "scenario-name" }, "Name", nameInput)),
    h("label", { for: "scenario-overlay" }, "Overlay (JSON object)", overlay,
      h("span", { id: "scenario-overlay-help", class: "help", text: "Mappings merge into the base project; lists and values replace it. Example: {\"solver_settings\": {\"mip_gap\": 0.01}}." })),
    h("div", { class: "actions" }, h("button", { type: "button", id: "add-scenario", text: "Add scenario", onclick: () => {
      const problems = [];
      const id = idInput.value.trim();
      if (!ID_PATTERN.test(id)) problems.push("Scenario id must start with a letter or digit and use letters, digits, '.', '_' or '-'.");
      if (scenarios.some((s) => s.id === id)) problems.push(`Scenario '${id}' already exists.`);
      let parsed = null;
      try { parsed = JSON.parse(overlay.value || "{}"); if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("the overlay must be a JSON object"); }
      catch (error) { problems.push(`Overlay: ${error.message}`); }
      if (problems.length) { clear(errors).append(h("div", { class: "error-panel", role: "alert" }, h("ul", {}, problems.map((p) => h("li", { text: p }))))); return; }
      S.project.scenarios = [...scenarios, { id, name: nameInput.value.trim() || null, overlay: parsed }];
      toast(`Scenario ${id} added to the working copy.`, "info"); render();
    } })), errors));
}

// ----- validation
function renderValidation(section) {
  if (!S.project) return needProject(section, "Validation");
  section.append(h("h2", { id: "h-validation", text: "Validation" }),
    h("p", { class: "muted", text: "Contract errors block saving. Run-readiness errors block run creation. Warnings are reported for review." }),
    h("div", { class: "actions" }, h("button", { type: "button", id: "run-validation", class: "primary", text: "Validate working copy",
      onclick: async () => { await validateProject(); render(); } })));
  const validation = currentValidation();
  if (!validation) { section.append(h("p", { class: "empty", text: "The current working copy has not been validated yet." })); return; }
  section.append(h("div", { id: "validation-summary", class: validation.contract_valid ? (validation.run_ready ? "ok-panel" : "warn-panel") : "error-panel", role: "status" },
    h("strong", { text: validation.contract_valid ? (validation.run_ready ? "Valid and run-ready" : "Valid project, but not run-ready") : "Validation failed - the project cannot be saved" }),
    h("p", { text: `${validation.errors} error(s), ${validation.warnings} warning(s), ${validation.issues.filter((i) => i.severity === "info").length} note(s).` })),
    issuesTable(validation.issues, { id: "validation-issues" }));
}

// ----- runs
function createRunBlocker() {
  if (!S.project) return "Open a project first.";
  if (!S.savedJson) return "Save the project before creating a run.";
  if (isDirty()) return "Save your changes first: a run uses the saved project snapshot.";
  const validation = currentValidation();
  if (validation && !validation.run_ready) return "The saved project has blocking validation errors (see Validation).";
  if (S.system && !S.system.executor.configured) return "No backend executor is configured.";
  if (S.system && !S.system.solvers.highs) return "The HiGHS solver is not installed.";
  return null;
}
function renderRuns(section) {
  section.append(h("h2", { id: "h-runs", text: "Runs" }));
  if (S.project) {
    const scenarios = S.project.scenarios || [];
    const scenarioSelect = h("select", { id: "run-scenario" }, h("option", { value: "", text: "Base case" }),
      scenarios.map((s) => h("option", { value: s.id, text: s.name ? `${s.name} (${s.id})` : s.id })));
    const blocker = createRunBlocker();
    const errorBox = h("div", { id: "run-error", "aria-live": "assertive" });
    if (S.lastRunError) errorBox.append(errorPanel(S.lastRunError, "The run could not start"));
    const button = h("button", { type: "button", id: "create-run", class: "primary", disabled: Boolean(blocker),
      "aria-describedby": "create-run-reason", text: "Create and launch run",
      onclick: async () => {
        button.disabled = true; S.lastRunError = null; clear(errorBox);
        try {
          const query = scenarioSelect.value ? `?scenario_id=${encodeURIComponent(scenarioSelect.value)}` : "";
          const run = await api(`/projects/${encodeURIComponent(S.project.id)}/runs${query}`, { method: "POST" });
          const launched = await api(`/runs/${encodeURIComponent(run.id)}/launch`, { method: "POST" });
          S.selectedRunId = run.id;
          if (!launched.accepted) S.lastRunError = { message: `Run ${run.id} failed to start: ${launched.reason}`, code: launched.reason };
          else toast(`Run ${run.id.slice(0, 8)} queued.`, "ok");
        } catch (error) { S.lastRunError = error; }
        await refreshRuns(); await refreshSystem(); render();
      } });
    section.append(h("div", { class: "card" }, h("h3", { text: `New run for ${S.project.metadata?.name || S.project.id}` }),
      h("p", { class: "help", text: `Workflow: ${S.project.workflow}. The run uses an immutable snapshot of the saved project.` }),
      h("div", { class: "form-grid" }, h("label", { for: "run-scenario" }, "Scenario", scenarioSelect)),
      h("div", { class: "actions" }, button,
        h("span", { id: "create-run-reason", class: blocker ? "field-error" : "help", text: blocker || "Ready to run." })),
      errorBox));
  } else {
    section.append(h("p", { class: "info-panel", text: "Showing all runs in the workspace. Open a project to create runs." }));
  }
  if (!S.runs.length) { section.append(h("p", { class: "empty", text: "No runs yet." })); return; }
  section.append(h("div", { class: "table-wrap" }, h("table", { id: "runs-table" },
    h("thead", {}, h("tr", {}, ["Created", "Run", "Scenario", "Workflow", "Status", "QA", "Publishable", "Actions"].map((t) => h("th", { scope: "col", text: t })))),
    h("tbody", {}, S.runs.map((run) => h("tr", { dataset: { runId: run.id }, class: run.id === S.selectedRunId ? "selected" : null },
      h("td", { text: (run.created_at || "").replace("T", " ").slice(0, 19) }), h("td", {}, h("code", { text: run.id.slice(0, 8) })),
      h("td", { text: run.scenario_id || "Base case" }), h("td", { text: run.workflow || "-" }),
      h("td", {}, badge(run.status, statusKind(run.status))),
      h("td", {}, run.qa_status ? badge(run.qa_status, run.qa_status === "pass" ? "ok" : run.qa_status === "warn" ? "warn" : "err") : h("span", { class: "muted", text: "-" })),
      h("td", {}, run.publishable === true ? badge("yes", "ok") : run.publishable === false ? badge("no", "err") : h("span", { class: "muted", text: "-" })),
      h("td", {}, h("div", { class: "actions" },
        h("button", { type: "button", class: "link view-run", text: "Details", onclick: () => { S.selectedRunId = run.id; render(); } }),
        h("button", { type: "button", class: "link", text: "Results", onclick: () => { S.resultsRunId = run.id; show("results"); } }),
        CANCELLABLE.has(run.status) ? h("button", { type: "button", class: "link cancel-run", text: "Cancel", onclick: async () => {
          try { await api(`/runs/${encodeURIComponent(run.id)}/cancel`, { method: "POST" }); toast("Run cancelled.", "info"); }
          catch (error) { toast(`Cancel failed: ${error.message}`, "err"); }
          await refreshRuns(); render(); } }) : null,
        TERMINAL.has(run.status) ? h("button", { type: "button", class: "link", text: "Export", onclick: () => download(`/runs/${encodeURIComponent(run.id)}/export`) }) : null))))))));
  if (S.selectedRunId) {
    const detail = h("div", { class: "card", id: "run-detail" }, h("h3", { text: `Run ${S.selectedRunId.slice(0, 8)} lifecycle` }));
    section.append(detail);
    api(`/runs/${encodeURIComponent(S.selectedRunId)}`).then((run) => {
      detail.append(h("ol", { class: "timeline" }, run.events.map((e) => h("li", {},
        badge(e.status, statusKind(e.status)), " ", h("span", { class: "muted", text: e.timestamp.replace("T", " ").slice(0, 19) }), " ",
        e.message || "", e.details && e.details.error ? h("div", { class: "error-panel", role: "alert", text: `Cause: ${e.details.error}` }) : null))));
    }).catch((error) => detail.append(errorPanel(error)));
  }
}

// ----- results
function renderResults(section) {
  section.append(h("h2", { id: "h-results", text: "Results" }));
  const finished = S.runs.filter((run) => TERMINAL.has(run.status) || run.status === "completed");
  if (!S.runs.length) { section.append(h("p", { class: "empty", text: "No runs yet. Create a run on the Runs page." })); return; }
  if (!S.resultsRunId || !S.runs.some((r) => r.id === S.resultsRunId)) S.resultsRunId = (finished[0] || S.runs[0]).id;
  const select = h("select", { id: "results-run", onchange: () => { S.resultsRunId = select.value; render(); } },
    S.runs.map((run) => h("option", { value: run.id, selected: run.id === S.resultsRunId,
      text: `${(run.created_at || "").replace("T", " ").slice(0, 16)} - ${run.scenario_id || "Base case"} - ${run.status}` })));
  section.append(h("div", { class: "actions" }, h("label", { for: "results-run" }, "Run", select)));
  const panel = h("div", { id: "results-panel", "aria-live": "polite" }, h("p", { class: "muted", text: "Loading result..." }));
  section.append(panel);
  loadSummary(S.resultsRunId).then((summary) => renderSummary(clear(panel), summary))
    .catch((error) => clear(panel).append(errorPanel(error, "The result could not be loaded")));
}
async function loadSummary(runId) {
  if (S.summaries.has(runId)) return S.summaries.get(runId);
  const summary = await api(`/runs/${encodeURIComponent(runId)}/summary`);
  if (TERMINAL.has(summary.status)) S.summaries.set(runId, summary);
  return summary;
}
function renderSummary(panel, summary) {
  const pub = summary.publication;
  panel.append(h("div", { class: "tiles" },
    h("div", { class: "tile" }, h("div", { class: "label", text: "Run status" }), h("div", { id: "result-status" }, badge(summary.status, statusKind(summary.status)))),
    h("div", { class: "tile" }, h("div", { class: "label", text: "QA" }), h("div", { id: "result-qa" },
      summary.qa ? badge(`QA ${summary.qa.status}`, summary.qa.status === "pass" ? "ok" : summary.qa.status === "warn" ? "warn" : "err") : badge("QA not available", "neutral"))),
    h("div", { class: "tile" }, h("div", { class: "label", text: "Publication" }), h("div", { id: "result-publication" },
      pub ? badge(pub.publishable ? (pub.qualified ? "Publishable (with warnings)" : "Publishable") : "Not publishable", pub.publishable ? (pub.qualified ? "warn" : "ok") : "err") : badge("No result", "neutral"))),
    h("div", { class: "tile" }, h("div", { class: "label", text: "Workflow / scenario" }), h("div", { text: `${summary.workflow || "-"} / ${summary.scenario_id || "Base case"}` }))));
  if (!TERMINAL.has(summary.status)) {
    panel.append(h("div", { class: "info-panel", role: "status", text: `The run is ${summary.status}. Results appear here when it completes.` }));
    return;
  }
  if (summary.failure) {
    panel.append(h("div", { class: summary.failure.status === "cancelled" ? "warn-panel" : "error-panel", role: "alert" },
      h("strong", { text: summary.failure.status === "cancelled" ? "Run cancelled" : "Run failed" }),
      h("p", { text: summary.failure.message || "" }), summary.failure.error ? h("p", {}, "Cause: ", h("code", { text: summary.failure.error })) : null));
  }
  if (summary.result_available && !summary.view) {
    panel.append(h("div", { class: "error-panel", id: "results-withheld", role: "alert" },
      h("strong", { text: "Results withheld" }), h("p", { text: summary.withheld_reason }),
      h("p", { text: "Unserved energy of zero does not make a result publishable when canonical QA or the publication gate rejects it." })));
  }
  if (summary.view) {
    if (pub.qualified) panel.append(h("div", { class: "warn-panel", text: "QA passed with warnings. Review the QA checks before using these results." }));
    panel.append(resultView(summary.view));
  }
  if (summary.qa) {
    panel.append(h("details", { class: "card", open: !summary.view },
      h("summary", { text: `QA checks (${summary.qa.checks.length})` }),
      h("div", { class: "table-wrap" }, h("table", { id: "qa-checks" },
        h("thead", {}, h("tr", {}, ["Check", "Status", "Message", "Max violation", "Tolerance"].map((t) => h("th", { text: t })))),
        h("tbody", {}, summary.qa.checks.map((c) => h("tr", {}, h("td", {}, h("code", { text: c.check_id })),
          h("td", {}, badge(c.status, c.status === "pass" ? "ok" : c.status === "warn" ? "warn" : "err")), h("td", { text: c.message || "" }),
          h("td", { class: "num", text: num(c.max_violation, 6) }), h("td", { class: "num", text: num(c.tolerance, 6) }))))))));
  }
  if (summary.solver) {
    panel.append(h("details", { class: "card" }, h("summary", { text: "Solver diagnostics and reproducibility" }),
      h("dl", { class: "kv" },
        [["Backend", `${summary.solver.backend || "-"} ${summary.solver.backend_version || ""}`], ["Status", summary.solver.normalized_status],
         ["Termination", summary.solver.termination_condition], ["Solve time", summary.solver.runtime_s != null ? `${num(summary.solver.runtime_s, 2)} s` : "-"],
         ["Orchestration time", summary.solver.orchestration_runtime_s != null ? `${num(summary.solver.orchestration_runtime_s, 2)} s` : "-"],
         ["Validity", summary.validity], ["Snapshot fingerprint", summary.snapshot_fingerprint],
         ["Persisted hashes verified", summary.verification?.valid ? "yes" : "no"]].map(([k, v]) => [h("dt", { text: k }), h("dd", { text: String(v ?? "-") })]))));
  }
  panel.append(h("div", { class: "actions" }, h("button", { type: "button", id: "export-run", text: "Export run bundle (ZIP)",
    onclick: () => download(`/runs/${encodeURIComponent(summary.run_id)}/export`) }),
    h("span", { class: "help", text: summary.view ? "Includes the gated result envelope and CSV summaries." : "Includes the snapshot and diagnostics; numeric results are excluded because the run is not publishable." })));
}
function resultView(view) {
  const box = h("div", { id: "result-view" });
  if (view.kind === "dispatch") {
    const s = view.system_summary;
    box.append(h("div", { class: "tiles", id: "result-kpis" }, KPI_FIELDS.filter(([key]) => typeof s[key] === "number").map(([key, label, unit]) =>
      h("div", { class: "tile" }, h("div", { class: "label", text: `${label} (${unit})` }), h("div", { class: "value", text: num(s[key], 1) })))));
    if (view.series?.points?.length) box.append(seriesChart(view.series));
    box.append(h("h3", { text: "By resource" }), h("div", { class: "table-wrap" }, h("table", { id: "by-unit-table" },
      h("thead", {}, h("tr", {}, ["Resource", "Type", "Energy (MWh)", "Capacity factor (%)", "Gross cost", "Avg cost (/MWh)", "Starts"].map((t) => h("th", { text: t })))),
      h("tbody", {}, view.by_unit.map((u) => h("tr", {}, h("td", { text: u.name || u.id }), h("td", { text: u.type || "-" }),
        h("td", { class: "num", text: num(u.energy_mwh, 1) }), h("td", { class: "num", text: num(u.capacity_factor, 2) }),
        h("td", { class: "num", text: num(u.gross_cost, 0) }), h("td", { class: "num", text: num(u.avg_cost_mwh, 2) }),
        h("td", { class: "num", text: num(u.starts, 0) })))))));
    if (view.monthly.length) box.append(h("h3", { text: "Monthly summary" }), h("div", { class: "table-wrap" }, h("table", { id: "monthly-table" },
      h("thead", {}, h("tr", {}, ["Month", "Energy (MWh)", "Cost", "Peak load (MW)", "Avg price", "Unserved (MWh)", "Curtailed (MWh)"].map((t) => h("th", { text: t })))),
      h("tbody", {}, view.monthly.map((m) => h("tr", {}, h("td", { text: m.label }), h("td", { class: "num", text: num(m.total_energy_mwh, 0) }),
        h("td", { class: "num", text: num(m.total_cost_usd, 0) }), h("td", { class: "num", text: num(m.peak_load_mw, 1) }),
        h("td", { class: "num", text: num(m.avg_lambda, 2) }), h("td", { class: "num", text: num(m.unserved_mwh, 1) }),
        h("td", { class: "num", text: num(m.curtailed_mwh, 1) })))))));
  } else {
    const entries = Object.entries(view.metrics || {});
    box.append(h("h3", { text: "Reported metrics" }), entries.length ? h("div", { class: "table-wrap" }, h("table", { id: "metrics-table" },
      h("thead", {}, h("tr", {}, h("th", { text: "Metric" }), h("th", { class: "num", text: "Value" }))),
      h("tbody", {}, entries.map(([k, v]) => h("tr", {}, h("td", {}, h("code", { text: k })), h("td", { class: "num", text: num(v, 4) }))))))
      : h("p", { class: "muted", text: "This workflow reported no scalar metrics." }));
  }
  return box;
}
function seriesChart(series) {
  const ns = "http://www.w3.org/2000/svg";
  const points = series.points;
  const width = 900, height = 240, pad = { l: 56, r: 56, t: 14, b: 26 };
  const loads = points.map((p) => p.load_mw).filter((v) => v !== null);
  const prices = points.map((p) => p.price_usd_mwh).filter((v) => v !== null);
  const maxLoad = Math.max(1, ...loads), maxPrice = Math.max(1, ...prices);
  const x = (i) => pad.l + (i / Math.max(1, points.length - 1)) * (width - pad.l - pad.r);
  const yL = (v) => height - pad.b - (v / maxLoad) * (height - pad.t - pad.b);
  const yP = (v) => height - pad.b - (v / maxPrice) * (height - pad.t - pad.b);
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`); svg.setAttribute("class", "chart");
  svg.setAttribute("role", "img");
  const label = series.resolution === "daily_mean" ? "daily mean" : "hourly";
  svg.setAttribute("aria-label", `Load (${label}, peak ${num(maxLoad, 0)} MW) and system price (${label}, max ${num(maxPrice, 1)} USD/MWh) over ${points.length} points`);
  const line = (cls, getter, scale) => {
    const d = points.map((p, i) => (getter(p) === null ? null : `${x(i).toFixed(1)},${scale(getter(p)).toFixed(1)}`)).filter(Boolean).join(" ");
    const el = document.createElementNS(ns, "polyline"); el.setAttribute("class", cls); el.setAttribute("points", d); return el;
  };
  const axis = (x1, y1, x2, y2) => { const el = document.createElementNS(ns, "line"); Object.entries({ x1, y1, x2, y2 }).forEach(([k, v]) => el.setAttribute(k, v)); el.setAttribute("class", "axis"); return el; };
  const text = (tx, ty, content, anchor = "start") => { const el = document.createElementNS(ns, "text"); el.setAttribute("x", tx); el.setAttribute("y", ty); el.setAttribute("text-anchor", anchor); el.textContent = content; return el; };
  svg.append(axis(pad.l, height - pad.b, width - pad.r, height - pad.b), axis(pad.l, pad.t, pad.l, height - pad.b), axis(width - pad.r, pad.t, width - pad.r, height - pad.b),
    text(pad.l - 6, pad.t + 8, num(maxLoad, 0), "end"), text(pad.l - 6, height - pad.b, "0", "end"),
    text(width - pad.r + 6, pad.t + 8, num(maxPrice, 0)), text(width - pad.r + 6, height - pad.b, "0"),
    text(pad.l, height - 6, series.resolution === "daily_mean" ? "Day 1" : "Period 1"), text(width - pad.r, height - 6, `${points.length}`, "end"),
    line("s1", (p) => p.load_mw, yL), line("s2", (p) => p.price_usd_mwh, yP));
  return h("figure", { class: "card" }, h("figcaption", {}, h("strong", { text: `System load and price (${label})` }),
    h("div", { class: "legend" }, h("span", {}, h("span", { class: "sw1" }), "Load, MW (left axis)"), h("span", {}, h("span", { class: "sw2" }), "Price, USD/MWh (right axis)"))), svg);
}

// ----- compare
function renderCompare(section) {
  section.append(h("h2", { id: "h-compare", text: "Compare runs" }),
    h("p", { class: "muted", text: "Only completed runs with persisted results can be compared. Metrics are shown only when both runs are publishable and use the same workflow." }));
  const completed = S.runs.filter((run) => run.status === "completed");
  if (completed.length < 2) { section.append(h("p", { class: "empty", text: "At least two completed runs are needed for a comparison." })); return; }
  const option = (run, selected) => h("option", { value: run.id, selected,
    text: `${run.id.slice(0, 8)} - ${run.scenario_id || "Base case"} - ${run.workflow} - ${run.publishable === false ? "not publishable" : "publishable"}` });
  const left = h("select", { id: "compare-left" }, completed.map((r, i) => option(r, i === 1)));
  const right = h("select", { id: "compare-right" }, completed.map((r, i) => option(r, i === 0)));
  const out = h("div", { id: "compare-result", "aria-live": "polite" });
  const check = () => {
    const same = left.value === right.value;
    $("#compare-runs").disabled = same;
    $("#compare-reason").textContent = same ? "Select two different runs to compare." : "";
  };
  left.addEventListener("change", check); right.addEventListener("change", check);
  section.append(h("div", { class: "card" }, h("div", { class: "form-grid" },
    h("label", { for: "compare-left" }, "Baseline run (left)", left), h("label", { for: "compare-right" }, "Comparison run (right)", right)),
    h("div", { class: "actions" }, h("button", { type: "button", id: "compare-runs", class: "primary", text: "Compare",
      onclick: async () => {
        clear(out).append(h("p", { class: "muted", text: "Comparing..." }));
        try { S.compare = await api(`/runs/compare?left_run_id=${encodeURIComponent(left.value)}&right_run_id=${encodeURIComponent(right.value)}`); renderComparison(clear(out), S.compare); }
        catch (error) { clear(out).append(errorPanel(error, "Comparison failed")); }
      } }), h("span", { id: "compare-reason", class: "field-error" }))), out);
  check();
  if (S.compare && left.value === S.compare.left_run_id) renderComparison(out, S.compare);
}
function renderComparison(out, c) {
  out.append(h("div", { class: c.compatible ? "ok-panel" : "warn-panel", role: "status" },
    h("strong", { text: c.compatible ? "Compatible runs" : "Not directly comparable" }),
    h("p", { text: c.compatible ? `Same snapshot: ${c.same_snapshot ? "yes" : "no"}.` : c.compatibility_reason })));
  const l = c.summaries.left, r = c.summaries.right;
  const meta = [["Workflow", l.workflow, r.workflow], ["Scenario", l.scenario_id || "Base case", r.scenario_id || "Base case"],
    ["QA", l.qa_status, r.qa_status], ["Validity", l.validity, r.validity],
    ["Publishable", String(c.publication.left.publishable), String(c.publication.right.publishable)]];
  const keys = Array.from(new Set([...Object.keys(l.metrics || {}), ...Object.keys(r.metrics || {})]));
  out.append(h("div", { class: "table-wrap" }, h("table", { id: "compare-table" },
    h("thead", {}, h("tr", {}, ["Item", "Left", "Right", "Difference (right - left)"].map((t) => h("th", { text: t })))),
    h("tbody", {}, meta.map(([k, a, b]) => h("tr", {}, h("td", { text: k }), h("td", { text: a ?? "-" }), h("td", { text: b ?? "-" }), h("td", { text: "" }))),
      keys.map((k) => {
        const a = l.metrics?.[k], b = r.metrics?.[k];
        return h("tr", {}, h("td", {}, h("code", { text: k })), h("td", { class: "num", text: num(a) }), h("td", { class: "num", text: num(b) }),
          h("td", { class: "num", text: typeof a === "number" && typeof b === "number" ? num(b - a) : "unavailable" }));
      })))));
  if (c.compatible && !keys.length) out.append(h("p", { class: "muted", text: "Neither run emitted comparable canonical metrics." }));
}

// ----- reference data
async function loadReferenceTable() {
  const ref = S.ref;
  if (ref.table === "quality") { if (!ref.quality) ref.quality = await api("/reference/quality"); return; }
  ref.data = await api(`/reference/tables/${ref.table}?limit=1000${ref.q ? `&q=${encodeURIComponent(ref.q)}` : ""}`);
}
function renderReference(section) {
  const ref = S.ref;
  section.append(h("h2", { id: "h-reference", text: "Reference data" }));
  if (!ref.summary) {
    section.append(h("p", { class: "muted", text: "Loading reference catalogue..." }));
    api("/reference").then((summary) => { ref.summary = summary; return loadReferenceTable(); })
      .then(() => { if (S.view === "reference") render(); })
      .catch((error) => section.append(errorPanel(error, "The reference catalogue is unavailable")));
    return;
  }
  const s = ref.summary;
  section.append(h("div", { class: "warn-panel", id: "reference-banner" },
    h("strong", { text: "Reference catalogue - requires user review before use in a study" }),
    h("p", { text: s.important_note }),
    h("p", { class: "help" }, `Source: ${s.source_file} (SHA-256 `, h("code", { text: shortHash(s.sha256) }),
      `), ${s.sheet_count} sheets: ${s.tables.stations.records} station rows, ${s.tables.reservoirs.records} reservoirs, ${s.tables.hydropower.records} hydropower rows, ${s.tables.hpp_summary.records} HPP summary rows, ${s.detail_sheets} plant technical sheets.`)));
  const tabs = [["stations", "Stations"], ["reservoirs", "Reservoirs"], ["hydropower", "Hydropower"], ["hpp_summary", "HPP summary"], ["quality", "Quality report"]];
  section.append(h("div", { class: "tabs", role: "tablist", "aria-label": "Reference tables" }, tabs.map(([id, label]) =>
    h("button", { type: "button", role: "tab", id: `ref-tab-${id}`, "aria-selected": String(ref.table === id), dataset: { refTable: id }, text: label,
      onclick: async () => { ref.table = id; ref.detail = null; await loadReferenceTable(); render(); } }))));
  if (ref.table === "quality") section.append(qualityPanel());
  else section.append(referenceTablePanel());
  if (ref.detail) section.append(referenceDetail(ref.detail));
  section.append(stagingPanel());
}
function referenceTablePanel() {
  const ref = S.ref;
  const data = ref.data;
  const box = h("div", { role: "tabpanel", "aria-labelledby": `ref-tab-${ref.table}` });
  const search = h("input", { id: "reference-search", type: "search", value: ref.q, placeholder: "Search name, river, region...", "aria-label": "Search the reference table" });
  let timer = null;
  search.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(async () => { ref.q = search.value; await loadReferenceTable(); render(); const again = $("#reference-search"); again.focus(); again.setSelectionRange(again.value.length, again.value.length); }, 250);
  });
  box.append(h("div", { class: "actions" }, search, h("span", { class: "help", id: "reference-count", text: data ? `${data.total} record(s)` : "" })));
  if (!data) return box;
  if (!data.records.length) { box.append(h("p", { class: "empty", text: "No records match the search." })); return box; }
  const columns = data.columns.filter((c) => !["latitude", "longitude"].includes(c.key)).slice(0, 9);
  box.append(h("div", { class: "table-wrap tall" }, h("table", { id: "reference-table" },
    h("thead", {}, h("tr", {}, ref.table === "stations" ? h("th", { scope: "col", text: "Stage" }) : null,
      h("th", { scope: "col", text: "Row" }), h("th", { scope: "col", text: "Type" }),
      columns.map((c) => h("th", { scope: "col", text: c.unit ? `${c.source_header}` : c.source_header })), h("th", { scope: "col", text: "Quality" }), h("th", { text: "" }))),
    h("tbody", {}, data.records.map((record) => {
      const quality = record.quality;
      const name = record.values.plant_name || record.values.reservoir_name || "(no name)";
      return h("tr", { class: ref.selected.has(record.record_id) ? "selected" : null },
        ref.table === "stations" ? h("td", {}, record.importable
          ? h("input", { type: "checkbox", class: "stage-checkbox", value: record.record_id, checked: ref.selected.has(record.record_id),
              "aria-label": `Stage ${name} for import`,
              onchange: (event) => { if (event.target.checked) ref.selected.set(record.record_id, name); else ref.selected.delete(record.record_id); renderStagingOnly(); } })
          : h("span", { class: "help", title: "Section headings and totals cannot be imported", text: "n/a" })) : null,
        h("td", { class: "num", text: record.source_row }), h("td", { class: "help", text: record.record_type }),
        columns.map((c) => h("td", { class: typeof record.values[c.key] === "number" ? "num" : null, text: record.values[c.key] ?? "" })),
        h("td", {}, quality.error ? badge(`${quality.error} error`, "err") : null, " ", quality.warning ? badge(`${quality.warning} warn`, "warn") : null,
          !quality.error && !quality.warning ? badge("ok", "ok") : null),
        h("td", {}, h("button", { type: "button", class: "link", text: "Details", "aria-label": `Details of ${name}`, onclick: async () => {
          ref.detail = await api(`/reference/records/${encodeURIComponent(record.record_id)}`); render(); $("#reference-detail")?.scrollIntoView(); } })));
    })))));
  return box;
}
function qualityPanel() {
  const q = S.ref.quality;
  const box = h("div", { role: "tabpanel", "aria-labelledby": "ref-tab-quality" });
  if (!q) return box;
  box.append(h("p", {}, badge(`${q.errors} errors`, "err"), " ", badge(`${q.warnings} warnings`, "warn"), " ",
    h("span", { class: "help", text: `Capacity tolerance ${q.tolerances.capacity_relative * 100}% / ${q.tolerances.capacity_absolute_mw} MW; name similarity ${q.tolerances.name_similarity}.` })),
    h("div", { class: "table-wrap" }, h("table", { id: "quality-counts" }, h("thead", {}, h("tr", {}, h("th", { text: "Check" }), h("th", { class: "num", text: "Findings" }))),
      h("tbody", {}, Object.entries(q.counts).map(([code, count]) => h("tr", {}, h("td", {}, h("code", { text: code })), h("td", { class: "num", text: count })))))));
  const severity = h("select", { id: "quality-severity", onchange: () => { S.ref.qualitySeverity = severity.value; render(); } },
    ["error", "warning", "info", "all"].map((v) => h("option", { value: v, selected: v === S.ref.qualitySeverity, text: v })));
  const issues = q.issues.filter((i) => S.ref.qualitySeverity === "all" || i.severity === S.ref.qualitySeverity);
  box.append(h("div", { class: "actions" }, h("label", { for: "quality-severity" }, "Severity", severity), h("span", { class: "help", text: `${issues.length} finding(s)` })),
    h("div", { class: "table-wrap tall" }, h("table", { id: "quality-issues" },
      h("thead", {}, h("tr", {}, ["Severity", "Table", "Row", "Code", "Finding", "Remediation"].map((t) => h("th", { text: t })))),
      h("tbody", {}, issues.slice(0, 500).map((i) => h("tr", {}, h("td", {}, badge(i.severity, severityKind(i.severity))), h("td", { text: i.table }),
        h("td", { class: "num", text: i.source_row ?? "" }), h("td", {}, h("code", { text: i.code })), h("td", { text: i.message }), h("td", { class: "help", text: i.remediation || "" })))))));
  return box;
}
function referenceDetail(detail) {
  const record = detail.record;
  const name = record.values.plant_name || record.values.reservoir_name;
  const card = h("div", { class: "card", id: "reference-detail" },
    h("div", { class: "actions" }, h("h3", { text: `${name || "(no name)"} - ${record.source_sheet} row ${record.source_row}` }),
      h("button", { type: "button", class: "link", text: "Close", onclick: () => { S.ref.detail = null; render(); } })),
    h("dl", { class: "kv" }, Object.entries(record.values).map(([k, v]) => [h("dt", { text: k }), h("dd", { text: v ?? "-" })])),
    h("h4", { text: "Quality findings" }), issuesTable(detail.issues.map((i) => ({ ...i, path: i.field }))),
    h("h4", { text: "Records in other tables with similar names" }),
    detail.matches.length ? h("ul", {}, detail.matches.map((m) => h("li", {}, `${m.table}: ${m.name} `, badge(m.relation, m.relation === "exact" ? "ok" : "warn"), m.source_row ? ` (row ${m.source_row})` : "")))
      : h("p", { class: "muted", text: "No matches." }));
  if (detail.detail_sheet) {
    card.append(h("details", {}, h("summary", { text: `Plant technical sheet: ${detail.detail_sheet.sheet} (${detail.detail_sheet.relation} match)` }),
      h("div", { class: "table-wrap" }, h("table", {}, h("tbody", {}, detail.detail_sheet.rows.map((row) => h("tr", {},
        h("td", { class: "num help", text: row.source_row }), row.cells.map((cell) => h("td", { text: cell ?? "" })))))))));
  }
  return card;
}
function renderStagingOnly() {
  const old = $("#staging-panel");
  if (old) old.replaceWith(stagingPanel());
}
function stagingPanel() {
  const ref = S.ref;
  const panel = h("div", { class: "card", id: "staging-panel" }, h("h3", { text: "Stage records into a project" }));
  if (!ref.selected.size) {
    panel.append(h("p", { class: "muted", id: "staged-count", text: "No records staged. Tick plant rows in the Stations table to stage them." }));
  } else {
    panel.append(h("p", { id: "staged-count", text: `${ref.selected.size} record(s) staged: ${Array.from(ref.selected.values()).join(", ")}` }));
  }
  const reviewButton = h("button", { type: "button", id: "review-import", class: "primary", disabled: !ref.selected.size, text: "Review import mapping",
    onclick: () => runReview(panel) });
  panel.append(h("div", { class: "actions" }, reviewButton,
    h("button", { type: "button", id: "clear-staged", disabled: !ref.selected.size, text: "Clear staged records",
      onclick: () => { ref.selected.clear(); ref.review = null; ref.overrides = {}; render(); } })));
  if (ref.review) panel.append(reviewPanel(ref.review));
  if (ref.importResult) panel.append(h("div", { class: "ok-panel", id: "import-result", role: "status" },
    h("strong", { text: `Imported ${ref.importResult.asset_ids.length} asset(s) into project ${ref.importResult.project_id} (revision ${ref.importResult.revision})` }),
    h("p", { text: `Assets ${ref.importResult.asset_ids.join(", ")} were added disabled with review status 'requires review'. Review them on the Assets page before enabling them.` })));
  return panel;
}
function selectionsPayload() {
  return Array.from(S.ref.selected.keys()).map((record_id) => {
    const overrides = {};
    for (const [field, value] of Object.entries(S.ref.overrides[record_id] || {})) {
      if (value.value !== "" && value.value !== undefined) overrides[field] = value;
    }
    return { record_id, overrides };
  });
}
async function runReview(panel) {
  const ref = S.ref;
  try {
    ref.review = await api("/reference/review", { method: "POST", body: { project_id: S.project && S.savedJson ? S.project.id : null, selections: selectionsPayload() } });
    ref.importResult = null;
  } catch (error) { ref.review = null; panel.append(errorPanel(error, "The review could not be produced")); return; }
  render();
  $("#import-review")?.scrollIntoView();
}
function reviewPanel(report) {
  const ref = S.ref;
  const box = h("div", { id: "import-review", class: "stack" },
    h("div", { class: report.importable ? "info-panel" : "error-panel", role: "status" },
      h("strong", { text: report.importable ? "No blocking issues. Review the warnings and assumptions before importing." : `${report.errors} blocking issue(s) must be resolved before import.` }),
      h("p", { text: `${report.warnings} warning(s) require acknowledgement. Importer ${report.importer_version}; workbook SHA-256 ${shortHash(report.source_sha256)}.` })));
  for (const review of report.reviews) {
    const overrides = ref.overrides[review.record_id] || (ref.overrides[review.record_id] = {});
    const card = h("div", { class: `review-card ${review.blocking ? "blocking" : "ready"}`, dataset: { recordId: review.record_id } },
      h("h4", {}, `${review.name || review.record_id} `, review.blocking ? badge(`${review.errors} blocking`, "err") : badge("importable", "ok"), " ",
        review.warnings ? badge(`${review.warnings} warnings`, "warn") : null),
      review.source ? h("p", { class: "help", text: `${review.source.sheet}, row ${review.source.row} (${review.record_id})` }) : null,
      h("div", { class: "table-wrap" }, h("table", { class: "mapping-table" },
        h("thead", {}, h("tr", {}, ["PowerSim field", "Value", "Status", "Origin", "Source cell", "Note"].map((t) => h("th", { text: t })))),
        h("tbody", {}, review.fields.map((f) => h("tr", {}, h("td", {}, h("code", { text: f.target })),
          h("td", { text: f.value === null || f.value === undefined ? "-" : String(f.value) }),
          h("td", {}, badge(f.status, { mapped: "ok", override: "info", derived: "info", assumed: "warn", missing: "err", conflict: "err", unmapped_required: "warn", not_in_source: "neutral" }[f.status] || "neutral")),
          h("td", { text: f.origin }), h("td", { class: "help", text: f.source_ref ? `${f.source_ref.sheet} r${f.source_ref.row} ${f.source_ref.column}: ${f.source_ref.raw ?? "(empty)"}` : "-" }),
          h("td", { class: "help", text: f.reason ? `Override reason: ${f.reason}` : (f.note || "") })))))),
      h("h4", { text: "Issues" }), issuesTable(review.issues.map((i) => ({ ...i, path: i.field }))));
    const overrideRows = OVERRIDE_FIELDS.filter(([field]) => field !== "fuel_type" || review.kind === "thermal" || overrides.kind?.value === "thermal").map(([field, label]) => {
      const current = overrides[field] || { value: "", reason: "" };
      const valueId = `ov-${review.record_id}-${field}`.replace(/[^A-Za-z0-9_-]/g, "_");
      const valueInput = field === "kind"
        ? h("select", { id: valueId }, h("option", { value: "", text: "(no override)" }), ["thermal", "hydro_ror", "hydro_reg", "solar", "wind"].map((k) => h("option", { value: k, selected: current.value === k, text: k })))
        : h("input", { id: valueId, value: current.value ?? "", type: field.startsWith("capacity") ? "number" : "text", step: "any" });
      const reasonInput = h("input", { id: `${valueId}-reason`, value: current.reason || "", placeholder: "Reason and source (required)", "aria-label": `Reason for ${label} override` });
      const sync = () => { overrides[field] = { value: field.startsWith("capacity") && valueInput.value !== "" ? Number(valueInput.value) : valueInput.value, reason: reasonInput.value }; };
      valueInput.addEventListener("input", sync); valueInput.addEventListener("change", sync); reasonInput.addEventListener("input", sync);
      return h("tr", {}, h("td", {}, h("label", { for: valueId, text: label })), h("td", {}, valueInput), h("td", {}, reasonInput));
    });
    card.append(h("details", { open: review.blocking }, h("summary", { text: "Manual overrides (recorded in provenance)" }),
      h("table", { class: "override-table" }, h("thead", {}, h("tr", {}, ["Field", "Override value", "Reason"].map((t) => h("th", { text: t })))), h("tbody", {}, overrideRows))));
    box.append(card);
  }
  const ack = h("input", { type: "checkbox", id: "ack-warnings", onchange: () => updateApplyButton() });
  const reviewer = h("input", { id: "import-reviewer", placeholder: "Your name or initials (optional)" });
  const applyReason = h("span", { id: "apply-import-reason", class: "field-error" });
  const applyButton = h("button", { type: "button", id: "apply-import", class: "primary", text: S.project ? `Import into project ${S.project.id}` : "Import into project",
    onclick: async () => {
      try {
        const result = await api(`/projects/${encodeURIComponent(S.project.id)}/reference-import`, { method: "POST",
          body: { selections: selectionsPayload(), acknowledge_warnings: ack.checked, reviewer: reviewer.value.trim() || null } });
        S.ref.importResult = result; S.ref.review = null; S.ref.selected.clear(); S.ref.overrides = {};
        await openProject(result.project_id); await refreshProjects(); await refreshSystem();
        toast(`Imported ${result.asset_ids.length} reference asset(s), disabled until reviewed.`, "ok"); render();
      } catch (error) { box.append(errorPanel(error, "The import was rejected")); }
    } });
  function updateApplyButton() {
    let reason = "";
    if (!S.project || !S.savedJson) reason = "Open a saved project to import into.";
    else if (isDirty()) reason = "Save the open project before importing.";
    else if (!report.importable) reason = "Resolve the blocking issues (overrides with reasons), then review again.";
    else if (report.warnings && !ack.checked) reason = "Acknowledge the warnings to continue.";
    applyButton.disabled = Boolean(reason); applyReason.textContent = reason;
  }
  box.append(h("div", { class: "card" },
    h("div", { class: "actions" }, h("button", { type: "button", id: "rerun-review", text: "Review again with overrides", onclick: () => runReview($("#staging-panel")) })),
    h("label", { class: "inline", for: "ack-warnings" }, ack, `I have reviewed the ${report.warnings} warning(s) and importer assumptions. Imported assets stay disabled until reviewed.`),
    h("label", { for: "import-reviewer" }, "Imported by", reviewer),
    h("div", { class: "actions" }, applyButton, applyReason)));
  updateApplyButton();
  return box;
}

// ----- about
function renderAbout(section) {
  section.append(h("h2", { id: "h-about", text: "Capabilities and limits" }));
  if (!S.system) { section.append(h("p", { class: "muted", text: "Backend information is unavailable." })); return; }
  const s = S.system;
  section.append(h("div", { class: "card" }, h("h3", { text: `PowerSim ${s.version}` }),
    h("dl", { class: "kv" }, [["Contract version", s.contract_version], ["Executor", s.executor.configured ? s.executor.mode : "not configured"],
      ["HiGHS", s.solvers.highs || "not installed"], ["Pyomo", s.solvers.pyomo || "not installed"],
      ["Reference catalogue", s.reference_catalogue.available ? `${s.reference_catalogue.source_file} (${shortHash(s.reference_catalogue.sha256)})` : `unavailable: ${s.reference_catalogue.reason}`]]
      .map(([k, v]) => [h("dt", { text: k }), h("dd", { text: String(v) })]))),
    h("div", { class: "card" }, h("h3", { text: "Validated workflows" }), h("ul", {}, s.workflows.map((w) => h("li", {}, h("strong", { text: w.label }), ` - ${w.description}`)))),
    h("div", { class: "warn-panel" }, h("strong", { text: "Not supported" }), h("ul", {}, s.unsupported.map((u) => h("li", { text: u })))),
    h("div", { class: "card" }, h("h3", { text: "Data boundaries" }), h("ul", {},
      h("li", { text: "The Georgia demonstration is sanitized and aggregated. It is not official GSE operational data and not an official dispatch case." }),
      h("li", { text: "The plant-parameter reference catalogue is reference data. Its values must be reviewed against approved sources before they are used in a study; imported assets stay disabled until a user marks them reviewed." }),
      h("li", { text: "Results are shown only when the run's result is valid and passes canonical QA and the publication gate." }),
      h("li", { text: "See docs/POWERSIM_1_0_CAPABILITIES.md, docs/POWERSIM_1_0_LIMITATIONS.md and docs/POWERSIM_1_0_USER_GUIDE.md in the distribution." }))));
}

// ---------------------------------------------------------------- startup
async function start() {
  for (const button of document.querySelectorAll(".sidenav button")) button.addEventListener("click", () => show(button.dataset.view));
  $("#validate-project").addEventListener("click", async () => { await validateProject(); show("validation"); });
  $("#save-project").addEventListener("click", () => saveProject());
  $("#export-project").addEventListener("click", (event) => { if (event.currentTarget.getAttribute("aria-disabled") === "true") { event.preventDefault(); toast("Save the project before exporting it.", "warn"); } });
  window.addEventListener("beforeunload", (event) => { if (isDirty()) { event.preventDefault(); event.returnValue = ""; } });
  await refreshSystem();
  try { await refreshProjects(); await refreshRuns(); } catch (error) { console.warn(error); }
  const initial = location.hash.replace("#", "");
  S.view = VIEWS[initial] ? initial : "overview";
  render();
  document.body.dataset.ready = "true";
}
start();
