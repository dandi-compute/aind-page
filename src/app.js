/* ─── Configuration ─────────────────────────────────────────── */
const OWNER = "dandi-compute";
const REPO = "001697";
const BRANCH = "draft";
const CDN_BASE = `https://raw.githubusercontent.com/${OWNER}/${REPO}/${BRANCH}`;

const QUEUE_CDN_BASE = `https://raw.githubusercontent.com/dandi-compute/queue/compressed`;

const GITHUB_API_BASE = `https://api.github.com/repos/${OWNER}/${REPO}`;

const PIPELINE_REPO_URL = "https://github.com/CodyCBakerPhD/aind-ephys-pipeline";
const PIPELINE_API_BASE = "https://api.github.com/repos/CodyCBakerPhD/aind-ephys-pipeline";
const CODE_REPO_URL = "https://github.com/dandi-compute/code";
const AIND_EPHYS_PIPELINE_CODE_URL =
    "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline";
const PARAMS_SCHEMA_URL =
    "https://raw.githubusercontent.com/CodyCBakerPhD/aind-ephys-pipeline/main/pipeline/default_params_schema.json";
const PARAMS_PLACEHOLDER_URL =
    "https://raw.githubusercontent.com/CodyCBakerPhD/aind-ephys-pipeline/main/pipeline/default_params.json";
const REGISTRY_FALLBACK_ALIAS_PRIORITY = 1;
const MIN_SHORT_COMMIT_HASH_LENGTH = 6;
const FULL_COMMIT_HASH_LENGTH = 40;
const DANDI_CODE_REPO_PATTERN = /github\.com\/dandi-compute\/code(?:\/|$)/;
const ROOT_DIFF_PATH_LABEL = "(root)";
const COMMIT_HASH_PATTERN = new RegExp(`^[0-9a-f]{${MIN_SHORT_COMMIT_HASH_LENGTH},${FULL_COMMIT_HASH_LENGTH}}$`, "i");
const REGISTERED_PARAMS_PATH = "src/dandi_compute_code/aind_ephys_pipeline/registries/registered_params.json";
const REGISTERED_CONFIGS_PATH = "src/dandi_compute_code/aind_ephys_pipeline/registries/registered_configs.json";
let PARAMS_REGISTRY = [];
let CONFIG_REGISTRY = [];
/* Dandisets used for internal testing – hidden from the main view and moved to
   the dedicated Tests page (?view=tests). */
const TEST_DANDISETS = new Set(["001849"]);
/* Per-dandiset fallback subject used when a queue entry carries a null subject */
const DANDISET_SUBJECT_DEFAULTS = new Map([["001849", "test"]]);

/* Module-level view mode ("tests" | null), set during init */
let _viewMode = null;

/* Module-level layout mode ("tree" | "flat"), toggled by the layout bar */
let _layoutMode = "tree";
/* Module-level sort mode ("attempt" | "created_at"), toggled by the layout bar */
let _sortMode = "attempt";
/* Module-level sort direction ("desc" | "asc"), toggled by the layout bar */
let _sortDirection = "desc";
/* Cached filtered runs for re-rendering on layout toggle */
let _filteredRuns = [];

function parseViewMode() {
    return new URLSearchParams(window.location.search).get("view") ?? null;
}

function syncTopNav(viewMode = parseViewMode()) {
    const navLinks = [
        { selector: '.site-view-toggle-link[href="./"]', mode: null },
        { selector: '.site-view-toggle-link[href="?view=compare"]', mode: "compare" },
        { selector: '.site-view-toggle-link[href="?view=params"]', mode: "params" },
        { selector: '.site-view-toggle-link[href="?view=tests"]', mode: "tests" },
    ];
    navLinks.forEach(({ selector, mode }) => {
        const link = document.querySelector(selector);
        if (!link) return;
        const active = viewMode === mode;
        link.classList.toggle("active", active);
        if (active) link.setAttribute("aria-current", "page");
        else link.removeAttribute("aria-current");
    });
}

function parseLayoutMode() {
    const layout = new URLSearchParams(window.location.search).get("layout");
    if (layout === "flat" || layout === "tree") return layout;
    return localStorage.getItem("layoutMode") === "flat" ? "flat" : "tree";
}

function parseSortMode() {
    const sort = new URLSearchParams(window.location.search).get("sort");
    if (sort === "attempt" || sort === "created_at") return sort;
    return localStorage.getItem("sortMode") === "created_at" ? "created_at" : "attempt";
}

function parseSortDirection() {
    const sortDir = new URLSearchParams(window.location.search).get("sortDir");
    if (sortDir === "asc" || sortDir === "desc") return sortDir;
    return localStorage.getItem("sortDirection") === "asc" ? "asc" : "desc";
}

function updateLayoutModeUrl(mode) {
    if (mode !== "flat" && mode !== "tree") return;
    const params = new URLSearchParams(window.location.search);
    params.set("layout", mode);
    const qs = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${qs ? `?${qs}` : ""}${window.location.hash}`);
}

function updateSortModeUrl(mode) {
    if (mode !== "attempt" && mode !== "created_at") return;
    const params = new URLSearchParams(window.location.search);
    params.set("sort", mode);
    const qs = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${qs ? `?${qs}` : ""}${window.location.hash}`);
}

function updateSortDirectionUrl(direction) {
    if (direction !== "asc" && direction !== "desc") return;
    const params = new URLSearchParams(window.location.search);
    params.set("sortDir", direction);
    const qs = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${qs ? `?${qs}` : ""}${window.location.hash}`);
}

function codeRepoBlobUrl(path) {
    return `${CODE_REPO_URL}/blob/main/${path.split("/").map(encodeURIComponent).join("/")}`;
}

function codeRepoRawUrl(path) {
    return `https://raw.githubusercontent.com/dandi-compute/code/main/${path.split("/").map(encodeURIComponent).join("/")}`;
}

function dandiBaseUrl(_dandisetId) {
    return "https://dandiarchive.org";
}
function dandiApiBaseUrl(_dandisetId) {
    return "https://api.dandiarchive.org";
}

/* ─── URL-based filtering ───────────────────────────────────── */
function parseFilter() {
    const params = new URLSearchParams(window.location.search);
    return {
        dandisetId: params.get("dandiset") ?? null,
        subject: params.get("subject") ?? null,
        session: params.get("session") ?? null,
        pipelineVersion: params.get("version") ?? null,
        paramsType: params.get("params") ?? null,
        configType: params.get("config") ?? null,
        dandiCodebaseHash: params.get("codebaseHash") ?? null,
        failureStep: params.get("failureStep") ?? null,
        status: params.get("status") ?? null,
    };
}

// Extract a stable hash-like identifier for the dandi-compute/code source
// backing a run. Prefer explicit commit-looking versions, then commit-like
// `+hash` segments, and finally fall back to the raw version text.
function runDandiCodebaseHash(run) {
    const generatedByEntries = Array.isArray(run.generatedBy) ? run.generatedBy : [];
    for (const entry of generatedByEntries) {
        if (!DANDI_CODE_REPO_PATTERN.test(String(entry.CodeURL ?? ""))) continue;
        const version = String(entry.Version ?? "").trim();
        if (!version) continue;
        if (COMMIT_HASH_PATTERN.test(version)) return version;
        const refCandidate = pipelineCompareRefCandidates(version)[0];
        if (refCandidate) return refCandidate;
        return version;
    }
    return null;
}

// Resolve a run params identifier to a registered alias when available;
// otherwise preserve the raw params value for matching and display.
function runParamsType(run) {
    const alias = resolveRegistryAlias(run.paramsProfile, PARAMS_REGISTRY)?.alias;
    return alias ?? run.paramsProfile ?? null;
}

// Resolve a run config identifier to a registered alias when available;
// otherwise preserve the raw config value for matching and display.
function runConfigType(run) {
    const alias = resolveRegistryAlias(run.configHash, CONFIG_REGISTRY)?.alias;
    return alias ?? run.configHash ?? null;
}

function matchesResolvedOrRawValue(filterValue, resolvedValue, rawValue) {
    if (!filterValue) return true;
    return resolvedValue === filterValue || rawValue === filterValue;
}

function classifyFailedTaskStep(taskName = "") {
    const normalized = String(taskName).toLowerCase();
    if (/dispatch/.test(normalized)) return "job-dispatch";
    if (/pre[\s_-]*process/.test(normalized)) return "pre-processing";
    if (/post[\s_-]*process/.test(normalized)) return "post-processing";
    return "other";
}

function isFailedStatus(status) {
    return String(status).toLowerCase() === "failed";
}

function runFailureStep(run) {
    if (!isFailedStatus(run.status)) return null;
    const failedTasks = (run.tasks ?? []).filter((task) => isFailedStatus(task.status));
    if (failedTasks.length === 0) return "other";

    const failedSteps = failedTasks.map((task) => classifyFailedTaskStep(task.name));
    if (failedSteps.includes("pre-processing")) return "pre-processing";
    if (failedSteps.includes("post-processing")) return "post-processing";
    if (failedSteps.includes("job-dispatch")) return "job-dispatch";
    return "other";
}

function normalizeStatus(status) {
    return status ? String(status).toLowerCase() : null;
}

function applyFilter(runs, filter) {
    const normalizedFilterStatus = normalizeStatus(filter.status);
    return runs.filter((r) => {
        if (filter.dandisetId && r.dandisetId !== filter.dandisetId) return false;
        if (filter.subject && r.subject !== filter.subject) return false;
        if (filter.session && r.session !== filter.session) return false;
        if (filter.pipelineVersion && r.pipelineVersion !== filter.pipelineVersion) return false;
        if (!matchesResolvedOrRawValue(filter.paramsType, runParamsType(r), r.paramsProfile)) return false;
        if (!matchesResolvedOrRawValue(filter.configType, runConfigType(r), r.configHash)) return false;
        if (filter.dandiCodebaseHash && runDandiCodebaseHash(r) !== filter.dandiCodebaseHash) return false;
        if (normalizedFilterStatus && String(r.status).toLowerCase() !== normalizedFilterStatus) return false;
        if (filter.failureStep) {
            if (!isFailedStatus(r.status)) return false;
            const failedStep = r.failureStep;
            if (filter.failureStep === "exclude-job-dispatch") return failedStep !== "job-dispatch";
            return failedStep === filter.failureStep;
        }
        return true;
    });
}

/* Build a page URL with the given filter parameters.
   When on the tests page (_viewMode === "tests"), the view=tests param is
   automatically preserved so navigation stays within the tests scope.
   When on the main page (_viewMode === null), no view param is emitted. */
function narrowUrl(params) {
    const sp = new URLSearchParams();
    sp.set("layout", parseLayoutMode());
    sp.set("sort", parseSortMode());
    sp.set("sortDir", parseSortDirection());
    if (_viewMode === "tests") sp.set("view", "tests");
    if (params.dandiset) sp.set("dandiset", params.dandiset);
    if (params.subject) sp.set("subject", params.subject);
    if (params.session) sp.set("session", params.session);
    if (params.pipelineVersion) sp.set("version", params.pipelineVersion);
    if (params.paramsType) sp.set("params", params.paramsType);
    if (params.configType) sp.set("config", params.configType);
    if (params.dandiCodebaseHash) sp.set("codebaseHash", params.dandiCodebaseHash);
    if (params.failureStep) sp.set("failureStep", params.failureStep);
    if (params.status) sp.set("status", params.status);
    const qs = sp.toString();
    return qs ? `?${qs}` : "./";
}

const FILTER_VALUE_COLLATOR = new Intl.Collator();
const uniqueSortedValues = (items) => [...new Set(items.filter(Boolean))].sort(FILTER_VALUE_COLLATOR.compare);
const FAILURE_STEP_FILTER_OPTIONS = ["exclude-job-dispatch", "pre-processing", "post-processing"];
const STATUS_LABELS = {
    success: "Successful",
    failed: "Failed",
    queued: "Queued",
    partial: "Partial",
};
const DECIMAL_DATA_SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
const DATA_SIZE_FORMATTER = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });

function normalizeByteCount(value) {
    if (value === null || value === undefined || value === "") return null;
    const numericValue = Number(value);
    if (!Number.isFinite(numericValue) || numericValue < 0) return null;
    return Math.round(numericValue);
}

function runByteCount(run) {
    return normalizeByteCount(run?.assetSizeBytes);
}

function sumRunByteCounts(runs) {
    return runs.reduce((sum, run) => {
        const bytes = runByteCount(run);
        return bytes === null ? sum : sum + bytes;
    }, 0);
}

function formatByteCount(value) {
    if (!Number.isFinite(value) || value < 0) return "0 B";
    let scaledValue = value;
    let unitIndex = 0;
    while (scaledValue >= 1000 && unitIndex < DECIMAL_DATA_SIZE_UNITS.length - 1) {
        scaledValue /= 1000;
        unitIndex += 1;
    }
    return `${DATA_SIZE_FORMATTER.format(scaledValue)} ${DECIMAL_DATA_SIZE_UNITS[unitIndex]}`;
}

function renderFilterInput(name, label, value, suggestions, clearHref = null) {
    const listId = `filter-options-${name}`;
    const options = suggestions.map((item) => `<option value="${e(item)}"></option>`).join("");
    const clearBtn = clearHref
        ? `<a class="filter-input-clear${value ? " filter-input-clear-active" : ""}" href="${e(clearHref)}" title="Clear ${label} filter" aria-label="Clear ${label} filter">×</a>`
        : "";
    return `
<label class="filter-input-wrap">
    <span class="filter-input-label">${label}</span>
    <span class="filter-input-row">
        <input class="filter-input" name="${name}" value="${e(value ?? "")}" list="${listId}" autocomplete="off">
        ${clearBtn}
    </span>
    <datalist id="${listId}">${options}</datalist>
</label>`;
}

function renderFilterBanner(filter, availableRuns = []) {
    const banner = document.getElementById("filter-banner");
    const layoutMode = parseLayoutMode();
    const sortMode = parseSortMode();
    const sortDirection = parseSortDirection();
    const isFiltered = !!(
        filter.dandisetId ||
        filter.subject ||
        filter.session ||
        filter.pipelineVersion ||
        filter.paramsType ||
        filter.configType ||
        filter.dandiCodebaseHash ||
        filter.failureStep ||
        filter.status
    );

    const crumbs = [];
    if (filter.dandisetId) {
        crumbs.push(
            `<a class="filter-crumb" href="${narrowUrl({ dandiset: filter.dandisetId })}">Dandiset&nbsp;${e(filter.dandisetId)}</a>`
        );
    }
    if (filter.subject) {
        crumbs.push(
            `<a class="filter-crumb" href="${narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject })}">Sub:&nbsp;${e(filter.subject)}</a>`
        );
    }
    if (filter.session) {
        crumbs.push(
            `<a class="filter-crumb" href="${narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session })}">Ses:&nbsp;${e(filter.session)}</a>`
        );
    }
    if (filter.pipelineVersion) {
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ pipelineVersion: filter.pipelineVersion }))}">Ver:&nbsp;${e(filter.pipelineVersion)}</a>`
        );
    }
    if (filter.paramsType) {
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ paramsType: filter.paramsType }))}">Params:&nbsp;${e(filter.paramsType)}</a>`
        );
    }
    if (filter.configType) {
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ configType: filter.configType }))}">Config:&nbsp;${e(filter.configType)}</a>`
        );
    }
    if (filter.dandiCodebaseHash) {
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ dandiCodebaseHash: filter.dandiCodebaseHash }))}">Codebase:&nbsp;${e(filter.dandiCodebaseHash)}</a>`
        );
    }
    if (filter.failureStep) {
        const failureStepLabel =
            filter.failureStep === "exclude-job-dispatch"
                ? "Failed (except dispatch)"
                : filter.failureStep === "pre-processing"
                  ? "Failed in pre-processing"
                  : filter.failureStep === "post-processing"
                    ? "Failed in post-processing"
                    : `Failed in ${filter.failureStep}`;
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ failureStep: filter.failureStep }))}">${e(failureStepLabel)}</a>`
        );
    }
    if (filter.status) {
        const statusLabel = normalizeStatus(filter.status);
        const summaryStatusLabel = STATUS_LABELS[statusLabel] ?? `Status: ${filter.status}`;
        crumbs.push(
            `<a class="filter-crumb" href="${e(narrowUrl({ status: filter.status }))}">${e(summaryStatusLabel)}</a>`
        );
    }

    const runsMatchingDandiset = filter.dandisetId
        ? availableRuns.filter((run) => run.dandisetId === filter.dandisetId)
        : availableRuns;
    const runsMatchingSubject = filter.subject
        ? runsMatchingDandiset.filter((run) => run.subject === filter.subject)
        : runsMatchingDandiset;
    const dandisets = uniqueSortedValues(availableRuns.map((r) => r.dandisetId));
    const subjects = uniqueSortedValues(runsMatchingDandiset.map((r) => r.subject));
    const sessions = uniqueSortedValues(runsMatchingSubject.map((r) => r.session));
    const versions = uniqueSortedValues(availableRuns.map((r) => r.pipelineVersion));
    const paramsTypes = uniqueSortedValues(availableRuns.map(runParamsType));
    const configTypes = uniqueSortedValues(availableRuns.map(runConfigType));
    const dandiCodebaseHashes = uniqueSortedValues(availableRuns.map(runDandiCodebaseHash));
    const failureSteps = uniqueSortedValues([
        ...FAILURE_STEP_FILTER_OPTIONS,
        ...availableRuns
            .filter((run) => isFailedStatus(run.status))
            .map((run) => run.failureStep)
            .filter((step) => step !== "other"),
    ]);
    const filteredViewHtml = isFiltered
        ? `<div class="filter-banner-active">
    <span class="filter-banner-label">Filtered view:</span>
    <span class="filter-crumbs">${crumbs.join('<span class="filter-sep">/</span>')}</span>
</div>`
        : "";

    const testsPageHtml =
        _viewMode === "tests"
            ? `<div class="tests-page-banner">
    <span class="tests-page-label">🧪 Tests</span>
    <span class="tests-page-desc">Showing internal test runs only (Dandiset 001849)</span>
    <a class="tests-back-link" href="./">← Back to main</a>
</div>`
            : "";

    const viewHiddenInput = _viewMode === "tests" ? `<input type="hidden" name="view" value="tests">` : "";
    const layoutHiddenInput = `<input type="hidden" name="layout" value="${layoutMode}">`;
    const sortHiddenInput = `<input type="hidden" name="sort" value="${sortMode}">`;
    const sortDirectionHiddenInput = `<input type="hidden" name="sortDir" value="${sortDirection}">`;
    const statusHiddenInput = filter.status ? `<input type="hidden" name="status" value="${e(filter.status)}">` : "";
    const clearAllParams = new URLSearchParams();
    clearAllParams.set("layout", layoutMode);
    clearAllParams.set("sort", sortMode);
    clearAllParams.set("sortDir", sortDirection);
    if (_viewMode === "tests") clearAllParams.set("view", "tests");
    const clearAllHref = `?${clearAllParams.toString()}`;

    banner.innerHTML = `
${testsPageHtml}<div class="filter-banner-main">
    <span class="filter-banner-label">Filter runs:</span>
    <form class="filter-form" method="get" action="">
        ${viewHiddenInput}
        ${layoutHiddenInput}
        ${sortHiddenInput}
        ${sortDirectionHiddenInput}
        ${statusHiddenInput}
        ${renderFilterInput("dandiset", "Dandiset", filter.dandisetId, dandisets, narrowUrl({ pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("subject", "Subject", filter.subject, subjects, narrowUrl({ dandiset: filter.dandisetId, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("session", "Session", filter.session, sessions, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("version", "Version", filter.pipelineVersion, versions, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("params", "Params Type", filter.paramsType, paramsTypes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("config", "Config Type", filter.configType, configTypes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("codebaseHash", "DANDI Codebase Hash", filter.dandiCodebaseHash, dandiCodebaseHashes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, failureStep: filter.failureStep, status: filter.status }))}
        ${renderFilterInput("failureStep", "Failure Step", filter.failureStep, failureSteps, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, status: filter.status }))}
        <button class="filter-apply" type="submit">Apply</button>
        <a class="filter-clear" href="${clearAllHref}">× View all runs</a>
    </form>
</div>
${filteredViewHtml}`;

    const form = banner.querySelector(".filter-form");
    const dandisetInput = form?.querySelector('input[name="dandiset"]');
    const subjectInput = form?.querySelector('input[name="subject"]');
    const sessionInput = form?.querySelector('input[name="session"]');
    const subjectDatalist = banner.querySelector("#filter-options-subject");
    const sessionDatalist = banner.querySelector("#filter-options-session");
    const renderDatalistOptions = (datalist, values) => {
        if (!datalist) return;
        datalist.innerHTML = values.map((item) => `<option value="${e(item)}"></option>`).join("");
    };
    const refreshDependentFilterOptions = () => {
        const selectedDandiset = dandisetInput?.value ?? "";
        const nextRunsMatchingDandiset = selectedDandiset
            ? availableRuns.filter((run) => run.dandisetId === selectedDandiset)
            : availableRuns;
        const nextSubjects = uniqueSortedValues(nextRunsMatchingDandiset.map((run) => run.subject));
        renderDatalistOptions(subjectDatalist, nextSubjects);
        if (subjectInput && subjectInput.value && !nextSubjects.includes(subjectInput.value)) {
            subjectInput.value = "";
        }

        const selectedSubject = subjectInput?.value ?? "";
        const nextRunsMatchingSubject = selectedSubject
            ? nextRunsMatchingDandiset.filter((run) => run.subject === selectedSubject)
            : nextRunsMatchingDandiset;
        const nextSessions = uniqueSortedValues(nextRunsMatchingSubject.map((run) => run.session));
        renderDatalistOptions(sessionDatalist, nextSessions);
        if (sessionInput && sessionInput.value && !nextSessions.includes(sessionInput.value)) {
            sessionInput.value = "";
        }
    };
    if (dandisetInput) {
        dandisetInput.addEventListener("input", refreshDependentFilterOptions);
    }
    if (subjectInput) {
        subjectInput.addEventListener("input", refreshDependentFilterOptions);
    }

    banner.style.display = "";
}

function setPageCopy(title, subtitleHtml) {
    const titleEl = document.querySelector("h1");
    const subtitleEl = document.querySelector(".page-subtitle");
    if (titleEl) titleEl.textContent = title;
    if (subtitleEl) subtitleEl.innerHTML = subtitleHtml;
}

/* ─── Theme toggle ──────────────────────────────────────────── */
function initTheme() {
    const btn = document.getElementById("theme_toggle_btn");
    const stored = localStorage.getItem("theme");
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    const isDark = stored ? stored === "dark" : prefersDark;
    applyTheme(isDark ? "dark" : "light", btn);
    btn.addEventListener("click", () => {
        const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
        applyTheme(next, btn);
        localStorage.setItem("theme", next);
    });
}

function applyTheme(theme, btn) {
    if (theme === "light") {
        document.documentElement.dataset.theme = "light";
        btn.setAttribute("aria-label", "Switch to dark mode");
        btn.title = "Switch to dark mode";
        btn.innerHTML = SUN_ICON;
    } else {
        delete document.documentElement.dataset.theme;
        btn.setAttribute("aria-label", "Switch to light mode");
        btn.title = "Switch to light mode";
        btn.innerHTML = MOON_ICON;
    }
}

const MOON_ICON = `<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z"/></svg>`;
const SUN_ICON = `<svg viewBox="0 0 20 20" aria-hidden="true"><circle cx="10" cy="10" r="3"/><path d="M10 1v2M10 17v2M1 10h2M17 10h2M3.22 3.22l1.42 1.42M15.36 15.36l1.42 1.42M3.22 16.78l1.42-1.42M15.36 4.64l1.42-1.42" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round"/></svg>`;

/* ─── ETag-aware fetch cache ────────────────────────────────── */
// Caches response bodies in sessionStorage keyed by URL together with the
// server's ETag.  Subsequent requests send "If-None-Match" so the server can
// respond with 304 Not Modified, avoiding a redundant download.  Storage
// errors (quota exceeded, private-browsing restrictions) are silently ignored
// so that a failed cache write never prevents the fetch from succeeding.
const ETAG_CACHE_PREFIX = "aind_etag:";

async function cachedFetch(url, init = {}) {
    const cacheKey = ETAG_CACHE_PREFIX + url;
    let cached = null;
    try {
        const raw = sessionStorage.getItem(cacheKey);
        if (raw) cached = JSON.parse(raw);
    } catch {
        /* sessionStorage unavailable or parse error; proceed without cache */
    }

    const headers = new Headers(init.headers ?? {});
    if (cached?.etag) {
        headers.set("If-None-Match", cached.etag);
    }

    const resp = await fetch(url, { ...init, headers });

    if (resp.status === 304 && cached) {
        return new Response(cached.body, {
            status: cached.status ?? 200,
            headers: { "Content-Type": cached.contentType ?? "" },
        });
    }

    if (!resp.ok) return resp;

    const body = await resp.text();
    const etag = resp.headers.get("ETag");
    const contentType = resp.headers.get("Content-Type") ?? "";

    if (etag) {
        try {
            sessionStorage.setItem(cacheKey, JSON.stringify({ etag, body, contentType, status: resp.status }));
        } catch {
            /* Ignore storage errors (e.g., quota exceeded) */
        }
    }

    return new Response(body, { status: resp.status, headers: { "Content-Type": contentType } });
}

function normalizeRegistryEntries(registryEntries) {
    if (!registryEntries || typeof registryEntries !== "object" || Array.isArray(registryEntries)) return [];
    return Object.entries(registryEntries).flatMap(([alias, entry]) => {
        if (typeof alias !== "string" || !alias.trim()) return [];
        if (!entry || typeof entry !== "object" || Array.isArray(entry)) return [];
        if (typeof entry.path !== "string" || !entry.path.trim()) return [];
        if (typeof entry.md5 !== "string" || !entry.md5.trim()) return [];
        return [
            {
                alias,
                md5: entry.md5.toLowerCase(),
                path: entry.path,
                priority: alias === "default" ? 0 : REGISTRY_FALLBACK_ALIAS_PRIORITY,
            },
        ];
    });
}

async function fetchRegistryEntries(path, kindLabel) {
    const response = await cachedFetch(codeRepoRawUrl(path));
    if (!response.ok) {
        throw new Error(`Failed to load ${kindLabel} registry (HTTP ${response.status}).`);
    }
    const entries = normalizeRegistryEntries(await response.json());
    if (entries.length === 0) {
        throw new Error(`Loaded ${kindLabel} registry is empty or invalid.`);
    }
    return entries;
}

async function loadAindPipelineRegistries() {
    const [paramsResult, configResult] = await Promise.allSettled([
        fetchRegistryEntries(REGISTERED_PARAMS_PATH, "params"),
        fetchRegistryEntries(REGISTERED_CONFIGS_PATH, "config"),
    ]);
    if (paramsResult.status === "fulfilled") {
        PARAMS_REGISTRY = paramsResult.value;
    } else {
        console.warn(paramsResult.reason?.message ?? "Failed to load params registry.");
    }
    if (configResult.status === "fulfilled") {
        CONFIG_REGISTRY = configResult.value;
    } else {
        console.warn(configResult.reason?.message ?? "Failed to load config registry.");
    }
    return { paramsRegistry: PARAMS_REGISTRY, configRegistry: CONFIG_REGISTRY };
}

/* ─── Data fetching ─────────────────────────────────────────── */
async function fetchQueueState() {
    const url = `${QUEUE_CDN_BASE}/state.jsonl.gz`;
    const cacheKey = ETAG_CACHE_PREFIX + url;

    let cached = null;
    try {
        const raw = sessionStorage.getItem(cacheKey);
        if (raw) cached = JSON.parse(raw);
    } catch {
        /* sessionStorage unavailable or parse error; proceed without cache */
    }

    const headers = new Headers();
    if (cached?.etag) {
        headers.set("If-None-Match", cached.etag);
    }

    const resp = await fetch(url, { headers });

    let text;
    if (resp.status === 304 && cached) {
        text = cached.body;
    } else if (resp.ok) {
        if (typeof DecompressionStream === "undefined") {
            throw new Error(
                "Your browser does not support DecompressionStream. Please upgrade to a modern browser (Chrome 80+, Firefox 113+, Safari 16.4+, or Edge 80+)."
            );
        }
        const ds = new DecompressionStream("gzip");
        const decompressed = resp.body.pipeThrough(ds);
        text = await new Response(decompressed).text();

        const etag = resp.headers.get("ETag");
        if (etag) {
            try {
                sessionStorage.setItem(cacheKey, JSON.stringify({ etag, body: text }));
            } catch {
                /* Ignore storage errors (e.g., quota exceeded) */
            }
        }
    } else {
        if (resp.status === 403 || resp.status === 429) {
            throw new Error("GitHub CDN rate limit exceeded. Please try again in a few minutes.");
        }
        throw new Error(`Failed to load queue state (HTTP ${resp.status}).`);
    }

    return text
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line));
}

function clearQueueStateCache() {
    const url = `${QUEUE_CDN_BASE}/state.jsonl.gz`;
    const cacheKey = ETAG_CACHE_PREFIX + url;
    try {
        sessionStorage.removeItem(cacheKey);
    } catch {
        /* sessionStorage unavailable; nothing to clear */
    }
}

async function fetchTraceText(runPath) {
    const pathParts = runPath.split("/").map(encodeURIComponent).join("/");
    const url = `${CDN_BASE}/${pathParts}/logs/trace.txt`;
    try {
        const resp = await cachedFetch(url);
        if (!resp.ok) return null;
        return resp.text();
    } catch {
        return null;
    }
}

async function fetchSlurmLogs(runPath) {
    try {
        const encodedPath = `${runPath}/logs`.split("/").map(encodeURIComponent).join("/");
        const url = `${GITHUB_API_BASE}/contents/${encodedPath}?ref=${BRANCH}`;
        const resp = await cachedFetch(url);
        if (!resp.ok) return [];
        const items = await resp.json();
        if (!Array.isArray(items)) return [];
        return items
            .filter((item) => item.type === "file" && item.name.endsWith("_slurm.log"))
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((item) => item.name);
    } catch {
        return [];
    }
}

// Fetch SLURM log filenames for all runs in a single recursive git-trees call.
// Returns a Map<runPath, sortedFileNames[]>. Falls back to an empty Map on failure.
async function fetchAllSlurmLogs() {
    try {
        const url = `${GITHUB_API_BASE}/git/trees/${BRANCH}?recursive=1`;
        const resp = await cachedFetch(url);
        if (!resp.ok) return new Map();
        const data = await resp.json();
        const byRunPath = new Map();
        for (const item of data.tree ?? []) {
            if (item.type !== "blob") continue;
            const m = item.path.match(/^(derivatives\/.+)\/logs\/(.+_slurm\.log)$/);
            if (!m) continue;
            const runPath = m[1];
            const fname = m[2];
            if (!byRunPath.has(runPath)) byRunPath.set(runPath, []);
            byRunPath.get(runPath).push(fname);
        }
        for (const files of byRunPath.values()) files.sort();
        return byRunPath;
    } catch {
        return new Map();
    }
}

async function fetchDatasetDescription(runPath) {
    const pathParts = runPath.split("/").map(encodeURIComponent).join("/");
    const url = `${CDN_BASE}/${pathParts}/dataset_description.json`;
    try {
        const resp = await cachedFetch(url);
        if (!resp.ok) return null;
        return resp.json();
    } catch {
        return null;
    }
}

async function fetchDandiAssetId(dandisetId, subject, session) {
    // Asset lookup requires a session path; return null when session is absent
    if (!session) return null;
    const apiBase = dandiApiBaseUrl(dandisetId);
    async function queryPath(assetPath) {
        const url = `${apiBase}/api/dandisets/${dandisetId}/versions/draft/assets/?path=${encodeURIComponent(assetPath)}&page_size=1`;
        const resp = await fetch(url);
        if (!resp.ok) return null;
        const data = await resp.json();
        return data.results && data.results.length > 0 ? data.results[0].asset_id : null;
    }
    try {
        const nwbName = `sub-${subject}_ses-${session}.nwb`;
        const assetId = await queryPath(`sub-${subject}/${nwbName}`);
        if (assetId) return { assetId, inSourcedata: false };
        const sourcedataAssetId = await queryPath(`sourcedata/sub-${subject}/${nwbName}`);
        if (sourcedataAssetId) return { assetId: sourcedataAssetId, inSourcedata: true };
        return null;
    } catch {
        return null;
    }
}

async function fetchVisualizationData(runPath) {
    try {
        const candidatePaths = [`${runPath}/derivatives/visualization`, `${runPath}/visualization`];
        let resolvedVizPath = null;
        let vizDirItems = null;
        for (const candidatePath of candidatePaths) {
            const encodedPath = candidatePath.split("/").map(encodeURIComponent).join("/");
            const vizDirUrl = `${GITHUB_API_BASE}/contents/${encodedPath}?ref=${BRANCH}`;
            const vizDirResp = await cachedFetch(vizDirUrl);
            if (!vizDirResp.ok) continue;
            const items = await vizDirResp.json();
            if (!items || !Array.isArray(items)) continue;
            resolvedVizPath = candidatePath;
            vizDirItems = items;
            break;
        }
        if (!resolvedVizPath || !vizDirItems) return null;

        const recordingDirs = vizDirItems.filter((item) => item.type === "dir");
        if (recordingDirs.length === 0) return null;

        const recordings = await Promise.all(
            recordingDirs.map(async (dir) => {
                const treeUrl = `${GITHUB_API_BASE}/git/trees/${dir.sha}`;
                const treeResp = await cachedFetch(treeUrl);
                if (!treeResp.ok) return null;
                const treeData = await treeResp.json();
                const images = (treeData.tree ?? [])
                    .filter((f) => f.type === "blob" && /\.png$/i.test(f.path))
                    .sort((a, b) => a.path.localeCompare(b.path))
                    .map((f) => ({
                        name: f.path,
                        url: cdnUrl(`${resolvedVizPath}/${dir.name}/${f.path}`),
                    }));
                return { name: dir.name, images };
            })
        );

        const validRecordings = recordings.filter(Boolean).filter((r) => r.images.length > 0);
        return validRecordings.length > 0 ? validRecordings : null;
    } catch {
        return null;
    }
}

/* ─── Path helpers ──────────────────────────────────────────── */
// Returns subject, falling back to a per-dandiset default when subject is null.
function resolveSubject(dandisetId, subject) {
    if (subject != null) return subject;
    return DANDISET_SUBJECT_DEFAULTS.get(String(dandisetId)) ?? null;
}

function parseDandiPath(dandiPath) {
    const pathParts = String(dandiPath ?? "")
        .split("/")
        .filter(Boolean);
    const subjectIndex = pathParts.findIndex((part) => part.startsWith("sub-"));
    const subjectPart = subjectIndex >= 0 ? pathParts[subjectIndex] : "";
    const searchAfterSubject = subjectIndex >= 0 ? pathParts.slice(subjectIndex + 1) : pathParts;

    // Prefer an explicit ses-{session} directory component...
    const sessionPart = searchAfterSubject.find((part) => part.startsWith("ses-")) ?? "";
    let session = sessionPart ? sessionPart.replace(/^ses-/, "") : null;

    // ...fall back to extracting session from the NWB filename (sub-{sub}_ses-{ses}_{rest}.nwb)
    if (!session) {
        const filename = pathParts[pathParts.length - 1] ?? "";
        const sesMatch = filename.match(/_ses-([^_]+)/);
        if (sesMatch) session = sesMatch[1];
    }

    return {
        subject: subjectPart ? subjectPart.replace(/^sub-/, "") : null,
        session,
    };
}

function dandiPathDirectoryParts(dandiPath) {
    const pathParts = String(dandiPath ?? "")
        .split("/")
        .filter(Boolean);
    if (pathParts.length === 0) return [];
    const terminalPart = pathParts[pathParts.length - 1] ?? "";
    return terminalPart.toLowerCase().endsWith(".nwb") ? pathParts.slice(0, -1) : pathParts;
}

// Build a run directory path from a JSONL queue entry.
// With session:    derivatives/dandiset-{id}/sub-{subject}/ses-{session}/pipeline-{pipeline}/version-{version}_params-{params}_config-{config}[_date-{date}]_attempt-{attempt}
// Without session: derivatives/dandiset-{id}/sub-{subject}/pipeline-{pipeline}/version-{version}_params-{params}_config-{config}[_date-{date}]_attempt-{attempt}
function buildRunPath(entry) {
    const parsed = parseDandiPath(entry.dandi_path);
    const subject = resolveSubject(entry.dandiset_id, entry.subject ?? parsed.subject);
    const session = entry.session ?? parsed.session;
    const dandiPathParts = dandiPathDirectoryParts(entry.dandi_path);
    const parts = ["derivatives", `dandiset-${entry.dandiset_id}`];
    if (dandiPathParts.length > 0) {
        parts.push(...dandiPathParts);
    } else {
        parts.push(`sub-${subject}`);
        if (session !== null && session !== undefined) {
            parts.push(`ses-${session}`);
        }
    }
    parts.push(`pipeline-${entry.pipeline}`);
    let capsule = `version-${entry.version}_params-${entry.params}_config-${entry.config}`;
    if (entry.date) {
        capsule += `_date-${entry.date}`;
    }
    capsule += `_attempt-${entry.attempt}`;
    parts.push(capsule);
    return parts.join("/");
}

/* ─── Queue entry parsing ───────────────────────────────────── */
// Convert raw JSONL entries from the queue state file into run objects.
function parseQueueEntries(entries) {
    return entries.map((entry) => {
        const parsed = parseDandiPath(entry.dandi_path);
        const createdAt = entry.created_at ?? null;
        return {
            path: buildRunPath(entry),
            dandisetId: entry.dandiset_id,
            dandiPath: entry.dandi_path ?? null,
            subject: resolveSubject(entry.dandiset_id, entry.subject ?? parsed.subject),
            session: entry.session ?? parsed.session,
            pipelineName: entry.pipeline,
            pipelineVersion: entry.version,
            paramsProfile: entry.params,
            configHash: normalizeConfigHash(entry.config),
            attempt: entry.attempt,
            hasCode: entry.has_code,
            hasOutput: entry.has_output,
            hasLogs: entry.has_logs,
            contentHash: entry.content_id ?? null,
            assetSizeBytes: normalizeByteCount(entry.asset_size_bytes ?? entry.asset_bytes ?? entry.bytes),
            createdAt,
            runDate: createdAt ?? entry.date ?? null,
        };
    });
}

// Strip _date-{...} suffix from a raw config field value so that the short
// commit hash can be resolved against the registry independently of whether
// the date was embedded in the same token (e.g. "0d4bf36_date-2026+05+21" → "0d4bf36").
function normalizeConfigHash(config) {
    if (!config) return config;
    const dateIndex = String(config).indexOf("_date-");
    return dateIndex !== -1 ? config.slice(0, dateIndex) : config;
}

/* ─── Path parsing ──────────────────────────────────────────── */
// Run paths are one of:
// - derivatives/{dandiset}/{subject}/{session?}/{pipeline}/version-{version}/{runId}
// - derivatives/{dandiset}/{subject}/{session?}/{pipeline}/version-{version}_{runId}
function parseRunPath(runPath) {
    const parts = runPath.split("/");

    const dandisetPart = parts.find((part) => part.startsWith("dandiset-")) ?? "";
    const subjectPart = parts.find((part) => part.startsWith("sub-")) ?? "";
    const pipelineIndex = parts.findIndex((part) => part.startsWith("pipeline-"));

    const dandisetId = dandisetPart.replace(/^dandiset-/, "");
    const subject = subjectPart.replace(/^sub-/, "");

    const sessionPart = pipelineIndex > 0 ? parts[pipelineIndex - 1] : "";
    const sesMatch = sessionPart?.match(/^ses-(.+)$/);
    const session = sesMatch ? sesMatch[1] : null;

    const pipelineName = pipelineIndex >= 0 ? parts[pipelineIndex].replace(/^pipeline-/, "") : "";
    const versionPart = pipelineIndex >= 0 ? (parts[pipelineIndex + 1] ?? "") : "";
    const runPart = pipelineIndex >= 0 ? (parts[pipelineIndex + 2] ?? "") : "";

    let pipelineVersion = versionPart.startsWith("version-") ? versionPart.slice("version-".length) : versionPart;
    let capsulePart = runPart;
    if (versionPart.startsWith("version-")) {
        const versionBody = versionPart.slice("version-".length);
        const flattenedMarker = "_params-";
        const flattenedIndex = versionBody.indexOf(flattenedMarker);
        if (flattenedIndex !== -1) {
            pipelineVersion = versionBody.slice(0, flattenedIndex);
            capsulePart = `params-${versionBody.slice(flattenedIndex + flattenedMarker.length)}`;
        }
    }

    let paramsProfile = capsulePart;
    let configHash = "";
    let attempt = 1;
    if (capsulePart.startsWith("params-")) {
        const capsuleBody = capsulePart.slice("params-".length);
        const attemptMarker = "_attempt-";
        const attemptIndex = capsuleBody.lastIndexOf(attemptMarker);
        if (attemptIndex !== -1) {
            const attemptText = capsuleBody.slice(attemptIndex + attemptMarker.length);
            if (/^\d+$/.test(attemptText)) {
                attempt = parseInt(attemptText, 10);
                const beforeAttempt = capsuleBody.slice(0, attemptIndex);
                const configMarker = "_config-";
                const configIndex = beforeAttempt.indexOf(configMarker);
                if (configIndex !== -1) {
                    paramsProfile = beforeAttempt.slice(0, configIndex);
                    configHash = normalizeConfigHash(beforeAttempt.slice(configIndex + configMarker.length));
                } else {
                    paramsProfile = beforeAttempt;
                }
            }
        }
    }

    return {
        path: runPath,
        dandisetId,
        subject,
        session,
        pipelineName,
        pipelineVersion,
        paramsProfile,
        configHash,
        createdAt: null,
        runDate: null,
        attempt,
    };
}

function sortRuns(runs, sortMode = _sortMode, sortDirection = _sortDirection) {
    return [...runs].sort((a, b) => {
        if (sortMode === "created_at") {
            const createdCompare = String(b.createdAt ?? "").localeCompare(String(a.createdAt ?? ""));
            if (createdCompare !== 0) return sortDirection === "asc" ? -createdCompare : createdCompare;
        }
        const attemptCompare = (b.attempt ?? 0) - (a.attempt ?? 0);
        if (attemptCompare !== 0) return sortDirection === "asc" ? -attemptCompare : attemptCompare;
        const pathCompare = String(a.path ?? "").localeCompare(String(b.path ?? ""));
        return sortDirection === "asc" ? pathCompare : -pathCompare;
    });
}

/* ─── Trace parsing ─────────────────────────────────────────── */
function parseTrace(text) {
    if (!text) return { status: "unknown", tasks: [] };
    const lines = text.trim().split("\n").filter(Boolean);
    if (lines.length < 2) return { status: "unknown", tasks: [] };

    const headers = lines[0].split("\t");
    const idx = (h) => headers.indexOf(h);

    const tasks = lines.slice(1).map((line) => {
        const cols = line.split("\t");
        return {
            name: cols[idx("name")] ?? "",
            status: cols[idx("status")] ?? "",
            exit: cols[idx("exit")] ?? "",
            duration: cols[idx("duration")] ?? "",
            realtime: cols[idx("realtime")] ?? "",
            nativeId: cols[idx("native_id")] ?? "",
        };
    });

    const anyFailed = tasks.some((t) => t.status === "FAILED");
    const allCompleted = tasks.every((t) => t.status === "COMPLETED");
    const status = anyFailed ? "failed" : allCompleted ? "success" : "partial";
    return { status, tasks };
}

/* ─── Rendering ─────────────────────────────────────────────── */
function renderSummary(runs) {
    const total = runs.length;
    const success = runs.filter((r) => r.status === "success").length;
    const failed = runs.filter((r) => r.status === "failed").length;
    const queued = runs.filter((r) => r.status === "queued").length;
    const partial = runs.filter((r) => r.status === "partial").length;
    const unknown = total - success - failed - queued - partial;
    const successfulRuns = runs.filter((run) => run.status === "success");
    const runsWithKnownByteCounts = successfulRuns.filter((run) => runByteCount(run) !== null).length;
    const totalBytes = sumRunByteCounts(successfulRuns);
    const filter = parseFilter();
    const successHref = narrowUrl({
        dandiset: filter.dandisetId,
        subject: filter.subject,
        session: filter.session,
        pipelineVersion: filter.pipelineVersion,
        paramsType: filter.paramsType,
        configType: filter.configType,
        dandiCodebaseHash: filter.dandiCodebaseHash,
        status: "success",
    });
    const failedHref = narrowUrl({
        dandiset: filter.dandisetId,
        subject: filter.subject,
        session: filter.session,
        pipelineVersion: filter.pipelineVersion,
        paramsType: filter.paramsType,
        configType: filter.configType,
        dandiCodebaseHash: filter.dandiCodebaseHash,
        failureStep: filter.failureStep,
        status: "failed",
    });

    document.getElementById("summary").innerHTML = `
        <div class="summary-stats">
            <div class="stat-item">
                <span class="stat-value">${total}</span>
                <span class="stat-label">Total Runs</span>
            </div>
            <a class="stat-item stat-success" href="${e(successHref)}" title="Show only successful runs">
                <span class="stat-value">${success}</span>
                <span class="stat-label">Successful</span>
            </a>
            <a class="stat-item stat-failed" href="${e(failedHref)}" title="Show only failed runs">
                <span class="stat-value">${failed}</span>
                <span class="stat-label">Failed</span>
            </a>
            ${
                queued
                    ? `<div class="stat-item stat-queued">
                <span class="stat-value">${queued}</span>
                <span class="stat-label">Queued</span>
            </div>`
                    : ""
            }
            ${
                partial
                    ? `<div class="stat-item stat-partial">
                <span class="stat-value">${partial}</span>
                <span class="stat-label">Partial</span>
            </div>`
                    : ""
            }
            ${
                unknown
                    ? `<div class="stat-item">
                <span class="stat-value">${unknown}</span>
                <span class="stat-label">Unknown</span>
            </div>`
                    : ""
            }
            ${
                runsWithKnownByteCounts
                    ? `<div class="stat-item stat-bytes">
               <span class="stat-value">${formatByteCount(totalBytes)}</span>
               <span class="stat-label">DATA PROCESSED</span>
           </div>`
                    : ""
            }
        </div>`;
}

/* Log files rendered as always-open inline iframes (not modal buttons) */
const INLINE_REPORT_FILES = new Set(["report.html", "timeline.html"]);
const INLINE_REPORT_ORDER = ["timeline.html", "report.html"];

/* Standard log files present whenever has_logs is true (Nextflow output) */
const STANDARD_LOG_FILES = ["dag.html", "nextflow.log", "report.html", "timeline.html", "trace.txt"];

/* Pretty-print a log file name */
const LOG_LABELS = {
    "dag.html": "Pipeline DAG",
    "nextflow.log": "Nextflow Log",
    "report.html": "Execution Report",
    "timeline.html": "Execution Timeline",
    "trace.txt": "Task Trace",
};
function logLabel(fileName) {
    if (LOG_LABELS[fileName]) return LOG_LABELS[fileName];
    if (fileName.includes("_slurm.log")) return "SLURM Job Log";
    return fileName;
}

/* Build a raw CDN URL for a repo file path */
function cdnUrl(filePath) {
    return `${CDN_BASE}/${filePath.split("/").map(encodeURIComponent).join("/")}`;
}

/* Build a GitHub tree URL for a repo directory path */
function treeUrl(filePath) {
    return `https://github.com/${OWNER}/${REPO}/tree/${BRANCH}/${filePath.split("/").map(encodeURIComponent).join("/")}`;
}

/* Build a Neurosift URL for a DANDI asset (legacy: via DANDI API asset download URL) */
function neurosiftUrl(dandisetId, assetId) {
    const assetDownloadUrl = `${dandiApiBaseUrl(dandisetId)}/api/assets/${assetId}/download/`;
    return `https://neurosift.app/nwb?url=${encodeURIComponent(assetDownloadUrl)}&dandisetId=${encodeURIComponent(dandisetId)}&dandisetVersion=draft`;
}

/* Build a Neurosift NWB URL from a DANDI S3 content hash */
function neurosiftBlobUrl(contentHash) {
    const blobFileUrl = `https://dandiarchive.s3.amazonaws.com/blobs/${contentHash.slice(0, 3)}/${contentHash.slice(3, 6)}/${contentHash}`;
    const neurosiftUrl_ = `https://neurosift.app/nwb?url=${encodeURIComponent(blobFileUrl)}`;
    return neurosiftUrl_;
}

/* Build the best available Neurosift NWB URL: prefer blob URL (no API call), fall back to asset download URL */
function neurosiftSessionUrl(dandisetId, contentHash, assetId) {
    if (contentHash) {
        return neurosiftBlobUrl(contentHash);
    }
    if (assetId) {
        const url = neurosiftUrl(dandisetId, assetId);
        return url;
    }
    return null;
}

/* Build a Neurosift dandiset URL */
function neurosiftDandisetUrl(dandisetId) {
    return `https://neurosift.app/dandiset/${encodeURIComponent(dandisetId)}`;
}

function renderRunEntry(run) {
    const sc =
        run.status === "success"
            ? "status-success"
            : run.status === "failed"
              ? "status-failed"
              : run.status === "queued"
                ? "status-queued"
                : run.status === "partial"
                  ? "status-partial"
                  : "status-unknown";
    const slbl =
        run.status === "success"
            ? "✓ Success"
            : run.status === "failed"
              ? "✗ Failed"
              : run.status === "queued"
                ? "⧗ Queued"
                : run.status === "partial"
                  ? "⚠ Partial"
                  : "? Unknown";

    // Log files known to be present when has_logs is true (standard Nextflow output).
    const logFiles = run.hasLogs ? STANDARD_LOG_FILES : [];

    const inlineLogs = logFiles
        .filter((f) => INLINE_REPORT_FILES.has(f))
        .sort((a, b) => {
            const ai = INLINE_REPORT_ORDER.indexOf(a);
            const bi = INLINE_REPORT_ORDER.indexOf(b);
            return (ai === -1 ? Infinity : ai) - (bi === -1 ? Infinity : bi);
        });
    const buttonLogs = [...logFiles.filter((f) => !INLINE_REPORT_FILES.has(f)), ...(run.slurmLogs ?? [])];
    const hasLogs = buttonLogs.length > 0;
    const hasInline = inlineLogs.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;
    const hasSourceVersions = run.generatedBy && run.generatedBy.length > 0;
    const hasViz = run.vizData && run.vizData.length > 0;
    const bytes = runByteCount(run);
    const bytesHtml =
        bytes === null
            ? ""
            : `<span class="run-sep">·</span><span class="run-bytes">Asset size:&nbsp;${formatByteCount(bytes)}</span>`;

    return `
<div class="run-entry ${sc}">
    <div class="run-entry-header">
        <span class="status-badge ${sc}">${slbl}</span>
        ${run.runDate ? `<span class="run-date">${e(run.runDate)}</span><span class="run-sep">·</span>` : ""}
        ${bytesHtml}
        <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
        <a class="run-entry-github-link" href="${e(treeUrl(run.path))}" target="_blank" rel="noopener">↗ GitHub</a>
    </div>

    ${hasSourceVersions ? renderSourceVersionsSection(run.generatedBy) : ""}
    ${hasTasks ? renderTraceSection(run.tasks) : ""}
    ${hasViz ? renderVisualizationSection(run.vizData) : ""}
    ${hasLogs ? renderLogSection(run.path, buttonLogs) : ""}
    ${hasInline ? renderReportSection(run.path, inlineLogs) : ""}
</div>`;
}

function renderPipelineInfo(pipelineName, pipelineVersion) {
    const commitHash = pipelineCompareRef(pipelineVersion);
    const hasCommit = commitHash !== pipelineVersion;

    const displayName = e(pipelineName.replace(/\+/g, "-"));

    if (hasCommit) {
        const displayVer = e(pipelineVersion.replace(/\+/g, "-"));
        const url = e(`${PIPELINE_REPO_URL}/commit/${commitHash}`);
        return (
            `<a class="pipeline-link" href="${url}" target="_blank" rel="noopener">` +
            `<span class="pipeline-name">${displayName}</span>` +
            `<span class="pipeline-version">${displayVer}</span>` +
            `</a>`
        );
    }

    const displayVer = e(pipelineVersion.replace(/\+/g, "-"));
    return `<span class="pipeline-name">${displayName}</span>` + `<span class="pipeline-version">${displayVer}</span>`;
}

function resolveCodeUrl(codeUrl, version) {
    if (!codeUrl || !version) return codeUrl ?? null;
    // If the version looks like a bare commit hash and the CodeURL doesn't already
    // point to a specific commit/tree/tag, append /commit/<hash> so the link goes
    // directly to the commit rather than the repository root.
    const isCommitHash = /^[0-9a-f]{6,40}$/i.test(version);
    const alreadySpecific = /\/(commit|tree|blob|releases\/tag)\//i.test(codeUrl);
    if (isCommitHash && !alreadySpecific) {
        return codeUrl.replace(/\/+$/, "") + "/tree/" + version;
    }
    return codeUrl;
}

function renderSourceVersionsSection(generatedBy) {
    const items = generatedBy
        .map((entry) => {
            const name = e(entry.Name ?? "");
            const version = e(entry.Version ?? "");
            const rawUrl = entry.CodeURL ?? null;
            const resolvedUrl = resolveCodeUrl(rawUrl, entry.Version ?? "");
            const codeUrl = resolvedUrl ? e(resolvedUrl) : null;
            const versionHtml = version ? `<span class="src-version">${version}</span>` : "";
            const nameHtml = codeUrl
                ? `<a class="src-link" href="${codeUrl}" target="_blank" rel="noopener">${name}</a>`
                : `<span class="src-name">${name}</span>`;
            return `<span class="src-entry">${nameHtml}${versionHtml}</span>`;
        })
        .join("");

    return `
<div class="source-versions">
    <span class="source-versions-label">Source versions:</span>
    <span class="source-versions-list">${items}</span>
</div>`;
}

function renderTraceSection(tasks) {
    const rows = tasks
        .map((t) => {
            const sc = t.status === "COMPLETED" ? "task-ok" : t.status === "FAILED" ? "task-fail" : "task-other";
            return `<tr>
            <td>${e(t.name)}</td>
            <td><span class="task-status ${sc}">${e(t.status)}</span></td>
            <td class="mono">${e(t.exit)}</td>
            <td class="mono">${e(t.duration)}</td>
            <td class="mono">${e(t.realtime)}</td>
        </tr>`;
        })
        .join("");

    return `
<details class="run-section">
    <summary class="run-section-title">
        Pipeline Steps
        <span class="count-badge">${tasks.length}</span>
    </summary>
    <div class="trace-table-wrap">
        <table class="trace-table">
            <thead><tr>
                <th>Step</th><th>Status</th><th>Exit</th><th>Wall time</th><th>CPU time</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
    </div>
</details>`;
}

function renderLogSection(runPath, logFiles) {
    const buttons = logFiles
        .map((fname) => {
            const filePath = `${runPath}/logs/${fname}`;
            const isHtml = fname.endsWith(".html");
            const label = logLabel(fname);
            const href = cdnUrl(filePath);
            return `<button class="log-link"
            data-log-path="${e(filePath)}"
            data-log-label="${e(label)}"
            data-log-html="${isHtml}"
            data-log-external="${e(href)}">${e(label)}</button>`;
        })
        .join("");

    return `
<details class="run-section">
    <summary class="run-section-title">
        Logs
        <span class="count-badge">${logFiles.length}</span>
    </summary>
    <div class="log-links">${buttons}</div>
</details>`;
}

function renderReportSection(runPath, reportFiles) {
    const frames = reportFiles
        .map((fname) => {
            const filePath = `${runPath}/logs/${fname}`;
            const label = logLabel(fname);
            return `<div class="inline-report-wrap">
            <div class="inline-report-header">
                <span class="inline-report-label">${e(label)}</span>
            </div>
            <iframe class="inline-report-iframe"
                data-srcdoc-path="${e(filePath)}"
                sandbox="allow-scripts"
                title="${e(label)}"></iframe>
        </div>`;
        })
        .join("");

    return `
<details class="run-section">
    <summary class="run-section-title">
        Reports
        <span class="count-badge">${reportFiles.length}</span>
    </summary>
    <div class="inline-reports">${frames}</div>
</details>`;
}

function renderVisualizationSection(recordings) {
    const totalImages = recordings.reduce((sum, r) => sum + r.images.length, 0);
    const recordingHtml = recordings
        .map((rec) => {
            const imgHtml = rec.images
                .map((img) => {
                    const href = e(img.url);
                    const caption = e(img.name.replace(/\.png$/i, "").replace(/_/g, " "));
                    return `<figure class="viz-figure">
                <a class="viz-link" data-viz-url="${href}" data-viz-label="${caption}" href="${href}" rel="noopener" aria-haspopup="dialog">
                    <img class="viz-img" src="${href}" loading="lazy" alt="${e(img.name)}">
                </a>
                <figcaption>${caption}</figcaption>
            </figure>`;
                })
                .join("");
            return `<div class="viz-recording">
        <div class="viz-recording-label">${e(rec.name)}</div>
        <div class="viz-grid">${imgHtml}</div>
    </div>`;
        })
        .join("");

    return `
<details class="run-section">
    <summary class="run-section-title">
        Visualizations
        <span class="count-badge">${totalImages}</span>
    </summary>
    ${recordingHtml}
</details>`;
}

/* ─── Grouping helpers ──────────────────────────────────────── */
function groupBy(arr, keyFn) {
    const map = new Map();
    for (const item of arr) {
        const key = keyFn(item);
        if (!map.has(key)) map.set(key, []);
        map.get(key).push(item);
    }
    return map;
}

function renderGroupBadges(runs) {
    const s = runs.filter((r) => r.status === "success").length;
    const f = runs.filter((r) => r.status === "failed").length;
    const q = runs.filter((r) => r.status === "queued").length;
    const p = runs.filter((r) => r.status === "partial").length;
    const u = runs.length - s - f - q - p;
    const runsWithKnownByteCounts = runs.filter((run) => runByteCount(run) !== null).length;
    const totalBytes = sumRunByteCounts(runs);
    const parts = [];
    if (s)
        parts.push(
            `<span class="gbadge gbadge-success" title="${s} successful run${s !== 1 ? "s" : ""}">${s}&thinsp;✓</span>`
        );
    if (f)
        parts.push(
            `<span class="gbadge gbadge-failed"  title="${f} failed run${f !== 1 ? "s" : ""}">${f}&thinsp;✗</span>`
        );
    if (q)
        parts.push(
            `<span class="gbadge gbadge-queued" title="${q} queued run${q !== 1 ? "s" : ""}">${q}&thinsp;⧗</span>`
        );
    if (p)
        parts.push(
            `<span class="gbadge gbadge-partial" title="${p} partial run${p !== 1 ? "s" : ""}">${p}&thinsp;⚠</span>`
        );
    if (u)
        parts.push(
            `<span class="gbadge gbadge-unknown" title="${u} unknown run${u !== 1 ? "s" : ""}">${u}&thinsp;?</span>`
        );
    if (runsWithKnownByteCounts) {
        const totalBytesLabel = formatByteCount(totalBytes);
        parts.push(
            `<span class="gbadge gbadge-bytes" title="DATA PROCESSED: ${totalBytesLabel}">DATA PROCESSED:&nbsp;${totalBytesLabel}</span>`
        );
    }
    return parts.join("");
}

function resolveRegistryAlias(hash, registry) {
    if (!hash) return null;
    const normalizedHash = String(hash).toLowerCase();
    const exactHashMatches = [];
    const prefixHashMatches = [];
    for (const entry of registry) {
        if (entry.md5 === normalizedHash) {
            exactHashMatches.push(entry);
        } else if (entry.md5.startsWith(normalizedHash)) {
            prefixHashMatches.push(entry);
        }
    }
    const candidates = exactHashMatches.length > 0 ? exactHashMatches : prefixHashMatches;
    const FALLBACK_ALIAS_PRIORITY = 1;
    const aliasPriority = (entry) => entry.priority ?? FALLBACK_ALIAS_PRIORITY;
    candidates.sort((a, b) => aliasPriority(b) - aliasPriority(a) || a.alias.localeCompare(b.alias));
    return candidates[0] ?? null;
}

function renderRegistryLink(prefix, hash, registry, subdir) {
    if (!hash) return "";
    const match = resolveRegistryAlias(hash, registry);
    if (!match) return `${prefix}:&nbsp;${e(String(hash))}`;
    const sourceUrl = e(`${AIND_EPHYS_PIPELINE_CODE_URL}/${subdir}/${match.path}`);
    return `${prefix}:&nbsp;<a class="src-link" href="${sourceUrl}" target="_blank" rel="noopener">${e(match.alias)}</a>`;
}

function uniqueRegistryEntries(registry) {
    const entriesByHash = new Map();
    for (const entry of registry) {
        const key = `${entry.md5}\x00${entry.path}`;
        const existing = entriesByHash.get(key);
        const storedPriority = existing?.priority ?? REGISTRY_FALLBACK_ALIAS_PRIORITY;
        const candidatePriority = entry.priority ?? REGISTRY_FALLBACK_ALIAS_PRIORITY;
        if (
            !existing ||
            candidatePriority > storedPriority ||
            (candidatePriority === storedPriority && entry.alias.localeCompare(existing.alias) < 0)
        ) {
            entriesByHash.set(key, entry);
        }
    }
    return [...entriesByHash.values()].sort((a, b) => a.alias.localeCompare(b.alias));
}

function buildPairwiseComparisons(items) {
    const pairs = [];
    for (let i = 0; i < items.length; i++) {
        for (let j = i + 1; j < items.length; j++) {
            pairs.push([items[i], items[j]]);
        }
    }
    return pairs;
}

function uniquePipelineEntries(runs) {
    return [...groupBy(runs, (run) => run.pipelineVersion).values()]
        .map((versions) => versions[0])
        .sort((a, b) => FILTER_VALUE_COLLATOR.compare(a.pipelineVersion, b.pipelineVersion))
        .map((run) => ({
            key: run.pipelineVersion,
            pipelineName: run.pipelineName,
            pipelineVersion: run.pipelineVersion,
        }));
}

function pipelineCompareRef(version) {
    const versionText = String(version ?? "");
    const hashPart = pipelineCompareRefCandidates(versionText)[0];
    if (hashPart) {
        return hashPart;
    }
    return versionText;
}

function pipelineCompareRefCandidates(version) {
    return String(version ?? "")
        .split("+")
        .slice(1)
        .filter((part) => COMMIT_HASH_PATTERN.test(part));
}

const _pipelineCompareRefCache = new Map();

async function resolvePipelineCompareRef(version) {
    const versionText = String(version ?? "");
    const refs = pipelineCompareRefCandidates(versionText);
    if (refs.length === 0) {
        return versionText;
    }
    for (const ref of refs) {
        if (ref.length === FULL_COMMIT_HASH_LENGTH) {
            return ref;
        }
        if (_pipelineCompareRefCache.has(ref)) {
            const cachedResolved = await _pipelineCompareRefCache.get(ref);
            if (cachedResolved) {
                return cachedResolved;
            }
            continue;
        }
        const request = (async () => {
            try {
                const resp = await cachedFetch(`${PIPELINE_API_BASE}/commits/${encodeURIComponent(ref)}`, {
                    headers: { Accept: "application/vnd.github+json" },
                });
                if (!resp.ok) return null;
                const data = await resp.json();
                return typeof data?.sha === "string" && data.sha ? data.sha : null;
            } catch {
                return null;
            }
        })();
        _pipelineCompareRefCache.set(ref, request);
        const resolved = await request;
        if (resolved) {
            return resolved;
        }
    }
    return refs[0] ?? versionText;
}

async function buildPipelineCompareEntries(runs) {
    const uniqueVersions = uniquePipelineEntries(runs);
    const resolvedEntries = await Promise.all(
        uniqueVersions.map(async (entry) => ({
            ...entry,
            compareRef: await resolvePipelineCompareRef(entry.pipelineVersion),
        }))
    );
    return Array.from(groupBy(resolvedEntries, (entry) => entry.compareRef).values())
        .map(
            (entriesForRef) =>
                [...entriesForRef].sort(
                    (a, b) =>
                        a.pipelineVersion.split("+").length - b.pipelineVersion.split("+").length ||
                        FILTER_VALUE_COLLATOR.compare(a.pipelineVersion, b.pipelineVersion)
                )[0]
        )
        .sort((a, b) => FILTER_VALUE_COLLATOR.compare(a.pipelineVersion, b.pipelineVersion));
}

async function fetchPipelineCompareSummary(baseRef, headRef) {
    if (!baseRef || !headRef || baseRef === headRef) {
        return { kind: "same-ref" };
    }
    try {
        const resp = await cachedFetch(
            `${PIPELINE_API_BASE}/compare/${encodeURIComponent(baseRef)}...${encodeURIComponent(headRef)}`,
            {
                headers: { Accept: "application/vnd.github+json" },
            }
        );
        if (!resp.ok) {
            return { kind: "error", message: `Unable to load pipeline compare details (HTTP ${resp.status}).` };
        }
        const data = await resp.json();
        return {
            kind: "compare",
            aheadBy: Number(data?.ahead_by ?? 0),
            behindBy: Number(data?.behind_by ?? 0),
            totalCommits: Number(data?.total_commits ?? 0),
            files: Array.isArray(data?.files) ? data.files : [],
            commits: Array.isArray(data?.commits) ? data.commits : [],
        };
    } catch {
        return { kind: "error", message: "Unable to load pipeline compare details." };
    }
}

function renderNamedPairTable(
    rowLabel,
    leftColumnLabel,
    rightColumnLabel,
    leftCellHtml,
    rightCellHtml,
    leftColumnHtml = null,
    rightColumnHtml = null
) {
    return `<div class="diff-detail-table-wrap">
        <table class="diff-detail-table diff-detail-table-pair">
            <thead>
                <tr>
                    <th class="diff-detail-corner" aria-hidden="true"></th>
                    <th scope="col">${leftColumnHtml ?? e(leftColumnLabel)}</th>
                    <th scope="col">${rightColumnHtml ?? e(rightColumnLabel)}</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <th scope="row" class="diff-detail-key">${e(rowLabel)}</th>
                    <td>${leftCellHtml}</td>
                    <td>${rightCellHtml}</td>
                </tr>
            </tbody>
        </table>
    </div>`;
}

function renderTextPairTable(rowLabel, leftLabel, rightLabel) {
    const leftCellText = e(leftLabel);
    const rightCellText = e(rightLabel);
    return renderNamedPairTable(rowLabel, leftLabel, rightLabel, leftCellText, rightCellText);
}

function renderKeyValueTable(rows) {
    return `<div class="diff-detail-table-wrap">
        <table class="diff-detail-table">
            <thead>
                <tr>
                    <th scope="col">Metric</th>
                    <th scope="col">Value</th>
                </tr>
            </thead>
            <tbody>${rows
                .map(
                    (row) => `<tr>
                    <th scope="row" class="diff-detail-key">${e(row.label)}</th>
                    <td>${row.valueHtml}</td>
                </tr>`
                )
                .join("")}</tbody>
        </table>
    </div>`;
}

function renderTwoColumnTable(headers, rows) {
    return `<div class="diff-detail-table-wrap">
        <table class="diff-detail-table">
            <thead>
                <tr>
                    <th scope="col">${e(headers[0])}</th>
                    <th scope="col">${e(headers[1])}</th>
                </tr>
            </thead>
            <tbody>${rows
                .map(
                    (row) => `<tr>
                    <td>${row[0]}</td>
                    <td>${row[1]}</td>
                </tr>`
                )
                .join("")}</tbody>
        </table>
    </div>`;
}

function renderDiffInlineLink(url, label) {
    return `<a class="diff-inline-link" href="${e(url)}" target="_blank" rel="noopener">${e(label)}</a>`;
}

function renderNamedDiffTable(
    parameterLabel,
    leftLabel,
    rightLabel,
    changes,
    leftColumnHtml = null,
    rightColumnHtml = null
) {
    if (changes.length === 0) {
        return '<p class="diff-empty-state diff-empty-state-inline">No JSON differences detected.</p>';
    }
    return `<div class="diff-detail-table-wrap">
        <table class="diff-detail-table">
            <thead>
                <tr>
                    <th scope="col">${e(parameterLabel)}</th>
                    <th scope="col">${leftColumnHtml ?? e(leftLabel)}</th>
                    <th scope="col">${rightColumnHtml ?? e(rightLabel)}</th>
                </tr>
            </thead>
            <tbody>${changes
                .map(
                    (change) => `<tr>
                    <th scope="row" class="diff-detail-key diff-change-path">${e(change.path || ROOT_DIFF_PATH_LABEL)}</th>
                    <td><span class="diff-detail-chip diff-change-before">${e(renderDiffValue(change.left))}</span></td>
                    <td><span class="diff-detail-chip diff-change-after">${e(renderDiffValue(change.right))}</span></td>
                </tr>`
                )
                .join("")}</tbody>
        </table>
    </div>`;
}

function renderConfigDiffTable(leftLabel, rightLabel, changes, leftColumnHtml = null, rightColumnHtml = null) {
    if (changes.length === 0) {
        return '<p class="diff-empty-state diff-empty-state-inline">No config differences detected.</p>';
    }
    return `<div class="diff-detail-table-wrap">
        <table class="diff-detail-table">
            <thead>
                <tr>
                    <th scope="col">Config snippet</th>
                    <th scope="col">${leftColumnHtml ?? e(leftLabel)}</th>
                    <th scope="col">${rightColumnHtml ?? e(rightLabel)}</th>
                </tr>
            </thead>
            <tbody>${changes
                .map(
                    (change) => `<tr>
                    <th scope="row" class="diff-detail-key diff-change-path">${e(change.path || ROOT_DIFF_PATH_LABEL)}</th>
                    <td>${renderConfigSnippet(change.left, "before")}</td>
                    <td>${renderConfigSnippet(change.right, "after")}</td>
                </tr>`
                )
                .join("")}</tbody>
        </table>
    </div>`;
}

function renderConfigSnippet(snippetText, side) {
    const lines = (snippetText ?? "").split("\n");
    return `<code class="diff-config-snippet diff-config-snippet-${e(side)}">${lines
        .map((line) => {
            const match = line.match(/^(\s*\d+)\s*([ +-])(.*)$/);
            const lineNumber = match ? match[1] : "";
            const marker = match ? match[2] : " ";
            const content = match ? match[3].replace(/^ /, "") : line;
            const isChanged = marker === "+" || marker === "-";
            return `<span class="diff-config-line${isChanged ? " diff-config-line-changed" : ""}">
                <span class="diff-config-line-number">${e(lineNumber)}</span>
                <span class="diff-config-line-marker">${e(marker === " " ? "·" : marker)}</span>
                <span class="diff-config-line-content">${e(content)}</span>
            </span>`;
        })
        .join("")}</code>`;
}

function renderPipelineCompareBody(baseVersion, headVersion, summary) {
    if (summary?.kind === "same-ref") {
        return '<p class="diff-empty-state diff-empty-state-inline">No distinct pipeline repository commit comparison is available for this version pair.</p>';
    }
    if (summary?.kind === "error") {
        return `<p class="diff-empty-state diff-empty-state-inline">${e(summary.message)}</p>`;
    }
    const summaryRows = [
        {
            label: "Commits",
            valueHtml: e(`${summary.totalCommits} commit${summary.totalCommits !== 1 ? "s" : ""}`),
        },
        {
            label: "Files",
            valueHtml: e(`${summary.files.length} file${summary.files.length !== 1 ? "s" : ""}`),
        },
    ];
    if (summary.aheadBy) {
        summaryRows.push({
            label: "Ahead",
            valueHtml: e(`${summary.aheadBy} commit${summary.aheadBy !== 1 ? "s" : ""} ahead in comparison`),
        });
    }
    if (summary.behindBy) {
        summaryRows.push({
            label: "Behind",
            valueHtml: e(`${summary.behindBy} commit${summary.behindBy !== 1 ? "s" : ""} behind in comparison`),
        });
    }
    const summaryTable = renderKeyValueTable(summaryRows);
    const commitItems =
        summary.commits.length > 0
            ? renderTwoColumnTable(
                  ["Commit", "Message"],
                  summary.commits.map((commit) => {
                      const message = String(commit?.commit?.message ?? "").split("\n")[0] || "(no commit message)";
                      return [
                          `<span class="diff-change-path">${e(String(commit?.sha ?? "").slice(0, 7) || "commit")}</span>`,
                          e(message),
                      ];
                  })
              )
            : '<p class="diff-empty-state diff-empty-state-inline">No commit metadata returned.</p>';
    const fileItems =
        summary.files.length > 0
            ? renderTwoColumnTable(
                  ["File", "Status"],
                  summary.files.map((file) => [
                      `<span class="diff-change-path">${e(file.filename ?? "(unknown file)")}</span>`,
                      e(file.status ?? "changed"),
                  ])
              )
            : '<p class="diff-empty-state diff-empty-state-inline">No changed files were returned.</p>';
    const baseLabel = baseVersion.replace(/\+/g, "-");
    const headLabel = headVersion.replace(/\+/g, "-");
    return `<div class="diff-pair-card">
        ${renderTextPairTable("Pipeline version", baseLabel, headLabel)}
        ${summaryTable}
        ${commitItems}
        ${fileItems}
    </div>`;
}

async function buildPipelineDiffPairs(runs) {
    const compareEntries = await buildPipelineCompareEntries(runs);
    return Promise.all(
        buildPairwiseComparisons(compareEntries).map(async ([base, head]) => {
            const compareUrl = `${PIPELINE_REPO_URL}/compare/${encodeURIComponent(base.compareRef)}...${encodeURIComponent(head.compareRef)}`;
            return {
                pipelineName: base.pipelineName,
                baseVersion: base.pipelineVersion,
                headVersion: head.pipelineVersion,
                compareUrl,
                modalHtml: renderPipelineCompareBody(
                    base.pipelineVersion,
                    head.pipelineVersion,
                    await fetchPipelineCompareSummary(base.compareRef, head.compareRef)
                ),
            };
        })
    );
}

function isPlainObject(value) {
    return Object.prototype.toString.call(value) === "[object Object]";
}

function collectJsonDiffs(left, right, path = []) {
    if (Array.isArray(left) && Array.isArray(right)) {
        const maxLength = Math.max(left.length, right.length);
        return Array.from({ length: maxLength }, (_, index) =>
            collectJsonDiffs(left[index], right[index], [...path, index])
        )
            .flat()
            .filter(Boolean);
    }
    if (isPlainObject(left) && isPlainObject(right)) {
        return [...new Set([...Object.keys(left), ...Object.keys(right)])]
            .sort(FILTER_VALUE_COLLATOR.compare)
            .flatMap((key) => collectJsonDiffs(left[key], right[key], [...path, key]))
            .filter(Boolean);
    }
    const leftText = JSON.stringify(left);
    const rightText = JSON.stringify(right);
    if (leftText === rightText) return [];
    return [{ path: path.join("."), left, right }];
}

function renderDiffValue(value) {
    return value === undefined ? "undefined" : JSON.stringify(value);
}

async function fetchRegistryFile(path, subdir, kindLabel) {
    const resp = await cachedFetch(codeRepoRawUrl(`src/dandi_compute_code/aind_ephys_pipeline/${subdir}/${path}`));
    if (!resp.ok) {
        throw new Error(`Failed to load registered ${kindLabel} file ${path} (HTTP ${resp.status}).`);
    }
    return resp;
}

async function buildParamsDiffPairs() {
    const paramsEntries = uniqueRegistryEntries(PARAMS_REGISTRY);
    const paramsWithJson = await Promise.all(
        paramsEntries.map(async (entry) => ({
            ...entry,
            sourceUrl: codeRepoBlobUrl(`src/dandi_compute_code/aind_ephys_pipeline/params/${entry.path}`),
            json: await (await fetchRegistryFile(entry.path, "params", "params")).json(),
        }))
    );
    return buildPairwiseComparisons(paramsWithJson).map(([base, head]) => ({
        baseAlias: base.alias,
        headAlias: head.alias,
        baseSourceUrl: base.sourceUrl,
        headSourceUrl: head.sourceUrl,
        changes: collectJsonDiffs(base.json, head.json),
    }));
}

function collectTextDiffs(leftText, rightText) {
    const leftLines = (leftText ?? "").split("\n");
    const rightLines = (rightText ?? "").split("\n");
    const maxLength = Math.max(leftLines.length, rightLines.length);
    const changedLineIndexes = [];
    for (let index = 0; index < maxLength; index += 1) {
        if (leftLines[index] !== rightLines[index]) {
            changedLineIndexes.push(index);
        }
    }
    if (changedLineIndexes.length === 0) return [];
    const contextWindows = changedLineIndexes
        .map((changedLineIndex) => ({
            start: Math.max(0, changedLineIndex - 3),
            end: Math.min(maxLength - 1, changedLineIndex + 3),
        }))
        .sort((a, b) => a.start - b.start);
    const mergedWindows = [];
    for (const window of contextWindows) {
        const previous = mergedWindows[mergedWindows.length - 1];
        if (!previous || window.start > previous.end + 1) {
            mergedWindows.push({ ...window });
            continue;
        }
        previous.end = Math.max(previous.end, window.end);
    }
    return mergedWindows.map(({ start, end }) => {
        const lineNumberWidth = String(end + 1).length;
        const leftSnippet = [];
        const rightSnippet = [];
        for (let index = start; index <= end; index += 1) {
            const left = leftLines[index] ?? "";
            const right = rightLines[index] ?? "";
            const lineNumber = String(index + 1).padStart(lineNumberWidth, " ");
            const isChanged = leftLines[index] !== rightLines[index];
            leftSnippet.push(`${lineNumber} ${isChanged ? "-" : " "} ${left}`);
            rightSnippet.push(`${lineNumber} ${isChanged ? "+" : " "} ${right}`);
        }
        return {
            path: start === end ? `line ${start + 1}` : `lines ${start + 1}-${end + 1}`,
            left: leftSnippet.join("\n"),
            right: rightSnippet.join("\n"),
        };
    });
}

async function buildConfigDiffPairs() {
    const configEntries = uniqueRegistryEntries(CONFIG_REGISTRY);
    const configWithText = await Promise.all(
        configEntries.map(async (entry) => ({
            ...entry,
            sourceUrl: codeRepoBlobUrl(`src/dandi_compute_code/aind_ephys_pipeline/configs/${entry.path}`),
            text: await (await fetchRegistryFile(entry.path, "configs", "config")).text(),
        }))
    );
    return buildPairwiseComparisons(configWithText).map(([base, head]) => ({
        baseAlias: base.alias,
        headAlias: head.alias,
        baseSourceUrl: base.sourceUrl,
        headSourceUrl: head.sourceUrl,
        changes: collectTextDiffs(base.text, head.text),
    }));
}

function renderDiffMatrix(entries, renderHeaderCell, renderBodyCell) {
    if (entries.length < 2) return "";
    const columnEntries = entries.slice(0, -1);
    const columnHeaders = columnEntries
        .map((entry) => `<th scope="col" class="diff-matrix-col-header">${renderHeaderCell(entry)}</th>`)
        .join("");
    const bodyRows = entries
        .map((rowEntry, rowIndex) => {
            const cells = columnEntries
                .map((columnEntry, columnIndex) => {
                    if (rowIndex <= columnIndex) {
                        return '<td class="diff-matrix-cell diff-matrix-cell-empty" aria-hidden="true"></td>';
                    }
                    return `<td class="diff-matrix-cell">${renderBodyCell(columnEntry, rowEntry)}</td>`;
                })
                .join("");
            return `<tr>
                <th scope="row" class="diff-matrix-row-header">${renderHeaderCell(rowEntry)}</th>
                ${cells}
            </tr>`;
        })
        .join("");
    return `<div class="diff-matrix-wrap">
        <table class="diff-matrix">
            <thead>
                <tr>
                    <th class="diff-matrix-corner" aria-hidden="true"></th>
                    ${columnHeaders}
                </tr>
            </thead>
            <tbody>${bodyRows}</tbody>
        </table>
    </div>`;
}

function renderDiffModalTrigger(label, bodyHtml, title = null) {
    const titleAttr = title ? ` data-modal-title="${e(title)}"` : "";
    return `<button type="button" class="diff-cell-trigger" aria-haspopup="dialog" data-modal-html="${e(bodyHtml)}"${titleAttr}>
        <span class="diff-cell-trigger-label">${e(label)}</span>
    </button>`;
}

function renderDiffPage(data) {
    const configEntries = data.configEntries ?? [];
    const configPairMap = data.configPairMap ?? new Map();
    const pipelineHtml =
        data.pipelineEntries.length > 1
            ? renderDiffMatrix(
                  data.pipelineEntries,
                  (entry) => renderPipelineInfo(entry.pipelineName, entry.pipelineVersion),
                  (baseEntry, headEntry) => {
                      const pair = data.pipelinePairMap.get(`${baseEntry.key}\x00${headEntry.key}`);
                      if (!pair) return "";
                      return renderDiffModalTrigger("View compare", pair.modalHtml);
                  }
              )
            : '<p class="diff-empty-state">Only one distinct pipeline repository revision is currently present, so there are no pipeline comparisons to show.</p>';
    const paramsHtml =
        data.paramsEntries.length > 1
            ? renderDiffMatrix(
                  data.paramsEntries,
                  (entry) => renderDiffInlineLink(entry.sourceUrl, entry.alias),
                  (baseEntry, headEntry) => {
                      const pair = data.paramsPairMap.get(`${baseEntry.key}\x00${headEntry.key}`);
                      const pairChanges = pair?.changes ?? [];
                      const baseLinkHtml = renderDiffInlineLink(baseEntry.sourceUrl, baseEntry.alias);
                      const headLinkHtml = renderDiffInlineLink(headEntry.sourceUrl, headEntry.alias);
                      const bodyHtml = `<div class="diff-pair-card">
                            ${renderNamedDiffTable(
                                "Parameter",
                                baseEntry.alias,
                                headEntry.alias,
                                pairChanges,
                                baseLinkHtml,
                                headLinkHtml
                            )}
                        </div>`;
                      const buttonLabel =
                          pairChanges.length > 0
                              ? `View ${pairChanges.length} change${pairChanges.length !== 1 ? "s" : ""}`
                              : "View diff";
                      return renderDiffModalTrigger(buttonLabel, bodyHtml);
                  }
              )
            : '<p class="diff-empty-state">No registered params files were found.</p>';
    const configHtml =
        configEntries.length > 1
            ? renderDiffMatrix(
                  configEntries,
                  (entry) => renderDiffInlineLink(entry.sourceUrl, entry.alias),
                  (baseEntry, headEntry) => {
                      const pair = configPairMap.get(`${baseEntry.key}\x00${headEntry.key}`);
                      const pairChanges = pair?.changes ?? [];
                      const baseLinkHtml = renderDiffInlineLink(baseEntry.sourceUrl, baseEntry.alias);
                      const headLinkHtml = renderDiffInlineLink(headEntry.sourceUrl, headEntry.alias);
                      const bodyHtml = `<div class="diff-pair-card">
                            ${renderConfigDiffTable(
                                baseEntry.alias,
                                headEntry.alias,
                                pairChanges,
                                baseLinkHtml,
                                headLinkHtml
                            )}
                        </div>`;
                      const buttonLabel =
                          pairChanges.length > 0
                              ? `View ${pairChanges.length} change${pairChanges.length !== 1 ? "s" : ""}`
                              : "View diff";
                      return renderDiffModalTrigger(buttonLabel, bodyHtml);
                  }
              )
            : '<p class="diff-empty-state">No registered config files were found.</p>';

    return `<div class="diff-page">
    <section class="diff-section">
        <div class="diff-section-banner">
            Pipeline GitHub compares
        </div>
        ${pipelineHtml}
    </section>
    <section class="diff-section">
        <div class="diff-section-banner">
            Registered params JSON diffs
        </div>
        ${paramsHtml}
    </section>
    <section class="diff-section">
        <div class="diff-section-banner">
            Registered config diffs
        </div>
        ${configHtml}
    </section>
</div>`;
}

/* ─── Nested rendering ──────────────────────────────────────── */
function renderDandisets(runs) {
    const byDandiset = groupBy(runs, (r) => r.dandisetId);
    // Sort dandisets by most recent run (runs are already sorted newest-first)
    const dandisetIds = [...byDandiset.keys()].sort((a, b) => {
        const aDate = byDandiset.get(a)[0]?.runDate ?? "";
        const bDate = byDandiset.get(b)[0]?.runDate ?? "";
        return bDate.localeCompare(aDate);
    });
    const autoExpand = dandisetIds.length === 1;
    return dandisetIds.map((id) => renderDandisetGroup(id, byDandiset.get(id), autoExpand)).join("");
}

function renderDandisetGroup(dandisetId, runs, autoExpand = false) {
    const bySubject = groupBy(runs, (r) => r.subject);
    const subjects = [...bySubject.keys()].sort();
    const autoExpandSubject = autoExpand && subjects.length === 1;
    const subjectHtml = subjects
        .map((s) => renderSubjectGroup(dandisetId, s, bySubject.get(s), autoExpandSubject))
        .join("");

    return `
<details class="dandiset-group"${autoExpand ? " open" : ""}>
    <summary class="dandiset-summary">
        <span class="dandiset-summary-inner">
            <a class="dandiset-link" href="${e(neurosiftDandisetUrl(dandisetId))}"
               target="_blank" rel="noopener" onclick="event.stopPropagation()">Dandiset&nbsp;${e(dandisetId)}</a>
            <a class="dandi-view-link" href="${dandiBaseUrl(dandisetId)}/dandiset/${e(dandisetId)}"
               target="_blank" rel="noopener" onclick="event.stopPropagation()">DANDI&nbsp;↖</a>
            <span class="group-meta">
                <span class="group-count">${subjects.length}&nbsp;subject${subjects.length !== 1 ? "s" : ""}</span>
                <span class="run-sep">·</span>
                <span class="group-count">${runs.length}&nbsp;run${runs.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
            <a class="narrow-link" href="${narrowUrl({ dandiset: dandisetId })}"
               title="Narrow view to Dandiset ${e(dandisetId)}" onclick="event.stopPropagation()">⊕ Narrow</a>
        </span>
    </summary>
    <div class="dandiset-body">
        ${subjectHtml}
    </div>
</details>`;
}

function renderSubjectGroup(dandisetId, subject, runs, autoExpand = false) {
    // Prefer a run with a known assetId to determine inSourcedata for the DANDI link
    const rep = runs.find((r) => r.assetId) ?? runs[0];
    const location = rep.inSourcedata ? `sourcedata/sub-${subject}` : `sub-${subject}`;
    const subjectUrl = `${dandiBaseUrl(dandisetId)}/dandiset/${e(dandisetId)}/draft/files?location=${e(location)}`;

    const bySession = groupBy(runs, (r) => r.session);
    // Sort sessions; null (no session) sorts last
    const sessions = [...bySession.keys()].sort((a, b) => {
        if (a === null) return 1;
        if (b === null) return -1;
        return String(a).localeCompare(String(b));
    });
    const autoExpandSession = autoExpand && sessions.length === 1;
    const sessionHtml = sessions
        .map((ses) => renderSessionGroup(dandisetId, subject, ses, bySession.get(ses), autoExpandSession))
        .join("");

    return `
<details class="subject-group"${autoExpand ? " open" : ""}>
    <summary class="subject-summary">
        <span class="group-summary-inner">
            <a class="group-link" href="${e(subjectUrl)}" target="_blank" rel="noopener"
               onclick="event.stopPropagation()">Sub:&nbsp;<strong>${e(subject)}</strong></a>
            <span class="group-meta">
                <span class="group-count">${sessions.length}&nbsp;session${sessions.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
            <a class="narrow-link" href="${narrowUrl({ dandiset: dandisetId, subject })}"
               title="Narrow view to Sub: ${e(subject)}" onclick="event.stopPropagation()">⊕ Narrow</a>
        </span>
    </summary>
    <div class="subject-body">
        ${sessionHtml}
    </div>
</details>`;
}

function renderSessionGroup(dandisetId, subject, session, runs, autoExpand = false) {
    const rep = runs.find((r) => r.contentHash || r.assetId) ?? runs[0];
    const sessionLabel = session !== null ? session : "—";
    const sessionHref = neurosiftSessionUrl(dandisetId, rep.contentHash, rep.assetId);
    const sessionLinkHtml = sessionHref
        ? `<a class="group-link" href="${e(sessionHref)}"
              target="_blank" rel="noopener" onclick="event.stopPropagation()">Ses:&nbsp;<strong>${e(sessionLabel)}</strong></a>`
        : `<span class="group-label">Ses:&nbsp;<strong>${e(sessionLabel)}</strong></span>`;

    const runsHtml = runs.map(renderRunEntry).join("");

    return `
<details class="session-group"${autoExpand ? " open" : ""}>
    <summary class="session-summary">
        <span class="group-summary-inner">
            ${sessionLinkHtml}
            <span class="group-meta">
                <span class="group-count">${runs.length}&nbsp;job${runs.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
            <a class="narrow-link" href="${narrowUrl({ dandiset: dandisetId, subject, session })}"
               title="Narrow view to Ses: ${e(session)}" onclick="event.stopPropagation()">⊕ Narrow</a>
        </span>
    </summary>
    <div class="session-body">
        ${runsHtml}
    </div>
</details>`;
}

function renderPipelineVersionGroup(dandisetId, subject, session, pipelineName, pipelineVersion, runs) {
    const byParams = groupBy(runs, (r) => `${r.paramsProfile}\x00${r.configHash}`);
    const paramKeys = [...byParams.keys()].sort();
    const paramsHtml = paramKeys
        .map((key) => {
            const sep = key.indexOf("\x00");
            const paramsProfile = key.slice(0, sep);
            const configHash = key.slice(sep + 1);
            return renderParamsGroup(paramsProfile, configHash, byParams.get(key));
        })
        .join("");

    return `
<details class="pipeline-version-group">
    <summary class="pipeline-version-summary">
        <span class="group-summary-inner">
            <span class="group-pipeline">${renderPipelineInfo(pipelineName, pipelineVersion)}</span>
            <span class="group-meta">
                <span class="group-count">${paramKeys.length}&nbsp;configuration${paramKeys.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
            <a class="narrow-link" href="${e(narrowUrl({ dandiset: dandisetId, subject, session, pipelineVersion }))}"
               title="Narrow view to version: ${e(pipelineVersion)}" onclick="event.stopPropagation()">⊕ Narrow</a>
        </span>
    </summary>
    <div class="pipeline-version-body">
        ${paramsHtml}
    </div>
</details>`;
}

function renderParamsGroup(paramsProfile, configHash, runs) {
    const runsHtml = runs.map(renderRunEntry).join("");
    const paramsLabel = renderRegistryLink("Params", paramsProfile, PARAMS_REGISTRY, "params");
    const configLabel = configHash
        ? `&nbsp;·&nbsp;${renderRegistryLink("Config", configHash, CONFIG_REGISTRY, "configs")}`
        : "";

    return `
<details class="params-group">
    <summary class="params-summary">
        <span class="group-summary-inner">
            <span class="group-label">${paramsLabel}${configLabel}</span>
            <span class="group-meta">
                <span class="group-count">${runs.length}&nbsp;run${runs.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
        </span>
    </summary>
    <div class="params-body">
        ${runsHtml}
    </div>
</details>`;
}

/* ─── Flat view rendering ───────────────────────────────────── */
function renderFlatRunEntry(run) {
    const sc =
        run.status === "success"
            ? "status-success"
            : run.status === "failed"
              ? "status-failed"
              : run.status === "queued"
                ? "status-queued"
                : run.status === "partial"
                  ? "status-partial"
                  : "status-unknown";
    const slbl =
        run.status === "success"
            ? "✓ Success"
            : run.status === "failed"
              ? "✗ Failed"
              : run.status === "queued"
                ? "⧗ Queued"
                : run.status === "partial"
                  ? "⚠ Partial"
                  : "? Unknown";

    const logFiles = run.hasLogs ? STANDARD_LOG_FILES : [];

    const inlineLogs = logFiles
        .filter((f) => INLINE_REPORT_FILES.has(f))
        .sort((a, b) => {
            const ai = INLINE_REPORT_ORDER.indexOf(a);
            const bi = INLINE_REPORT_ORDER.indexOf(b);
            return (ai === -1 ? Infinity : ai) - (bi === -1 ? Infinity : bi);
        });
    const buttonLogs = [...logFiles.filter((f) => !INLINE_REPORT_FILES.has(f)), ...(run.slurmLogs ?? [])];
    const hasLogs = buttonLogs.length > 0;
    const hasInline = inlineLogs.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;
    const hasSourceVersions = run.generatedBy && run.generatedBy.length > 0;
    const hasViz = run.vizData && run.vizData.length > 0;
    const bytes = runByteCount(run);
    const bytesHtml =
        bytes === null
            ? ""
            : `<span class="run-sep">·</span><span class="run-bytes">Asset size:&nbsp;${formatByteCount(bytes)}</span>`;

    const dandiPath = String(run.dandiPath ?? "").trim();
    const dandiDirectory = dandiPathDirectoryParts(dandiPath).join("/");
    const fallbackLocation = run.inSourcedata ? `sourcedata/sub-${run.subject}` : `sub-${run.subject}`;
    const location = dandiDirectory || fallbackLocation;
    const dandiPathLabel = dandiPath || location;
    const dandiPathUrl = `${dandiBaseUrl(run.dandisetId)}/dandiset/${e(run.dandisetId)}/draft/files?location=${encodeURIComponent(location)}`;

    return `
<div class="run-entry flat-run-entry ${sc}">
    <div class="run-entry-header flat-run-header">
        <a class="dandi-view-link" href="${dandiBaseUrl(run.dandisetId)}/dandiset/${e(run.dandisetId)}" target="_blank" rel="noopener">DANDI&nbsp;↖</a>
        <span class="status-badge ${sc}">${slbl}</span>
        <span class="flat-run-context">
            <a class="flat-ctx-link" href="${e(neurosiftDandisetUrl(run.dandisetId))}" target="_blank" rel="noopener">Dandiset&nbsp;${e(run.dandisetId)}</a>
            <span class="run-sep">·</span>
            <a class="flat-ctx-link flat-ctx-path" href="${e(dandiPathUrl)}" target="_blank" rel="noopener">Path:&nbsp;<strong>${e(dandiPathLabel)}</strong></a>
            ${run.runDate ? `<span class="flat-ctx-break"></span><span class="flat-ctx-text flat-ctx-date">${e(run.runDate)}</span>` : ""}
            <span class="run-sep">·</span>
            <span class="flat-ctx-text">${renderRegistryLink("Params", run.paramsProfile, PARAMS_REGISTRY, "params")}</span>
            ${run.configHash ? `<span class="run-sep">·</span><span class="flat-ctx-text">${renderRegistryLink("Config", run.configHash, CONFIG_REGISTRY, "configs")}</span>` : ""}
        </span>
        ${bytesHtml}
        <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
        <a class="run-entry-github-link" href="${e(treeUrl(run.path))}" target="_blank" rel="noopener">↗ GitHub</a>
    </div>

    ${hasSourceVersions ? renderSourceVersionsSection(run.generatedBy) : ""}
    ${hasTasks ? renderTraceSection(run.tasks) : ""}
    ${hasViz ? renderVisualizationSection(run.vizData) : ""}
    ${hasLogs ? renderLogSection(run.path, buttonLogs) : ""}
    ${hasInline ? renderReportSection(run.path, inlineLogs) : ""}
</div>`;
}

function renderFlatList(runs) {
    return `<div class="flat-list">${sortRuns(runs).map(renderFlatRunEntry).join("")}</div>`;
}

/* ─── Layout toggle ─────────────────────────────────────────── */
function renderLayoutBar() {
    const isFlat = _layoutMode === "flat";
    const isAscending = _sortDirection === "asc";
    return `<div class="layout-bar">
    <div class="layout-bar-group">
        <span class="layout-bar-label">View:</span>
        <button class="layout-btn${!isFlat ? " layout-btn-active" : ""}" data-layout="tree" aria-pressed="${!isFlat}">Tree</button>
        <button class="layout-btn${isFlat ? " layout-btn-active" : ""}" data-layout="flat" aria-pressed="${isFlat}">Flat</button>
    </div>
    <div class="layout-bar-group layout-bar-group-sort">
        <label class="layout-sort-wrap">
            <span class="layout-bar-label">Sort by:</span>
            <select class="layout-sort-select" data-sort-mode aria-label="Sort runs">
                <option value="attempt"${_sortMode === "attempt" ? " selected" : ""}>Attempt</option>
                <option value="created_at"${_sortMode === "created_at" ? " selected" : ""}>Created</option>
            </select>
        </label>
        <button
            class="layout-btn layout-sort-direction-btn"
            type="button"
            data-sort-direction
            aria-label="${isAscending ? "Sort ascending" : "Sort descending"}"
            aria-pressed="${isAscending}"
            title="${isAscending ? "Sort ascending" : "Sort descending"}"
        >${isAscending ? "↑" : "↓"}</button>
    </div>
    <button
        class="layout-btn layout-btn-refresh"
        type="button"
        data-refresh-queue
        aria-label="Refresh queue state"
        title="Clear cache and reload queue state"
    >↺ Refresh</button>
</div>`;
}

function rerenderRuns() {
    const sortedRuns = sortRuns(_filteredRuns);
    document.getElementById("runs").innerHTML =
        _layoutMode === "flat" ? renderFlatList(sortedRuns) : renderDandisets(sortedRuns);
    initInlineHtmlFrames();
}

function initLayoutToggle() {
    const bar = document.getElementById("layout-bar");
    if (!bar) return;
    bar.innerHTML = renderLayoutBar();
    if (bar.dataset.initialized) return;
    bar.dataset.initialized = "1";
    bar.addEventListener("click", (ev) => {
        const btn = ev.target.closest("[data-layout]");
        if (btn) {
            const mode = btn.dataset.layout;
            if (mode === _layoutMode) return;
            _layoutMode = mode;
            localStorage.setItem("layoutMode", mode);
            updateLayoutModeUrl(mode);
            bar.innerHTML = renderLayoutBar();
            rerenderRuns();
            return;
        }
        const directionBtn = ev.target.closest("[data-sort-direction]");
        if (directionBtn) {
            _sortDirection = _sortDirection === "desc" ? "asc" : "desc";
            localStorage.setItem("sortDirection", _sortDirection);
            updateSortDirectionUrl(_sortDirection);
            bar.innerHTML = renderLayoutBar();
            rerenderRuns();
            return;
        }
        const refreshBtn = ev.target.closest("[data-refresh-queue]");
        if (!refreshBtn) return;
        clearQueueStateCache();
        loadQueueData();
    });
    bar.addEventListener("change", (ev) => {
        const select = ev.target.closest("[data-sort-mode]");
        if (!select) return;
        const mode = select.value;
        if (mode === _sortMode) return;
        _sortMode = mode;
        localStorage.setItem("sortMode", mode);
        updateSortModeUrl(mode);
        bar.innerHTML = renderLayoutBar();
        rerenderRuns();
    });
}
/* ─── Log modal ─────────────────────────────────────────────── */
let _modalGeneration = 0;

function initModal() {
    document.getElementById("log-modal-close").addEventListener("click", closeLogModal);
    document.getElementById("log-modal").addEventListener("click", (evt) => {
        if (evt.target === document.getElementById("log-modal")) closeLogModal();
    });
    document.addEventListener("keydown", (evt) => {
        if (evt.key === "Escape" && !document.getElementById("log-modal").hidden) closeLogModal();
    });

    document.getElementById("runs").addEventListener("click", (evt) => {
        const btn = evt.target.closest(".log-link");
        if (!btn) return;
        openLogModal(
            btn.dataset.logPath,
            btn.dataset.logLabel,
            btn.dataset.logHtml === "true",
            btn.dataset.logExternal
        );
    });

    const runsEl = document.getElementById("runs");
    if (runsEl) {
        runsEl.addEventListener("click", (evt) => {
            const diffTrigger = evt.target.closest(".diff-cell-trigger");
            if (diffTrigger) {
                evt.preventDefault();
                openHtmlModal(
                    diffTrigger.dataset.modalTitle,
                    diffTrigger.dataset.modalHtml,
                    diffTrigger.dataset.modalExternal || null,
                    diffTrigger.dataset.modalExternalLabel || "↗ Open"
                );
                return;
            }
            const link = evt.target.closest(".viz-link");
            if (!link) return;
            evt.preventDefault();
            openVizModal(link.dataset.vizUrl, link.dataset.vizLabel);
        });
    }
}

function setModalExternalLink(externalHref, externalLabel = "↗ Open") {
    const extLink = document.getElementById("log-modal-external");
    extLink.textContent = externalLabel;
    if (externalHref) {
        extLink.href = externalHref;
        extLink.hidden = false;
    } else {
        extLink.hidden = true;
        extLink.removeAttribute("href");
    }
}

function setModalTitle(title) {
    const modalBox = document.querySelector("#log-modal .log-modal-box");
    const titleEl = document.getElementById("log-modal-title");
    titleEl.textContent = title ?? "";
    titleEl.hidden = !title;
    if (title) {
        modalBox?.setAttribute("aria-labelledby", "log-modal-title");
        modalBox?.removeAttribute("aria-label");
    } else {
        modalBox?.removeAttribute("aria-labelledby");
        modalBox?.setAttribute("aria-label", "Details");
    }
}

function openLogModal(filePath, label, isHtml, externalHref) {
    const overlay = document.getElementById("log-modal");
    const bodyEl = document.getElementById("log-modal-body");

    const generation = ++_modalGeneration;

    setModalTitle(label);
    setModalExternalLink(externalHref);
    overlay.hidden = false;
    document.body.style.overflow = "hidden";

    bodyEl.innerHTML = `<div class="log-modal-loading"><div class="spinner"></div> Loading…</div>`;
    fetchLogText(filePath).then((content) => {
        if (_modalGeneration !== generation) return;
        if (content === null) {
            bodyEl.innerHTML = `<p class="log-modal-error">Failed to load log file.</p>`;
            return;
        }
        bodyEl.innerHTML = "";
        if (isHtml) {
            // Use srcdoc to bypass X-Frame-Options restrictions on raw.githubusercontent.com
            const iframe = document.createElement("iframe");
            iframe.className = "log-modal-iframe";
            iframe.setAttribute("sandbox", "allow-scripts");
            iframe.setAttribute("title", label);
            iframe.srcdoc = content;
            bodyEl.appendChild(iframe);
        } else {
            const pre = document.createElement("pre");
            pre.className = "log-modal-text";
            pre.textContent = content;
            bodyEl.appendChild(pre);
        }
    });
}

function closeLogModal() {
    _modalGeneration++;
    const overlay = document.getElementById("log-modal");
    overlay.hidden = true;
    document.body.style.overflow = "";
    document.getElementById("log-modal-body").innerHTML = "";
}

function openHtmlModal(title, html, externalHref = null, externalLabel = "↗ Open") {
    const overlay = document.getElementById("log-modal");
    const bodyEl = document.getElementById("log-modal-body");

    _modalGeneration++;

    setModalTitle(title);
    setModalExternalLink(externalHref, externalLabel);
    overlay.hidden = false;
    document.body.style.overflow = "hidden";
    bodyEl.innerHTML = html;
}

function openVizModal(url, label) {
    const overlay = document.getElementById("log-modal");
    const bodyEl = document.getElementById("log-modal-body");

    _modalGeneration++;

    setModalTitle(label);
    setModalExternalLink(url);
    overlay.hidden = false;
    document.body.style.overflow = "hidden";

    bodyEl.innerHTML = "";
    const img = document.createElement("img");
    img.className = "viz-modal-img";
    img.src = url;
    img.alt = label;
    bodyEl.appendChild(img);
}

async function fetchLogText(filePath) {
    try {
        const resp = await cachedFetch(cdnUrl(filePath));
        if (!resp.ok) return null;
        return resp.text();
    } catch {
        return null;
    }
}

/* Fetch and inject srcdoc for all inline report iframes in the page */
function initInlineHtmlFrames() {
    const frames = Array.from(document.querySelectorAll("iframe[data-srcdoc-path]"));
    const frameSet = new Set(frames);

    // The injected script reports scrollHeight on load AND responds to a 'requestHeight'
    // message from the parent. The parent re-requests when a collapsed <details> is opened,
    // because the iframe layout is zero-height while the section is hidden.
    // Sandboxed srcdoc iframes have opaque origin so evt.origin === 'null'; we also verify
    // evt.source is one of our known iframe windows as an additional guard.
    const heightScript = `<script>
(function(){
function send(){window.parent.postMessage({type:'iframeHeight',h:document.documentElement.scrollHeight},'*');}
if(document.readyState==='complete'){send();}else{window.addEventListener('load',send);}
window.addEventListener('message',function(e){if(e.source===window.parent&&e.data&&e.data.type==='requestHeight')send();});
})();
</script>`;

    window.addEventListener("message", (evt) => {
        if (typeof evt.origin !== "string" || (evt.origin !== "null" && evt.origin !== window.location.origin)) return;
        if (!evt.data || evt.data.type !== "iframeHeight") return;
        for (const iframe of frameSet) {
            if (iframe.contentWindow === evt.source && evt.data.h > 0) {
                iframe.style.height = evt.data.h + "px";
            }
        }
    });

    // patchedContent holds the fully-patched HTML for each iframe, populated asynchronously.
    // injectedFrames tracks which iframes have already had their srcdoc set.
    // srcdoc is set lazily: only once the iframe's parent <details> is open (visible), so
    // that chart libraries (e.g. Highcharts in timeline.html) render in a non-zero container
    // and avoid producing invalid negative <rect> widths in the browser console.
    const patchedContent = new Map();
    const injectedFrames = new Set();

    function maybeInjectFrame(iframe) {
        if (injectedFrames.has(iframe)) return;
        if (!patchedContent.has(iframe)) return; // content not yet fetched
        if (iframe.closest("details:not([open])")) return; // still inside a closed <details>
        injectedFrames.add(iframe);
        iframe.srcdoc = patchedContent.get(iframe);
    }

    // When a <details> containing inline frames is opened, inject any pending frames
    // whose content is already available, and request a fresh height measurement from
    // frames that are already loaded.
    document.querySelectorAll("details").forEach((details) => {
        if (!details.querySelector("iframe[data-srcdoc-path]")) return;
        details.addEventListener("toggle", () => {
            if (!details.open) return;
            requestAnimationFrame(() => {
                details.querySelectorAll("iframe[data-srcdoc-path]").forEach((iframe) => {
                    if (injectedFrames.has(iframe)) {
                        if (iframe.contentWindow) {
                            // '*' is required: sandboxed srcdoc iframes have opaque ('null') origin,
                            // which is not a valid targetOrigin — only '*' reaches them.
                            // The message contains no sensitive data ({type:'requestHeight'} only).
                            iframe.contentWindow.postMessage({ type: "requestHeight" }, "*");
                        }
                    } else {
                        maybeInjectFrame(iframe);
                    }
                });
            });
        });
    });

    // Injected into <head> for most reports: nudge the browser's default color-scheme to light
    // so any unset colors pick up readable dark-on-white defaults.
    const lightStyle = "<style>html{color-scheme:light;}</style>";
    // timeline.html (Nextflow-generated) has an *explicit* dark navy background in its own CSS,
    // so color-scheme alone cannot help — the dark background stays and light-scheme defaults
    // produce dark text on dark background (invisible). Override background + text explicitly.
    const timelineLightStyle =
        "<style>html,body{background:#ffffff!important;color:#333333!important;color-scheme:light;}</style>";

    Promise.all(
        frames.map(async (iframe) => {
            const content = await fetchLogText(iframe.dataset.srcdocPath);
            const html =
                content !== null
                    ? content
                    : '<body style="font-family:sans-serif;padding:20px;color:#e05c5c">Failed to load report.</body>';
            // Insert light-mode override and height-reporter into the document.
            // Prefer inserting the style in <head> and the script before </body>.
            const isTimeline = iframe.dataset.srcdocPath.endsWith("timeline.html");
            const styleToInject = isTimeline ? timelineLightStyle : lightStyle;
            const lcHtml = html.toLowerCase();
            const headClose = lcHtml.indexOf("</head>");
            const bodyClose = lcHtml.lastIndexOf("</body>");
            let patched =
                headClose !== -1
                    ? html.slice(0, headClose) + styleToInject + html.slice(headClose)
                    : styleToInject + html;
            // styleToInject was inserted at or before bodyClose, so adjust the position by its length.
            const adjustedBodyClose = bodyClose !== -1 ? bodyClose + styleToInject.length : -1;
            patched =
                adjustedBodyClose !== -1
                    ? patched.slice(0, adjustedBodyClose) + heightScript + patched.slice(adjustedBodyClose)
                    : patched + heightScript;
            patchedContent.set(iframe, patched);
            maybeInjectFrame(iframe);
        })
    );
}

/* ─── Params Editor ─────────────────────────────────────────── */

/* Module-level params editor state */
let _paramsSchema = null;
let _paramsCurrentValues = null;
let _paramsDefaultValues = null;

/* Desired display order for top-level schema sections.
   Sections not listed here appear after, in schema order. */
const PARAMS_SECTION_ORDER = [
    "job_dispatch",
    "preprocessing",
    "spikesorting",
    "postprocessing",
    "curation",
    "visualization",
    "nwb",
];

/* Strip trailing commas from JSON-like text so files authored with them
   (e.g. default_params.json from the pipeline repo) can be parsed. */
function stripTrailingCommas(text) {
    return text.replace(/,\s*([}\]])/g, "$1");
}

function paramsResolveRef(node) {
    if (!node || typeof node !== "object") return node;
    if (node.$ref) {
        const parts = node.$ref.replace(/^#\//, "").split("/");
        let resolved = _paramsSchema;
        for (const p of parts) resolved = resolved[p];
        const { $ref: _, ...rest } = node;
        return { ...paramsResolveRef(resolved), ...rest };
    }
    return node;
}

function paramsBuildDefaults(schemaNode) {
    const node = paramsResolveRef(schemaNode);
    if (!node) return undefined;
    if (node.type === "object" && node.properties) {
        const obj = {};
        for (const [k, v] of Object.entries(node.properties)) {
            const val = paramsBuildDefaults(v);
            if (val !== undefined) obj[k] = val;
        }
        return Object.keys(obj).length ? obj : {};
    }
    if ("default" in node) return JSON.parse(JSON.stringify(node.default));
    return undefined;
}

function paramsDeepGet(obj, path) {
    let cur = obj;
    for (const p of path) {
        if (cur == null || typeof cur !== "object") return undefined;
        cur = cur[p];
    }
    return cur;
}

function paramsDeepSet(obj, path, value) {
    let cur = obj;
    for (let i = 0; i < path.length - 1; i++) {
        if (!(path[i] in cur) || typeof cur[path[i]] !== "object") cur[path[i]] = {};
        cur = cur[path[i]];
    }
    cur[path[path.length - 1]] = value;
}

function paramsGetTypeDefault(type) {
    if (type === "string") return "";
    if (type === "number" || type === "integer") return 0;
    if (type === "boolean") return false;
    if (type === "array") return [];
    if (type === "object") return {};
    return null;
}

function paramsEl(tag, attrs, ...children) {
    const elem = document.createElement(tag);
    if (attrs) {
        for (const [k, v] of Object.entries(attrs)) {
            if (k === "class") {
                elem.className = v;
            } else if (k.startsWith("on")) {
                elem.addEventListener(k.slice(2), v);
            } else if (v === true) {
                elem.setAttribute(k, "");
            } else if (v !== false && v !== null && v !== undefined) {
                elem.setAttribute(k, v);
            }
        }
    }
    for (const c of children) {
        if (c == null) continue;
        if (typeof c === "string") elem.appendChild(document.createTextNode(c));
        else elem.appendChild(c);
    }
    return elem;
}

function updateParamsPreview() {
    const output = document.getElementById("params-output");
    if (output) output.textContent = JSON.stringify(_paramsCurrentValues, null, 4);
}

function paramsMarkChanged(field, path) {
    const defVal = paramsDeepGet(_paramsDefaultValues, path);
    const curVal = paramsDeepGet(_paramsCurrentValues, path);
    field.classList.toggle("changed", JSON.stringify(curVal) !== JSON.stringify(defVal));
    updateParamsPreview();
}

function buildParamsField(label, schemaNode, path) {
    const node = paramsResolveRef(schemaNode);
    const curVal = paramsDeepGet(_paramsCurrentValues, path);
    const defVal = paramsDeepGet(_paramsDefaultValues, path);
    const isChanged = JSON.stringify(curVal) !== JSON.stringify(defVal);

    const field = paramsEl("div", { class: "params-field" + (isChanged ? " changed" : "") });
    field.dataset.path = path.join(".");

    const labelEl = paramsEl("div", { class: "params-field-label" }, label);
    if (node.description) {
        labelEl.appendChild(paramsEl("span", { class: "params-field-desc" }, node.description));
    }
    field.appendChild(labelEl);

    const inputWrap = paramsEl("div", { class: "params-field-input" });

    const nullable = Array.isArray(node.type) && node.type.includes("null");
    const types = Array.isArray(node.type) ? node.type.filter((t) => t !== "null") : [node.type];
    const primaryType = types[0] || "string";

    if (nullable) {
        const isNull = curVal === null || curVal === undefined;
        const toggle = paramsEl(
            "label",
            { class: "params-null-toggle" },
            paramsEl("input", {
                type: "checkbox",
                checked: isNull ? true : false,
                onchange: function () {
                    if (this.checked) {
                        paramsDeepSet(_paramsCurrentValues, path, null);
                    } else {
                        const fallback =
                            defVal !== null && defVal !== undefined ? defVal : paramsGetTypeDefault(primaryType);
                        paramsDeepSet(_paramsCurrentValues, path, JSON.parse(JSON.stringify(fallback)));
                    }
                    buildParamsForm();
                    updateParamsPreview();
                },
            }),
            "null"
        );
        inputWrap.appendChild(toggle);
        if (isNull) {
            field.appendChild(inputWrap);
            return field;
        }
    }

    if (node.enum) {
        const select = paramsEl("select", {
            onchange: function () {
                let v = this.value;
                if (v === "__null__") v = null;
                else if (primaryType === "number" || primaryType === "integer") v = Number(v);
                paramsDeepSet(_paramsCurrentValues, path, v);
                paramsMarkChanged(field, path);
            },
        });
        if (nullable) select.appendChild(paramsEl("option", { value: "__null__" }, "(null)"));
        for (const opt of node.enum) {
            if (opt === null) continue;
            const option = paramsEl("option", { value: String(opt) }, String(opt));
            if (String(curVal) === String(opt)) option.selected = true;
            select.appendChild(option);
        }
        inputWrap.appendChild(select);
    } else if (primaryType === "boolean") {
        const select = paramsEl("select", {
            onchange: function () {
                paramsDeepSet(_paramsCurrentValues, path, this.value === "true");
                paramsMarkChanged(field, path);
            },
        });
        [true, false].forEach((val) => {
            const opt = paramsEl("option", { value: String(val) }, String(val));
            if (curVal === val) opt.selected = true;
            select.appendChild(opt);
        });
        inputWrap.appendChild(select);
    } else if (primaryType === "array" || primaryType === "object") {
        const ta = document.createElement("textarea");
        ta.value = JSON.stringify(curVal ?? (primaryType === "array" ? [] : {}), null, 2);
        ta.addEventListener("input", function () {
            try {
                const parsed = JSON.parse(this.value);
                paramsDeepSet(_paramsCurrentValues, path, parsed);
                this.style.borderColor = "";
                paramsMarkChanged(field, path);
            } catch {
                this.style.borderColor = "var(--color-failed)";
            }
        });
        inputWrap.appendChild(ta);
    } else if (primaryType === "integer" || primaryType === "number") {
        const attrs = {
            type: "number",
            value: curVal != null ? String(curVal) : "",
            step: primaryType === "integer" ? "1" : "any",
            oninput: function () {
                const v = primaryType === "integer" ? parseInt(this.value, 10) : parseFloat(this.value);
                if (!isNaN(v)) {
                    paramsDeepSet(_paramsCurrentValues, path, v);
                    paramsMarkChanged(field, path);
                }
            },
        };
        if (node.minimum != null) attrs.min = String(node.minimum);
        if (node.maximum != null) attrs.max = String(node.maximum);
        inputWrap.appendChild(paramsEl("input", attrs));
    } else {
        inputWrap.appendChild(
            paramsEl("input", {
                type: "text",
                value: curVal != null ? String(curVal) : "",
                oninput: function () {
                    paramsDeepSet(_paramsCurrentValues, path, this.value);
                    paramsMarkChanged(field, path);
                },
            })
        );
    }

    field.appendChild(inputWrap);
    return field;
}

function buildParamsSection(label, schemaNode, path) {
    const node = paramsResolveRef(schemaNode);
    const wrapper = paramsEl("div", { class: "params-section collapsed" });

    const header = paramsEl(
        "div",
        { class: "params-section-header" },
        paramsEl("span", { class: "params-section-arrow" }, "▼"),
        document.createTextNode(label)
    );
    if (node.description) {
        header.appendChild(paramsEl("span", { class: "params-section-desc" }, node.description));
    }
    header.addEventListener("click", () => wrapper.classList.toggle("collapsed"));
    wrapper.appendChild(header);

    const body = paramsEl("div", { class: "params-section-body" });
    const nodeTypes = Array.isArray(node.type) ? node.type : [node.type];
    if (nodeTypes.includes("object") && node.properties) {
        for (const [k, v] of Object.entries(node.properties)) {
            const resolved = paramsResolveRef(v);
            const childPath = [...path, k];
            const resolvedTypes = Array.isArray(resolved.type) ? resolved.type : [resolved.type];
            if (resolvedTypes.includes("object") && resolved.properties) {
                body.appendChild(buildParamsSection(k, resolved, childPath));
            } else {
                body.appendChild(buildParamsField(k, resolved, childPath));
            }
        }
    }
    wrapper.appendChild(body);
    return wrapper;
}

function buildParamsForm() {
    const formRoot = document.getElementById("params-form-root");
    if (!formRoot) return;
    formRoot.innerHTML = "";

    const schemaEntries = Object.entries(_paramsSchema.properties);
    const ordered = [
        ...PARAMS_SECTION_ORDER.filter((k) => _paramsSchema.properties[k]).map((k) => [k, _paramsSchema.properties[k]]),
        ...schemaEntries.filter(([k]) => !PARAMS_SECTION_ORDER.includes(k)),
    ];
    for (const [key, propSchema] of ordered) {
        const resolved = paramsResolveRef(propSchema);
        formRoot.appendChild(buildParamsSection(key, resolved, [key]));
    }

    const chk = document.getElementById("params-chk-only-changed");
    if (chk && chk.checked) {
        formRoot.querySelectorAll(".params-field").forEach((f) => {
            f.style.display = f.classList.contains("changed") ? "" : "none";
        });
    }
}

function renderParamsEditorShell() {
    const readmeUrl = "https://github.com/dandi-compute/code#contributing-non-code-files";
    const codeRepoUrl = "https://github.com/dandi-compute/code";
    const paramsDir = "src/dandi_compute_code/aind_ephys_pipeline/params/";
    const registryFile = "src/dandi_compute_code/aind_ephys_pipeline/registries/registered_params.json";

    return `<div class="params-editor">
    <div class="params-editor-split">
        <div class="params-editor-left">
            <div class="params-toolbar">
                <button class="params-toolbar-btn" id="params-btn-defaults">Restore</button>
                <button class="params-toolbar-btn" id="params-btn-collapse">Collapse All</button>
                <button class="params-toolbar-btn" id="params-btn-expand">Expand All</button>
                <label class="params-toolbar-toggle">
                    <input type="checkbox" id="params-chk-only-changed"> Show only changed
                </label>
            </div>
            <div id="params-form-root"></div>
        </div>
        <div class="params-editor-right">
            <div class="params-output-header">
                <span class="params-output-title">Generated JSON</span>
                <div class="params-toolbar">
                    <button class="params-toolbar-btn" id="params-btn-download">Download</button>
                    <button class="params-toolbar-btn" id="params-btn-copy">Copy</button>
                    <label class="params-toolbar-file-label">Import…<input type="file" id="params-file-import" accept=".json"></label>
                </div>
            </div>
            <pre id="params-output" class="params-output-pre"></pre>
        </div>
    </div>
    <div class="params-instructions">
        <div class="params-instructions-title">How to submit your params file</div>
        <div class="params-instructions-step">
            <span class="params-instructions-num">2</span>
            <span>Customize your parameters using the form above, then click <strong>Download</strong> and rename the file to a short descriptive name (e.g.&nbsp;<code>my-params.json</code>).</span>
        </div>
        <div class="params-instructions-step">
            <span class="params-instructions-num">1</span>
            <span>Login to GitHub (or <a href="https://github.com/join" target="_blank" rel="noopener">create a free account</a>).</span>
        </div>
        <div class="params-instructions-step">
            <span class="params-instructions-num">3</span>
            <span>Open the <a href="${e(codeRepoUrl)}" target="_blank" rel="noopener">dandi-compute/code</a> repository and click <strong>Fork</strong> to create your own copy.</span>
        </div>
        <div class="params-instructions-step">
            <span class="params-instructions-num">4</span>
            <span>In your fork, upload your JSON file to <code>${e(paramsDir)}</code> and register it in <code>${e(registryFile)}</code> following the <a href="${e(readmeUrl)}" target="_blank" rel="noopener">contributing instructions in the README</a>.</span>
        </div>
        <div class="params-instructions-step">
            <span class="params-instructions-num">5</span>
            <span>Open a Pull Request from your fork back to <code>dandi-compute/code</code>. A maintainer will review and merge your file.</span>
        </div>
    </div>
</div>`;
}

function initParamsEditorUI() {
    buildParamsForm();
    updateParamsPreview();

    document.getElementById("params-btn-defaults").addEventListener("click", () => {
        _paramsCurrentValues = JSON.parse(JSON.stringify(_paramsDefaultValues));
        buildParamsForm();
        updateParamsPreview();
    });

    document.getElementById("params-btn-collapse").addEventListener("click", () => {
        document
            .getElementById("params-form-root")
            .querySelectorAll(".params-section")
            .forEach((s) => s.classList.add("collapsed"));
    });

    document.getElementById("params-btn-expand").addEventListener("click", () => {
        document
            .getElementById("params-form-root")
            .querySelectorAll(".params-section")
            .forEach((s) => s.classList.remove("collapsed"));
    });

    document.getElementById("params-chk-only-changed").addEventListener("change", function () {
        document
            .getElementById("params-form-root")
            .querySelectorAll(".params-field")
            .forEach((f) => {
                f.style.display = this.checked ? (f.classList.contains("changed") ? "" : "none") : "";
            });
    });

    document.getElementById("params-btn-download").addEventListener("click", () => {
        const json = JSON.stringify(_paramsCurrentValues, null, 4);
        const blob = new Blob([json], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "params.json";
        a.click();
        URL.revokeObjectURL(url);
    });

    document.getElementById("params-btn-copy").addEventListener("click", () => {
        const json = JSON.stringify(_paramsCurrentValues, null, 4);
        navigator.clipboard.writeText(json).then(() => {
            const btn = document.getElementById("params-btn-copy");
            btn.textContent = "Copied!";
            setTimeout(() => (btn.textContent = "Copy"), 1500);
        });
    });

    document.getElementById("params-file-import").addEventListener("change", function () {
        const file = this.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = () => {
            try {
                const text = stripTrailingCommas(reader.result);
                _paramsCurrentValues = JSON.parse(text);
                buildParamsForm();
                updateParamsPreview();
            } catch (err) {
                alert("Invalid JSON: " + err.message);
            }
        };
        reader.readAsText(file);
        this.value = "";
    });
}

async function initParamsEditor() {
    const schemaResp = await cachedFetch(PARAMS_SCHEMA_URL);
    if (!schemaResp.ok) {
        throw new Error(`Failed to load parameter schema (HTTP ${schemaResp.status}).`);
    }
    _paramsSchema = await schemaResp.json();

    try {
        const defResp = await cachedFetch(PARAMS_PLACEHOLDER_URL);
        if (defResp.ok) {
            const text = stripTrailingCommas(await defResp.text());
            _paramsDefaultValues = JSON.parse(text);
        } else {
            _paramsDefaultValues = paramsBuildDefaults(_paramsSchema);
        }
    } catch {
        _paramsDefaultValues = paramsBuildDefaults(_paramsSchema);
    }

    _paramsCurrentValues = JSON.parse(JSON.stringify(_paramsDefaultValues));

    const pageContent = document.querySelector(".page-content");
    if (pageContent) pageContent.classList.add("params-page");

    document.getElementById("runs").innerHTML = renderParamsEditorShell();
    showDiffResults();
    initParamsEditorUI();
}

/* ─── Page state helpers ────────────────────────────────────── */
function showLoading() {
    document.getElementById("loading").style.display = "";
    document.getElementById("error").style.display = "none";
    document.getElementById("filter-banner").style.display = "none";
    document.getElementById("summary").style.display = "none";
    document.getElementById("runs").style.display = "none";
}

function showError(msg) {
    document.getElementById("loading").style.display = "none";
    const el = document.getElementById("error");
    el.style.display = "";
    el.innerHTML = `<p class="error-icon">⚠</p><p>${e(msg)}</p>`;
}

function showResults() {
    document.getElementById("loading").style.display = "none";
    document.getElementById("summary").style.display = "";
    document.getElementById("layout-bar").style.display = "";
    document.getElementById("runs").style.display = "";
}

function showDiffResults() {
    document.getElementById("loading").style.display = "none";
    document.getElementById("error").style.display = "none";
    document.getElementById("filter-banner").style.display = "none";
    document.getElementById("summary").style.display = "none";
    document.getElementById("layout-bar").style.display = "none";
    document.getElementById("runs").style.display = "";
}

/* ─── Utility ───────────────────────────────────────────────── */
function e(str) {
    return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/* ─── Queue data loader ─────────────────────────────────────── */
// Fetches, processes, and renders the queue state for the current view.
// Called on initial page load and when the refresh button is clicked.
async function loadQueueData() {
    showLoading();
    renderFilterBanner(parseFilter(), []);

    try {
        const entries = await fetchQueueState();
        const runs = parseQueueEntries(entries);

        if (runs.length === 0) {
            renderFilterBanner(parseFilter(), []);
            showError("No pipeline runs found in the queue.");
            return;
        }

        // Fetch trace.txt, dataset_description.json and DANDI asset IDs for all runs in parallel.
        // Skip trace/dataset fetching for queued runs (no logs yet).
        // Fetch all SLURM logs at once (single git-trees API call) to avoid per-run rate limits.
        const slurmLogsByRun = await fetchAllSlurmLogs();
        const fetchIfLogs = (hasLogs, fn) => (hasLogs ? fn() : Promise.resolve(null));
        const runsWithStatus = await Promise.all(
            runs.map(async (run) => {
                const [text, datasetDesc, dandiResult, vizData] = await Promise.all([
                    fetchIfLogs(run.hasLogs, () => fetchTraceText(run.path)),
                    fetchIfLogs(run.hasLogs, () => fetchDatasetDescription(run.path)),
                    fetchDandiAssetId(run.dandisetId, run.subject, run.session),
                    run.hasOutput ? fetchVisualizationData(run.path) : Promise.resolve(null),
                ]);
                const parsed = parseTrace(text);
                const assetId = dandiResult?.assetId ?? null;
                const inSourcedata = dandiResult?.inSourcedata ?? false;
                const generatedBy = Array.isArray(datasetDesc?.GeneratedBy) ? datasetDesc.GeneratedBy : [];
                // Determine status from JSONL flags:
                //   has_output=true  → success (use trace status for task detail)
                //   has_logs=false && has_code=true → queued (not yet started)
                //   otherwise        → failed
                const status = run.hasOutput
                    ? parsed.status !== "unknown"
                        ? parsed.status
                        : "success"
                    : !run.hasLogs && run.hasCode
                      ? "queued"
                      : "failed";
                const failureStep = isFailedStatus(status) ? runFailureStep({ status, tasks: parsed.tasks }) : null;
                return {
                    ...run,
                    ...parsed,
                    assetId,
                    inSourcedata,
                    generatedBy,
                    vizData,
                    status,
                    failureStep,
                    slurmLogs: slurmLogsByRun.get(run.path) ?? [],
                };
            })
        );

        const sortedRuns = sortRuns(runsWithStatus, parseSortMode());

        // Scope runs by view mode:
        //   tests page  → show only TEST_DANDISETS entries
        //   main page   → hide TEST_DANDISETS entries
        const runsInScope =
            _viewMode === "tests"
                ? sortedRuns.filter((r) => TEST_DANDISETS.has(r.dandisetId))
                : sortedRuns.filter((r) => !TEST_DANDISETS.has(r.dandisetId));

        const filter = parseFilter();
        const isFiltered = !!(
            filter.dandisetId ||
            filter.subject ||
            filter.session ||
            filter.pipelineVersion ||
            filter.paramsType ||
            filter.configType ||
            filter.dandiCodebaseHash ||
            filter.failureStep ||
            filter.status
        );
        const filteredRuns = applyFilter(runsInScope, filter);

        if (isFiltered && filteredRuns.length === 0) {
            renderFilterBanner(filter, runsInScope);
            showError("No pipeline runs match the current filter.");
            return;
        }

        // Show the full summary for the in-scope runs; when a specific filter is
        // active show only the matching subset.
        const runsForSummary = isFiltered ? filteredRuns : runsInScope;
        renderSummary(runsForSummary);
        renderFilterBanner(filter, runsInScope);
        _filteredRuns = filteredRuns;
        _layoutMode = parseLayoutMode();
        _sortMode = parseSortMode();
        _sortDirection = parseSortDirection();
        updateLayoutModeUrl(_layoutMode);
        updateSortModeUrl(_sortMode);
        updateSortDirectionUrl(_sortDirection);
        document.getElementById("runs").innerHTML =
            _layoutMode === "flat" ? renderFlatList(filteredRuns) : renderDandisets(sortRuns(filteredRuns));
        initInlineHtmlFrames();
        initLayoutToggle();
        showResults();
    } catch (err) {
        renderFilterBanner(parseFilter(), []);
        showError(err.message || "An unexpected error occurred.");
    }
}

/* ─── Main ──────────────────────────────────────────────────── */
async function init() {
    _viewMode = parseViewMode();
    initTheme();
    initModal();
    syncTopNav(_viewMode);
    if (_viewMode === "compare") {
        setPageCopy(
            "AIND Pipeline Diffs Index",
            'Assembled comparison links for the <a href="https://github.com/CodyCBakerPhD/aind-ephys-pipeline" target="_blank" rel="noopener">pipeline repository</a> and registered parameter or configuration definitions.'
        );
    }
    if (_viewMode === "params") {
        setPageCopy(
            "Register New Params File",
            'Create a custom parameter file for the <a href="https://github.com/CodyCBakerPhD/aind-ephys-pipeline" target="_blank" rel="noopener">AIND Ephys Pipeline</a> and submit it for use in the compute pipeline.'
        );
    }

    showLoading();
    if (_viewMode !== "params") {
        await loadAindPipelineRegistries();
    }
    if (_viewMode === "compare") {
        try {
            const entries = await fetchQueueState();
            const runs = parseQueueEntries(entries);
            const pipelineEntries = await buildPipelineCompareEntries(runs);
            const pipelinePairs = await buildPipelineDiffPairs(runs);
            const paramsEntries = uniqueRegistryEntries(PARAMS_REGISTRY).map((entry) => ({
                key: entry.alias,
                alias: entry.alias,
                sourceUrl: codeRepoBlobUrl(`src/dandi_compute_code/aind_ephys_pipeline/params/${entry.path}`),
            }));
            const paramsPairs = await buildParamsDiffPairs();
            const configEntries = uniqueRegistryEntries(CONFIG_REGISTRY).map((entry) => ({
                key: entry.alias,
                alias: entry.alias,
                sourceUrl: codeRepoBlobUrl(`src/dandi_compute_code/aind_ephys_pipeline/configs/${entry.path}`),
            }));
            const configPairs = await buildConfigDiffPairs();
            const diffData = {
                pipelineEntries,
                pipelinePairs,
                pipelinePairMap: new Map(
                    pipelinePairs.map((pair) => [`${pair.baseVersion}\x00${pair.headVersion}`, pair.compareUrl])
                ),
                paramsEntries,
                paramsPairs,
                paramsPairMap: new Map(paramsPairs.map((pair) => [`${pair.baseAlias}\x00${pair.headAlias}`, pair])),
                configEntries,
                configPairs,
                configPairMap: new Map(configPairs.map((pair) => [`${pair.baseAlias}\x00${pair.headAlias}`, pair])),
            };
            document.getElementById("runs").innerHTML = renderDiffPage(diffData);
            showDiffResults();
        } catch (err) {
            showError(err.message || "An unexpected error occurred.");
        }
        return;
    }
    if (_viewMode === "params") {
        try {
            await initParamsEditor();
        } catch (err) {
            showError(err.message || "Failed to load the parameter schema.");
        }
        return;
    }
    await loadQueueData();
}

document.addEventListener("DOMContentLoaded", init);

if (typeof module !== "undefined" && module.exports) {
    module.exports = {
        applyFilter,
        buildRunPath,
        classifyFailedTaskStep,
        clearQueueStateCache,
        fetchQueueState,
        fetchSlurmLogs,
        fetchAllSlurmLogs,
        fetchVisualizationData,
        initModal,
        initLayoutToggle,
        loadQueueData,
        openHtmlModal,
        neurosiftBlobUrl,
        neurosiftDandisetUrl,
        neurosiftSessionUrl,
        parseQueueEntries,
        parseLayoutMode,
        parseSortDirection,
        parseSortMode,
        parseRunPath,
        parseTrace,
        parseViewMode,
        syncTopNav,
        renderDandisets,
        buildPipelineDiffPairs,
        collectJsonDiffs,
        collectTextDiffs,
        loadAindPipelineRegistries,
        normalizeConfigHash,
        normalizeRegistryEntries,
        renderParamsGroup,
        renderDiffPage,
        renderFilterBanner,
        renderSummary,
        renderFlatList,
        renderRegistryLink,
        renderVisualizationSection,
        runFailureStep,
        sortRuns,
        uniquePipelineEntries,
        uniqueRegistryEntries,
        showError,
        showDiffResults,
        showLoading,
        showResults,
        TEST_DANDISETS,
        DANDISET_SUBJECT_DEFAULTS,
        resolveSubject,
        treeUrl,
    };
}
