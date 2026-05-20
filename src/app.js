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
const REGISTRY_FALLBACK_ALIAS_PRIORITY = 1;
const MIN_SHORT_COMMIT_HASH_LENGTH = 6;
const FULL_COMMIT_HASH_LENGTH = 40;
const DANDI_CODE_REPO_PATTERN = /github\.com\/dandi-compute\/code(?:\/|$)/;
const ROOT_DIFF_PATH_LABEL = "(root)";
const COMMIT_HASH_PATTERN = new RegExp(`^[0-9a-f]{${MIN_SHORT_COMMIT_HASH_LENGTH},${FULL_COMMIT_HASH_LENGTH}}$`, "i");
const PARAMS_REGISTRY = [
    { alias: "deterministic", md5: "4af6a25e20e376c81895ce9350a9cbd4", path: "name-deterministic.json", priority: 2 },
    { alias: "default", md5: "4af6a25e20e376c81895ce9350a9cbd4", path: "name-deterministic.json", priority: 0 },
    { alias: "original", md5: "98fd947595f60b65812a4b0ea29b7141", path: "name-original.json", priority: 1 },
    { alias: "all+channels", md5: "e6a0e8603a19444c0006a1a4d279047a", path: "name-all+channels.json", priority: 1 },
    {
        alias: "no+motion",
        md5: "0d25c9ddf35d3653a693f63b7418c598",
        path: "name-no+motion_revision-1.json",
        priority: 1,
    },
    {
        alias: "no+motion_v0",
        md5: "aa073df2761666edbf0bb66cab85ca4c",
        path: "name-no+motion_revision-0.json",
        priority: 1,
    },
];
const CONFIG_REGISTRY = [
    { alias: "v1", md5: "0d4bf36ddb61418ae7714e7d6e5ff8b8", path: "name-mit+engaging_revision-1.config", priority: 2 },
    {
        alias: "default",
        md5: "0d4bf36ddb61418ae7714e7d6e5ff8b8",
        path: "name-mit+engaging_revision-1.config",
        priority: 0,
    },
    { alias: "v0", md5: "6568ddacdedabc7b855769340ed8874f", path: "name-mit+engaging_revision-0.config", priority: 1 },
];
/* Dandisets hosted on the sandbox archive instead of the production archive */
const SANDBOX_DANDISETS = new Set(["214527"]);
/* Dandisets used for internal testing – hidden from the main view and moved to
   the dedicated Tests page (?view=tests).  Currently all sandbox dandisets are
   also test dandisets, but the two concepts are kept separate so they can
   diverge independently in the future. */
const TEST_DANDISETS = new Set(["214527"]);

/* Module-level view mode ("tests" | null), set during init */
let _viewMode = null;

/* Module-level layout mode ("tree" | "flat"), toggled by the layout bar */
let _layoutMode = "tree";
/* Cached filtered runs for re-rendering on layout toggle */
let _filteredRuns = [];

function parseViewMode() {
    return new URLSearchParams(window.location.search).get("view") ?? null;
}

function parseLayoutMode() {
    const layout = new URLSearchParams(window.location.search).get("layout");
    if (layout === "flat" || layout === "tree") return layout;
    return localStorage.getItem("layoutMode") === "flat" ? "flat" : "tree";
}

function updateLayoutModeUrl(mode) {
    if (mode !== "flat" && mode !== "tree") return;
    const params = new URLSearchParams(window.location.search);
    params.set("layout", mode);
    const qs = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${qs ? `?${qs}` : ""}${window.location.hash}`);
}

function codeRepoBlobUrl(path) {
    return `${CODE_REPO_URL}/blob/main/${path.split("/").map(encodeURIComponent).join("/")}`;
}

function codeRepoRawUrl(path) {
    return `https://raw.githubusercontent.com/dandi-compute/code/main/${path.split("/").map(encodeURIComponent).join("/")}`;
}

function dandiBaseUrl(dandisetId) {
    return SANDBOX_DANDISETS.has(dandisetId) ? "https://sandbox.dandiarchive.org" : "https://dandiarchive.org";
}
function dandiApiBaseUrl(dandisetId) {
    return SANDBOX_DANDISETS.has(dandisetId) ? "https://api-staging.dandiarchive.org" : "https://api.dandiarchive.org";
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

function applyFilter(runs, filter) {
    return runs.filter((r) => {
        if (filter.dandisetId && r.dandisetId !== filter.dandisetId) return false;
        if (filter.subject && r.subject !== filter.subject) return false;
        if (filter.session && r.session !== filter.session) return false;
        if (filter.pipelineVersion && r.pipelineVersion !== filter.pipelineVersion) return false;
        if (!matchesResolvedOrRawValue(filter.paramsType, runParamsType(r), r.paramsProfile)) return false;
        if (!matchesResolvedOrRawValue(filter.configType, runConfigType(r), r.configHash)) return false;
        if (filter.dandiCodebaseHash && runDandiCodebaseHash(r) !== filter.dandiCodebaseHash) return false;
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
    if (_viewMode === "tests") sp.set("view", "tests");
    if (params.dandiset) sp.set("dandiset", params.dandiset);
    if (params.subject) sp.set("subject", params.subject);
    if (params.session) sp.set("session", params.session);
    if (params.pipelineVersion) sp.set("version", params.pipelineVersion);
    if (params.paramsType) sp.set("params", params.paramsType);
    if (params.configType) sp.set("config", params.configType);
    if (params.dandiCodebaseHash) sp.set("codebaseHash", params.dandiCodebaseHash);
    if (params.failureStep) sp.set("failureStep", params.failureStep);
    const qs = sp.toString();
    return qs ? `?${qs}` : "./";
}

const FILTER_VALUE_COLLATOR = new Intl.Collator();
const uniqueSortedValues = (items) => [...new Set(items.filter(Boolean))].sort(FILTER_VALUE_COLLATOR.compare);
const FAILURE_STEP_FILTER_OPTIONS = ["exclude-job-dispatch", "pre-processing", "post-processing"];

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
    const isFiltered = !!(
        filter.dandisetId ||
        filter.subject ||
        filter.session ||
        filter.pipelineVersion ||
        filter.paramsType ||
        filter.configType ||
        filter.dandiCodebaseHash ||
        filter.failureStep
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
    <span class="tests-page-desc">Showing internal test runs only (Dandiset 214527)</span>
    <a class="tests-back-link" href="./">← Back to main</a>
</div>`
            : "";

    const viewHiddenInput = _viewMode === "tests" ? `<input type="hidden" name="view" value="tests">` : "";
    const layoutHiddenInput = `<input type="hidden" name="layout" value="${layoutMode}">`;
    const clearAllParams = new URLSearchParams();
    clearAllParams.set("layout", layoutMode);
    if (_viewMode === "tests") clearAllParams.set("view", "tests");
    const clearAllHref = `?${clearAllParams.toString()}`;

    banner.innerHTML = `
${testsPageHtml}<div class="filter-banner-main">
    <span class="filter-banner-label">Filter runs:</span>
    <form class="filter-form" method="get" action="">
        ${viewHiddenInput}
        ${layoutHiddenInput}
        ${renderFilterInput("dandiset", "Dandiset", filter.dandisetId, dandisets, narrowUrl({ pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("subject", "Subject", filter.subject, subjects, narrowUrl({ dandiset: filter.dandisetId, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("session", "Session", filter.session, sessions, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("version", "Version", filter.pipelineVersion, versions, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("params", "Params Type", filter.paramsType, paramsTypes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("config", "Config Type", filter.configType, configTypes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, dandiCodebaseHash: filter.dandiCodebaseHash, failureStep: filter.failureStep }))}
        ${renderFilterInput("codebaseHash", "DANDI Codebase Hash", filter.dandiCodebaseHash, dandiCodebaseHashes, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, failureStep: filter.failureStep }))}
        ${renderFilterInput("failureStep", "Failure Step", filter.failureStep, failureSteps, narrowUrl({ dandiset: filter.dandisetId, subject: filter.subject, session: filter.session, pipelineVersion: filter.pipelineVersion, paramsType: filter.paramsType, configType: filter.configType, dandiCodebaseHash: filter.dandiCodebaseHash }))}
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
        const encodedPath = runPath.split("/").map(encodeURIComponent).join("/");
        const vizDirUrl = `${GITHUB_API_BASE}/contents/${encodedPath}/visualization?ref=${BRANCH}`;
        const vizDirResp = await cachedFetch(vizDirUrl);
        if (!vizDirResp.ok) return null;
        const vizDirItems = await vizDirResp.json();
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
                        url: cdnUrl(`${runPath}/visualization/${dir.name}/${f.path}`),
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
// Build a run directory path from a JSONL queue entry.
// With session:    derivatives/dandiset-{id}/sub-{subject}/ses-{session}/pipeline-{pipeline}/version-{version}_params-{params}_config-{config}_attempt-{attempt}
// Without session: derivatives/dandiset-{id}/sub-{subject}/pipeline-{pipeline}/version-{version}_params-{params}_config-{config}_attempt-{attempt}
function buildRunPath(entry) {
    const parts = ["derivatives", `dandiset-${entry.dandiset_id}`, `sub-${entry.subject}`];
    if (entry.session !== null && entry.session !== undefined) {
        parts.push(`ses-${entry.session}`);
    }
    parts.push(`pipeline-${entry.pipeline}`);
    parts.push(`version-${entry.version}_params-${entry.params}_config-${entry.config}_attempt-${entry.attempt}`);
    return parts.join("/");
}

/* ─── Queue entry parsing ───────────────────────────────────── */
// Convert raw JSONL entries from the queue state file into run objects.
function parseQueueEntries(entries) {
    return entries.map((entry) => ({
        path: buildRunPath(entry),
        dandisetId: entry.dandiset_id,
        subject: entry.subject,
        session: entry.session ?? null,
        pipelineName: entry.pipeline,
        pipelineVersion: entry.version,
        paramsProfile: entry.params,
        configHash: entry.config,
        attempt: entry.attempt,
        hasCode: entry.has_code,
        hasOutput: entry.has_output,
        hasLogs: entry.has_logs,
        contentHash: entry.content_hash ?? null,
        runDate: null,
    }));
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
                    configHash = beforeAttempt.slice(configIndex + configMarker.length);
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
        runDate: null,
        attempt,
    };
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

    document.getElementById("summary").innerHTML = `
        <div class="summary-stats">
            <div class="stat-item">
                <span class="stat-value">${total}</span>
                <span class="stat-label">Total Runs</span>
            </div>
            <div class="stat-item stat-success">
                <span class="stat-value">${success}</span>
                <span class="stat-label">Successful</span>
            </div>
            <div class="stat-item stat-failed">
                <span class="stat-value">${failed}</span>
                <span class="stat-label">Failed</span>
            </div>
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

/* Build a GitHub blob URL for a repo file path */
function blobUrl(filePath) {
    return `https://github.com/${OWNER}/${REPO}/blob/${BRANCH}/${filePath.split("/").map(encodeURIComponent).join("/")}`;
}

/* Build a Neurosift URL for a DANDI asset (legacy: via DANDI API asset download URL) */
function neurosiftUrl(dandisetId, assetId) {
    const isSandbox = SANDBOX_DANDISETS.has(dandisetId);
    const assetDownloadUrl = `${dandiApiBaseUrl(dandisetId)}/api/assets/${assetId}/download/`;
    const url = `https://neurosift.app/nwb?url=${encodeURIComponent(assetDownloadUrl)}&dandisetId=${encodeURIComponent(dandisetId)}&dandisetVersion=draft`;
    return isSandbox ? `${url}&staging=true` : url;
}

/* Build a Neurosift NWB URL from a DANDI S3 content hash */
function neurosiftBlobUrl(contentHash) {
    const blobFileUrl = `https://dandiarchive.s3.amazonaws.com/blobs/${contentHash.slice(0, 3)}/${contentHash.slice(3, 6)}/${contentHash}`;
    const neurosiftUrl_ = `https://neurosift.app/nwb?url=${encodeURIComponent(blobFileUrl)}`;
    console.log("[neurosiftBlobUrl] contentHash:", contentHash);
    console.log("[neurosiftBlobUrl] S3 blob URL:", blobFileUrl);
    console.log("[neurosiftBlobUrl] Neurosift URL:", neurosiftUrl_);
    return neurosiftUrl_;
}

/* Build the best available Neurosift NWB URL: prefer blob URL (no API call), fall back to asset download URL */
function neurosiftSessionUrl(dandisetId, contentHash, assetId) {
    if (contentHash) {
        console.log("[neurosiftSessionUrl] path=blob  dandisetId:", dandisetId, "contentHash:", contentHash);
        return neurosiftBlobUrl(contentHash);
    }
    if (assetId) {
        const url = neurosiftUrl(dandisetId, assetId);
        console.log("[neurosiftSessionUrl] path=asset dandisetId:", dandisetId, "assetId:", assetId, "url:", url);
        return url;
    }
    console.log(
        "[neurosiftSessionUrl] path=null  dandisetId:",
        dandisetId,
        "contentHash:",
        contentHash,
        "assetId:",
        assetId
    );
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
    const buttonLogs = logFiles.filter((f) => !INLINE_REPORT_FILES.has(f));
    const hasLogs = buttonLogs.length > 0;
    const hasInline = inlineLogs.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;
    const hasSourceVersions = run.generatedBy && run.generatedBy.length > 0;
    const hasViz = run.vizData && run.vizData.length > 0;

    return `
<div class="run-entry ${sc}">
    <div class="run-entry-header">
        <span class="status-badge ${sc}">${slbl}</span>
        ${run.runDate ? `<span class="run-date">${e(run.runDate)}</span><span class="run-sep">·</span>` : ""}
        <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
        <a class="run-entry-github-link" href="${e(blobUrl(run.path))}" target="_blank" rel="noopener">↗ GitHub</a>
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

async function fetchRegistryJson(path) {
    const resp = await cachedFetch(codeRepoRawUrl(`src/dandi_compute_code/aind_ephys_pipeline/params/${path}`));
    if (!resp.ok) {
        throw new Error(`Failed to load registered params file ${path} (HTTP ${resp.status}).`);
    }
    return resp.json();
}

async function buildParamsDiffPairs() {
    const paramsEntries = uniqueRegistryEntries(PARAMS_REGISTRY);
    const paramsWithJson = await Promise.all(
        paramsEntries.map(async (entry) => ({
            ...entry,
            sourceUrl: codeRepoBlobUrl(`src/dandi_compute_code/aind_ephys_pipeline/params/${entry.path}`),
            json: await fetchRegistryJson(entry.path),
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
    const buttonLogs = logFiles.filter((f) => !INLINE_REPORT_FILES.has(f));
    const hasLogs = buttonLogs.length > 0;
    const hasInline = inlineLogs.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;
    const hasSourceVersions = run.generatedBy && run.generatedBy.length > 0;
    const hasViz = run.vizData && run.vizData.length > 0;

    const location = run.inSourcedata ? `sourcedata/sub-${run.subject}` : `sub-${run.subject}`;
    const subjectUrl = `${dandiBaseUrl(run.dandisetId)}/dandiset/${e(run.dandisetId)}/draft/files?location=${e(location)}`;
    const sessionHref = neurosiftSessionUrl(run.dandisetId, run.contentHash, run.assetId);
    const sessionContextHtml =
        run.session !== null
            ? sessionHref
                ? `<span class="run-sep">·</span><a class="flat-ctx-link" href="${e(sessionHref)}" target="_blank" rel="noopener">Ses:&nbsp;<strong>${e(run.session)}</strong></a>`
                : `<span class="run-sep">·</span><span class="flat-ctx-text">Ses:&nbsp;<strong>${e(run.session)}</strong></span>`
            : "";

    return `
<div class="run-entry flat-run-entry ${sc}">
    <div class="run-entry-header flat-run-header">
        <a class="dandi-view-link" href="${dandiBaseUrl(run.dandisetId)}/dandiset/${e(run.dandisetId)}" target="_blank" rel="noopener">DANDI&nbsp;↖</a>
        <span class="status-badge ${sc}">${slbl}</span>
        <span class="flat-run-context">
            <a class="flat-ctx-link" href="${e(neurosiftDandisetUrl(run.dandisetId))}" target="_blank" rel="noopener">Dandiset&nbsp;${e(run.dandisetId)}</a>
            <span class="run-sep">·</span>
            <a class="flat-ctx-link" href="${e(subjectUrl)}" target="_blank" rel="noopener">Sub:&nbsp;<strong>${e(run.subject)}</strong></a>
            ${sessionContextHtml}
            <span class="run-sep">·</span>
            <span class="flat-ctx-text">${renderRegistryLink("Params", run.paramsProfile, PARAMS_REGISTRY, "params")}</span>
        </span>
        ${run.runDate ? `<span class="run-date">${e(run.runDate)}</span><span class="run-sep">·</span>` : ""}
        <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
        <a class="run-entry-github-link" href="${e(blobUrl(run.path))}" target="_blank" rel="noopener">↗ GitHub</a>
    </div>

    ${hasSourceVersions ? renderSourceVersionsSection(run.generatedBy) : ""}
    ${hasTasks ? renderTraceSection(run.tasks) : ""}
    ${hasViz ? renderVisualizationSection(run.vizData) : ""}
    ${hasLogs ? renderLogSection(run.path, buttonLogs) : ""}
    ${hasInline ? renderReportSection(run.path, inlineLogs) : ""}
</div>`;
}

function renderFlatList(runs) {
    return `<div class="flat-list">${runs.map(renderFlatRunEntry).join("")}</div>`;
}

/* ─── Layout toggle ─────────────────────────────────────────── */
function renderLayoutBar() {
    const isFlat = _layoutMode === "flat";
    return `<div class="layout-bar">
    <span class="layout-bar-label">View:</span>
    <button class="layout-btn${!isFlat ? " layout-btn-active" : ""}" data-layout="tree" aria-pressed="${!isFlat}">Tree</button>
    <button class="layout-btn${isFlat ? " layout-btn-active" : ""}" data-layout="flat" aria-pressed="${isFlat}">Flat</button>
</div>`;
}

function rerenderRuns() {
    document.getElementById("runs").innerHTML =
        _layoutMode === "flat" ? renderFlatList(_filteredRuns) : renderDandisets(_filteredRuns);
    initInlineHtmlFrames();
}

function initLayoutToggle() {
    const bar = document.getElementById("layout-bar");
    if (!bar) return;
    bar.innerHTML = renderLayoutBar();
    bar.addEventListener("click", (ev) => {
        const btn = ev.target.closest("[data-layout]");
        if (!btn) return;
        const mode = btn.dataset.layout;
        if (mode === _layoutMode) return;
        _layoutMode = mode;
        localStorage.setItem("layoutMode", mode);
        updateLayoutModeUrl(mode);
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

    // When a <details> containing inline frames is opened, request a fresh height
    // measurement after layout has settled (the iframe was hidden while collapsed).
    document.querySelectorAll("details").forEach((details) => {
        if (!details.querySelector("iframe[data-srcdoc-path]")) return;
        details.addEventListener("toggle", () => {
            if (!details.open) return;
            requestAnimationFrame(() => {
                details.querySelectorAll("iframe[data-srcdoc-path]").forEach((iframe) => {
                    if (iframe.contentWindow) {
                        // '*' is required: sandboxed srcdoc iframes have opaque ('null') origin,
                        // which is not a valid targetOrigin — only '*' reaches them.
                        // The message contains no sensitive data ({type:'requestHeight'} only).
                        iframe.contentWindow.postMessage({ type: "requestHeight" }, "*");
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
            iframe.srcdoc = patched;
        })
    );
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

/* ─── Main ──────────────────────────────────────────────────── */
async function init() {
    _viewMode = parseViewMode();
    initTheme();
    initModal();

    // Hide the "Tests" nav link when already on the tests page
    if (_viewMode === "tests") {
        const testsLink = document.querySelector(".site-tests-link");
        if (testsLink) testsLink.hidden = true;
    }
    if (_viewMode === "compare") {
        const diffsLink = document.querySelector(".site-diffs-link");
        if (diffsLink) diffsLink.hidden = true;
        setPageCopy(
            "AIND Pipeline Diffs Index",
            'Assembled comparison links for the <a href="https://github.com/CodyCBakerPhD/aind-ephys-pipeline" target="_blank" rel="noopener">pipeline repository</a> and registered parameter or configuration definitions .'
        );
    }

    showLoading();
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
            const diffData = {
                pipelineEntries,
                pipelinePairs,
                pipelinePairMap: new Map(
                    pipelinePairs.map((pair) => [`${pair.baseVersion}\x00${pair.headVersion}`, pair.compareUrl])
                ),
                paramsEntries,
                paramsPairs,
                paramsPairMap: new Map(paramsPairs.map((pair) => [`${pair.baseAlias}\x00${pair.headAlias}`, pair])),
            };
            document.getElementById("runs").innerHTML = renderDiffPage(diffData);
            showDiffResults();
        } catch (err) {
            showError(err.message || "An unexpected error occurred.");
        }
        return;
    }
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
                return { ...run, ...parsed, assetId, inSourcedata, generatedBy, vizData, status, failureStep };
            })
        );

        // Sort by attempt (descending); no run date available from JSONL
        runsWithStatus.sort((a, b) => b.attempt - a.attempt);

        // Scope runs by view mode:
        //   tests page  → show only TEST_DANDISETS entries
        //   main page   → hide TEST_DANDISETS entries
        const runsInScope =
            _viewMode === "tests"
                ? runsWithStatus.filter((r) => TEST_DANDISETS.has(r.dandisetId))
                : runsWithStatus.filter((r) => !TEST_DANDISETS.has(r.dandisetId));

        const filter = parseFilter();
        const isFiltered = !!(
            filter.dandisetId ||
            filter.subject ||
            filter.session ||
            filter.pipelineVersion ||
            filter.paramsType ||
            filter.configType ||
            filter.dandiCodebaseHash ||
            filter.failureStep
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
        updateLayoutModeUrl(_layoutMode);
        document.getElementById("runs").innerHTML =
            _layoutMode === "flat" ? renderFlatList(filteredRuns) : renderDandisets(filteredRuns);
        initInlineHtmlFrames();
        initLayoutToggle();
        showResults();
    } catch (err) {
        renderFilterBanner(parseFilter(), []);
        showError(err.message || "An unexpected error occurred.");
    }
}

document.addEventListener("DOMContentLoaded", init);

if (typeof module !== "undefined" && module.exports) {
    module.exports = {
        applyFilter,
        buildRunPath,
        classifyFailedTaskStep,
        fetchQueueState,
        fetchVisualizationData,
        initModal,
        initLayoutToggle,
        openHtmlModal,
        neurosiftBlobUrl,
        neurosiftDandisetUrl,
        neurosiftSessionUrl,
        parseQueueEntries,
        parseLayoutMode,
        parseRunPath,
        parseTrace,
        parseViewMode,
        renderDandisets,
        buildPipelineDiffPairs,
        collectJsonDiffs,
        renderParamsGroup,
        renderDiffPage,
        renderFilterBanner,
        renderFlatList,
        renderRegistryLink,
        renderVisualizationSection,
        runFailureStep,
        uniquePipelineEntries,
        uniqueRegistryEntries,
        showError,
        showDiffResults,
        showLoading,
        showResults,
        TEST_DANDISETS,
    };
}
