/* ─── Configuration ─────────────────────────────────────────── */
const OWNER = "dandi-compute";
const REPO = "001697";
const BRANCH = "main";
const CDN_BASE = `https://raw.githubusercontent.com/${OWNER}/${REPO}/${BRANCH}`;
const API_BASE = `https://api.github.com/repos/${OWNER}/${REPO}`;

const PIPELINE_REPO_URL = "https://github.com/CodyCBakerPhD/aind-ephys-pipeline";
/* Dandisets hosted on the sandbox archive instead of the production archive */
const SANDBOX_DANDISETS = new Set(["214527"]);
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
        failureStep: params.get("failureStep") ?? null,
    };
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
        if (filter.failureStep) {
            if (!isFailedStatus(r.status)) return false;
            const failedStep = r.failureStep;
            if (filter.failureStep === "exclude-job-dispatch") return failedStep !== "job-dispatch";
            return failedStep === filter.failureStep;
        }
        return true;
    });
}

/* Build a page URL with the given filter parameters */
function narrowUrl(params) {
    const sp = new URLSearchParams();
    if (params.dandiset) sp.set("dandiset", params.dandiset);
    if (params.subject) sp.set("subject", params.subject);
    if (params.session) sp.set("session", params.session);
    if (params.pipelineVersion) sp.set("version", params.pipelineVersion);
    if (params.failureStep) sp.set("failureStep", params.failureStep);
    const qs = sp.toString();
    return qs ? `?${qs}` : "./";
}

const FILTER_VALUE_COLLATOR = new Intl.Collator();
const uniqueSortedValues = (items) => [...new Set(items.filter(Boolean))].sort(FILTER_VALUE_COLLATOR.compare);
const FAILURE_STEP_FILTER_OPTIONS = ["exclude-job-dispatch", "pre-processing", "post-processing"];

function renderFilterInput(name, label, value, suggestions) {
    const listId = `filter-options-${name}`;
    const options = suggestions.map((item) => `<option value="${e(item)}"></option>`).join("");
    return `
<label class="filter-input-wrap">
    <span class="filter-input-label">${label}</span>
    <input class="filter-input" name="${name}" value="${e(value ?? "")}" list="${listId}" autocomplete="off">
    <datalist id="${listId}">${options}</datalist>
</label>`;
}

function renderFilterBanner(filter, availableRuns = []) {
    const banner = document.getElementById("filter-banner");
    const isFiltered = !!(
        filter.dandisetId ||
        filter.subject ||
        filter.session ||
        filter.pipelineVersion ||
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

    const dandisets = uniqueSortedValues(availableRuns.map((r) => r.dandisetId));
    const subjects = uniqueSortedValues(availableRuns.map((r) => r.subject));
    const sessions = uniqueSortedValues(availableRuns.map((r) => r.session));
    const versions = uniqueSortedValues(availableRuns.map((r) => r.pipelineVersion));
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

    banner.innerHTML = `
<div class="filter-banner-main">
    <span class="filter-banner-label">Filter runs:</span>
    <form class="filter-form" method="get" action="">
        ${renderFilterInput("dandiset", "Dandiset", filter.dandisetId, dandisets)}
        ${renderFilterInput("subject", "Subject", filter.subject, subjects)}
        ${renderFilterInput("session", "Session", filter.session, sessions)}
        ${renderFilterInput("version", "Version", filter.pipelineVersion, versions)}
        ${renderFilterInput("failureStep", "Failure Step", filter.failureStep, failureSteps)}
        <button class="filter-apply" type="submit">Apply</button>
        <a class="filter-clear" href="./">× View all runs</a>
    </form>
</div>
${filteredViewHtml}`;
    banner.style.display = "";
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
async function fetchRepoTree() {
    const resp = await cachedFetch(`${API_BASE}/git/trees/HEAD?recursive=1`);
    if (!resp.ok) {
        if (resp.status === 403 || resp.status === 429) {
            throw new Error("GitHub API rate limit exceeded. Please try again in a few minutes.");
        }
        throw new Error(`Failed to load repository data (HTTP ${resp.status}).`);
    }
    const data = await resp.json();
    if (data.truncated) {
        console.warn("Repository tree is truncated; some runs may not appear.");
    }
    return data.tree;
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

/* ─── Path parsing ──────────────────────────────────────────── */
// Run paths are: derivatives/{dandiset}/{subject}/{session}/{pipeline}/{version}/{runId}
function parseRuns(tree) {
    const runItems = tree.filter((item) => {
        if (item.type !== "tree") return false;
        const parts = item.path.split("/");
        return parts[0] === "derivatives" && parts.length === 7;
    });

    const blobsByRun = {};
    for (const item of tree) {
        if (item.type !== "blob") continue;
        const parts = item.path.split("/");
        if (parts.length < 8 || parts[0] !== "derivatives") continue;
        const runPath = parts.slice(0, 7).join("/");
        if (!blobsByRun[runPath]) blobsByRun[runPath] = [];
        blobsByRun[runPath].push(item.path);
    }

    return runItems.map((item) => ({
        ...parseRunPath(item.path),
        files: blobsByRun[item.path] || [],
    }));
}

function parseRunPath(runPath) {
    const parts = runPath.split("/");
    //  parts[0] = 'derivatives'
    //  parts[1] = 'dandiset-XXXXXX'
    //  parts[2] = 'sub-NAME'
    //  parts[3] = 'ses-SESSION'
    //  parts[4] = 'pipeline-NAME'
    //  parts[5] = 'version-VER'
    //  parts[6] = 'params-HASH_config-HASH_attempt-N'

    const dandisetId = parts[1].replace(/^dandiset-/, "");
    const subject = parts[2].replace(/^sub-/, "");

    const sesMatch = parts[3].match(/^ses-(.+)$/);
    const session = sesMatch ? sesMatch[1] : parts[3];

    const pipelineName = parts[4].replace(/^pipeline-/, "");
    const pipelineVersion = parts[5].replace(/^version-/, "");

    const runMatch = parts[6].match(/^params-(.+?)_config-(.+?)_attempt-(\d+)$/);
    const paramsProfile = runMatch ? runMatch[1] : parts[6];
    const configHash = runMatch ? runMatch[2] : "";
    const attempt = runMatch ? parseInt(runMatch[3], 10) : 1;

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
    const partial = runs.filter((r) => r.status === "partial").length;
    const unknown = total - success - failed - partial;

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

/* Pretty-print a visualization image name */
function vizLabel(fileName) {
    return (
        fileName
            .replace(/\.png$/i, "")
            .replace(/_/g, " ")
            .replace(/\bseg(\d+)\b/g, "Seg $1")
            .replace(/\bfull\b/i, "Full")
            .replace(/\bproc\b/i, "Processed")
            .replace(/\btraces\b/i, "Traces")
            .replace(/\bdrift map\b/i, "Drift Map")
            .replace(/\bmotion\b/i, "Motion")
            // title-case first letter
            .replace(/^\w/, (c) => c.toUpperCase())
    );
}

/* Build a raw CDN URL for a repo file path */
function cdnUrl(filePath) {
    return `${CDN_BASE}/${filePath.split("/").map(encodeURIComponent).join("/")}`;
}

/* Build a GitHub blob URL for a repo file path */
function blobUrl(filePath) {
    return `https://github.com/${OWNER}/${REPO}/blob/${BRANCH}/${filePath.split("/").map(encodeURIComponent).join("/")}`;
}

/* Build a Neurosift URL for a DANDI asset */
function neurosiftUrl(dandisetId, assetId) {
    const isSandbox = SANDBOX_DANDISETS.has(dandisetId);
    const assetDownloadUrl = `${dandiApiBaseUrl(dandisetId)}/api/assets/${assetId}/download/`;
    const url = `https://neurosift.app/nwb?url=${encodeURIComponent(assetDownloadUrl)}&dandisetId=${encodeURIComponent(dandisetId)}&dandisetVersion=draft`;
    return isSandbox ? `${url}&staging=true` : url;
}

function renderRunEntry(run) {
    const sc =
        run.status === "success"
            ? "status-success"
            : run.status === "failed"
              ? "status-failed"
              : run.status === "partial"
                ? "status-partial"
                : "status-unknown";
    const slbl =
        run.status === "success"
            ? "✓ Success"
            : run.status === "failed"
              ? "✗ Failed"
              : run.status === "partial"
                ? "⚠ Partial"
                : "? Unknown";

    const logFiles = run.files.filter((f) => f.includes("/logs/")).map((f) => f.split("/").pop());

    const vizByRecording = {};
    for (const f of run.files) {
        if (!f.includes("/visualization/") || !f.endsWith(".png")) continue;
        const parts = f.split("/");
        // …/visualization/{recording}/{file.png}
        const recIdx = parts.indexOf("visualization");
        if (recIdx < 0 || recIdx + 1 >= parts.length - 1) continue;
        const rec = parts[recIdx + 1];
        const fname = parts[parts.length - 1];
        if (!vizByRecording[rec]) vizByRecording[rec] = [];
        vizByRecording[rec].push({ path: f, name: fname });
    }

    const hasViz = Object.keys(vizByRecording).length > 0;
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
    ${hasLogs ? renderLogSection(run.path, buttonLogs) : ""}
    ${hasInline ? renderReportSection(run.path, inlineLogs) : ""}
    ${hasViz ? renderVizSection(vizByRecording) : ""}
</div>`;
}

function renderPipelineInfo(pipelineName, pipelineVersion) {
    const MIN_COMMIT_HASH_LENGTH = 6;
    const vParts = pipelineVersion.split("+");
    const lastPart = vParts[vParts.length - 1];
    const hasCommit = vParts.length > 1 && lastPart.length >= MIN_COMMIT_HASH_LENGTH && /^[0-9a-f]+$/i.test(lastPart);

    const displayName = e(pipelineName.replace(/\+/g, "-"));

    if (hasCommit) {
        const commitHash = lastPart;
        // Version parts use '-' as separator; the final '+' preserves the commit hash as a distinct suffix.
        const displayVer = e(vParts.slice(0, -1).join("-") + "+" + commitHash);
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

function renderVizSection(vizByRecording) {
    const recordings = Object.keys(vizByRecording).sort();
    const sections = recordings
        .map((rec) => {
            const images = vizByRecording[rec]
                .sort((a, b) => a.name.localeCompare(b.name))
                .map((img) => {
                    const src = cdnUrl(img.path);
                    const lbl = vizLabel(img.name);
                    return `<figure class="viz-figure">
                    <a href="${e(src)}" target="_blank" rel="noopener">
                        <img src="${e(src)}" alt="${e(lbl)}" loading="lazy" class="viz-img">
                    </a>
                    <figcaption>${e(lbl)}</figcaption>
                </figure>`;
                })
                .join("");

            const recLabel = rec
                .replace(/block(\d+)_acquisition-(\w+)_recording(\d+)/, "Block $1 · $2 · Recording $3")
                .replace(/ElectricalSeriesRaw/g, "Electrical Series (Raw)");

            return `<div class="viz-recording">
            <div class="viz-recording-label">${e(recLabel)}</div>
            <div class="viz-grid">${images}</div>
        </div>`;
        })
        .join("");

    return `
<details class="run-section" open>
    <summary class="run-section-title">
        Visualizations
        <span class="count-badge">${Object.values(vizByRecording).reduce((s, a) => s + a.length, 0)}</span>
    </summary>
    ${sections}
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
    const p = runs.filter((r) => r.status === "partial").length;
    const u = runs.length - s - f - p;
    const parts = [];
    if (s)
        parts.push(
            `<span class="gbadge gbadge-success" title="${s} successful run${s !== 1 ? "s" : ""}">${s}&thinsp;✓</span>`
        );
    if (f)
        parts.push(
            `<span class="gbadge gbadge-failed"  title="${f} failed run${f !== 1 ? "s" : ""}">${f}&thinsp;✗</span>`
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
            <a class="dandiset-link" href="${dandiBaseUrl(dandisetId)}/dandiset/${e(dandisetId)}"
               target="_blank" rel="noopener" onclick="event.stopPropagation()">Dandiset&nbsp;${e(dandisetId)}</a>
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
    const sessions = [...bySession.keys()].sort();
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
    const rep = runs.find((r) => r.assetId) ?? runs[0];
    const sessionLinkHtml = rep.assetId
        ? `<a class="group-link" href="${e(neurosiftUrl(dandisetId, rep.assetId))}"
              target="_blank" rel="noopener" onclick="event.stopPropagation()">Ses:&nbsp;<strong>${e(session)}</strong></a>`
        : `<span class="group-label">Ses:&nbsp;<strong>${e(session)}</strong></span>`;

    const byPipeline = groupBy(runs, (r) => `${r.pipelineName}\x00${r.pipelineVersion}`);
    const pipelineKeys = [...byPipeline.keys()].sort();
    const pipelineHtml = pipelineKeys
        .map((key) => {
            const sep = key.indexOf("\x00");
            const pipelineName = key.slice(0, sep);
            const pipelineVersion = key.slice(sep + 1);
            return renderPipelineVersionGroup(pipelineName, pipelineVersion, byPipeline.get(key));
        })
        .join("");

    return `
<details class="session-group"${autoExpand ? " open" : ""}>
    <summary class="session-summary">
        <span class="group-summary-inner">
            ${sessionLinkHtml}
            <span class="group-meta">
                <span class="group-count">${pipelineKeys.length}&nbsp;pipeline${pipelineKeys.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
            <a class="narrow-link" href="${narrowUrl({ dandiset: dandisetId, subject, session })}"
               title="Narrow view to Ses: ${e(session)}" onclick="event.stopPropagation()">⊕ Narrow</a>
        </span>
    </summary>
    <div class="session-body">
        ${pipelineHtml}
    </div>
</details>`;
}

function renderPipelineVersionGroup(pipelineName, pipelineVersion, runs) {
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
            <a class="narrow-link" href="${e(narrowUrl({ pipelineVersion }))}"
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
    const paramsLabel = `Params:&nbsp;${e(paramsProfile)}`;
    const configLabel = configHash ? `&nbsp;·&nbsp;Config:&nbsp;${e(configHash)}` : "";

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
}

function openLogModal(filePath, label, isHtml, externalHref) {
    const overlay = document.getElementById("log-modal");
    const titleEl = document.getElementById("log-modal-title");
    const bodyEl = document.getElementById("log-modal-body");
    const extLink = document.getElementById("log-modal-external");

    const generation = ++_modalGeneration;

    titleEl.textContent = label;
    extLink.href = externalHref;
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
    document.getElementById("runs").style.display = "";
}

/* ─── Utility ───────────────────────────────────────────────── */
function e(str) {
    return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/* ─── Main ──────────────────────────────────────────────────── */
async function init() {
    initTheme();
    initModal();
    showLoading();
    renderFilterBanner(parseFilter(), []);

    try {
        const tree = await fetchRepoTree();
        const runs = parseRuns(tree);

        if (runs.length === 0) {
            renderFilterBanner(parseFilter(), []);
            showError("No pipeline runs found in the repository.");
            return;
        }

        // Fetch trace.txt, dataset_description.json and DANDI asset IDs for all runs in parallel
        const runsWithStatus = await Promise.all(
            runs.map(async (run) => {
                const [text, datasetDesc, dandiResult] = await Promise.all([
                    fetchTraceText(run.path),
                    fetchDatasetDescription(run.path),
                    fetchDandiAssetId(run.dandisetId, run.subject, run.session),
                ]);
                const parsed = parseTrace(text);
                const assetId = dandiResult?.assetId ?? null;
                const inSourcedata = dandiResult?.inSourcedata ?? false;
                const generatedBy = Array.isArray(datasetDesc?.GeneratedBy) ? datasetDesc.GeneratedBy : [];
                // Any run without an /output folder is considered failed
                const hasOutput = run.files.some((f) => f.includes("/output/"));
                const status = hasOutput ? parsed.status : "failed";
                const failureStep = isFailedStatus(status) ? runFailureStep({ status, tasks: parsed.tasks }) : null;
                return { ...run, ...parsed, assetId, inSourcedata, generatedBy, status, failureStep };
            })
        );

        // Newest first by date, then attempt
        runsWithStatus.sort((a, b) => {
            const d = (b.runDate ?? "").localeCompare(a.runDate ?? "");
            return d !== 0 ? d : b.attempt - a.attempt;
        });

        const EXCLUDED_FROM_SUMMARY = new Set(["214527"]);
        const filter = parseFilter();
        const isFiltered = !!(
            filter.dandisetId ||
            filter.subject ||
            filter.session ||
            filter.pipelineVersion ||
            filter.failureStep
        );
        const filteredRuns = applyFilter(runsWithStatus, filter);

        if (isFiltered && filteredRuns.length === 0) {
            renderFilterBanner(filter, runsWithStatus);
            showError("No pipeline runs match the current filter.");
            return;
        }

        // On the global view, exclude sandbox dandisets from the summary.
        // When a specific dandiset (or subject/session within one) is selected,
        // show the full summary for the filtered scope.
        const runsForSummary = isFiltered
            ? filteredRuns
            : filteredRuns.filter((r) => !EXCLUDED_FROM_SUMMARY.has(r.dandisetId));
        renderSummary(runsForSummary);
        renderFilterBanner(filter, runsWithStatus);
        document.getElementById("runs").innerHTML = renderDandisets(filteredRuns);
        initInlineHtmlFrames();
        showResults();
    } catch (err) {
        renderFilterBanner(parseFilter(), []);
        showError(err.message || "An unexpected error occurred.");
    }
}

document.addEventListener("DOMContentLoaded", init);
