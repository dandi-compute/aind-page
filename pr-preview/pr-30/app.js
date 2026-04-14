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

/* ─── Data fetching ─────────────────────────────────────────── */
async function fetchRepoTree() {
    const resp = await fetch(`${API_BASE}/git/trees/HEAD?recursive=1`);
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
        const resp = await fetch(url);
        if (!resp.ok) return null;
        return resp.text();
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
    const inlineLogs = logFiles.filter((f) => INLINE_REPORT_FILES.has(f));
    const buttonLogs = logFiles.filter((f) => !INLINE_REPORT_FILES.has(f));
    const hasLogs = buttonLogs.length > 0;
    const hasInline = inlineLogs.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;

    return `
<div class="run-entry ${sc}">
    <div class="run-entry-header">
        <span class="status-badge ${sc}">${slbl}</span>
        ${run.runDate ? `<span class="run-date">${e(run.runDate)}</span><span class="run-sep">·</span>` : ""}
        <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
        <a class="run-entry-github-link" href="${e(blobUrl(run.path))}" target="_blank" rel="noopener">↗ GitHub</a>
    </div>

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
    return dandisetIds.map((id) => renderDandisetGroup(id, byDandiset.get(id))).join("");
}

function renderDandisetGroup(dandisetId, runs) {
    const bySubject = groupBy(runs, (r) => r.subject);
    const subjects = [...bySubject.keys()].sort();
    const subjectHtml = subjects.map((s) => renderSubjectGroup(dandisetId, s, bySubject.get(s))).join("");

    return `
<details class="dandiset-group">
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
        </span>
    </summary>
    <div class="dandiset-body">
        ${subjectHtml}
    </div>
</details>`;
}

function renderSubjectGroup(dandisetId, subject, runs) {
    // Prefer a run with a known assetId to determine inSourcedata for the DANDI link
    const rep = runs.find((r) => r.assetId) ?? runs[0];
    const location = rep.inSourcedata ? `sourcedata/sub-${subject}` : `sub-${subject}`;
    const subjectUrl = `${dandiBaseUrl(dandisetId)}/dandiset/${e(dandisetId)}/draft/files?location=${e(location)}`;

    const bySession = groupBy(runs, (r) => r.session);
    const sessions = [...bySession.keys()].sort();
    const sessionHtml = sessions.map((ses) => renderSessionGroup(dandisetId, ses, bySession.get(ses))).join("");

    return `
<details class="subject-group">
    <summary class="subject-summary">
        <span class="group-summary-inner">
            <a class="group-link" href="${e(subjectUrl)}" target="_blank" rel="noopener"
               onclick="event.stopPropagation()">Sub:&nbsp;<strong>${e(subject)}</strong></a>
            <span class="group-meta">
                <span class="group-count">${sessions.length}&nbsp;session${sessions.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
        </span>
    </summary>
    <div class="subject-body">
        ${sessionHtml}
    </div>
</details>`;
}

function renderSessionGroup(dandisetId, session, runs) {
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
<details class="session-group">
    <summary class="session-summary">
        <span class="group-summary-inner">
            ${sessionLinkHtml}
            <span class="group-meta">
                <span class="group-count">${pipelineKeys.length}&nbsp;pipeline${pipelineKeys.length !== 1 ? "s" : ""}</span>
            </span>
            <span class="group-badges">${renderGroupBadges(runs)}</span>
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
        const resp = await fetch(cdnUrl(filePath));
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

    try {
        const tree = await fetchRepoTree();
        const runs = parseRuns(tree);

        if (runs.length === 0) {
            showError("No pipeline runs found in the repository.");
            return;
        }

        // Fetch trace.txt and DANDI asset IDs for all runs in parallel
        const runsWithStatus = await Promise.all(
            runs.map(async (run) => {
                const [text, dandiResult] = await Promise.all([
                    fetchTraceText(run.path),
                    fetchDandiAssetId(run.dandisetId, run.subject, run.session),
                ]);
                const parsed = parseTrace(text);
                const assetId = dandiResult?.assetId ?? null;
                const inSourcedata = dandiResult?.inSourcedata ?? false;
                return { ...run, ...parsed, assetId, inSourcedata };
            })
        );

        // Newest first by date, then attempt
        runsWithStatus.sort((a, b) => {
            const d = (b.runDate ?? "").localeCompare(a.runDate ?? "");
            return d !== 0 ? d : b.attempt - a.attempt;
        });

        const EXCLUDED_FROM_SUMMARY = new Set(["214527"]);
        const runsForSummary = runsWithStatus.filter((r) => !EXCLUDED_FROM_SUMMARY.has(r.dandisetId));
        renderSummary(runsForSummary);
        document.getElementById("runs").innerHTML = renderDandisets(runsWithStatus);
        initInlineHtmlFrames();
        showResults();
    } catch (err) {
        showError(err.message || "An unexpected error occurred.");
    }
}

document.addEventListener("DOMContentLoaded", init);
