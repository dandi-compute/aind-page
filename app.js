/* ─── Configuration ─────────────────────────────────────────── */
const OWNER  = 'dandi-compute';
const REPO   = '001697';
const BRANCH = 'main';
const CDN_BASE = `https://raw.githubusercontent.com/${OWNER}/${REPO}/${BRANCH}`;
const API_BASE = `https://api.github.com/repos/${OWNER}/${REPO}`;

/* ─── Theme toggle ──────────────────────────────────────────── */
function initTheme() {
    const btn = document.getElementById('theme_toggle_btn');
    const stored = localStorage.getItem('theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const isDark = stored ? stored === 'dark' : prefersDark;
    applyTheme(isDark ? 'dark' : 'light', btn);
    btn.addEventListener('click', () => {
        const next = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
        applyTheme(next, btn);
        localStorage.setItem('theme', next);
    });
}

function applyTheme(theme, btn) {
    if (theme === 'light') {
        document.documentElement.dataset.theme = 'light';
        btn.setAttribute('aria-label', 'Switch to dark mode');
        btn.title = 'Switch to dark mode';
        btn.innerHTML = SUN_ICON;
    } else {
        delete document.documentElement.dataset.theme;
        btn.setAttribute('aria-label', 'Switch to light mode');
        btn.title = 'Switch to light mode';
        btn.innerHTML = MOON_ICON;
    }
}

const MOON_ICON = `<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z"/></svg>`;
const SUN_ICON  = `<svg viewBox="0 0 20 20" aria-hidden="true"><circle cx="10" cy="10" r="3"/><path d="M10 1v2M10 17v2M1 10h2M17 10h2M3.22 3.22l1.42 1.42M15.36 15.36l1.42 1.42M3.22 16.78l1.42-1.42M15.36 4.64l1.42-1.42" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round"/></svg>`;

/* ─── Data fetching ─────────────────────────────────────────── */
async function fetchRepoTree() {
    const resp = await fetch(`${API_BASE}/git/trees/HEAD?recursive=1`);
    if (!resp.ok) {
        if (resp.status === 403 || resp.status === 429) {
            throw new Error('GitHub API rate limit exceeded. Please try again in a few minutes.');
        }
        throw new Error(`Failed to load repository data (HTTP ${resp.status}).`);
    }
    const data = await resp.json();
    if (data.truncated) {
        console.warn('Repository tree is truncated; some runs may not appear.');
    }
    return data.tree;
}

async function fetchTraceText(runPath) {
    const pathParts = runPath.split('/').map(encodeURIComponent).join('/');
    const url = `${CDN_BASE}/${pathParts}/logs/trace.txt`;
    try {
        const resp = await fetch(url);
        if (!resp.ok) return null;
        return resp.text();
    } catch {
        return null;
    }
}

/* ─── Path parsing ──────────────────────────────────────────── */
// Run paths are: derivatives/{dandiset}/{subject}/{session}/{pipeline}/{runId}
function parseRuns(tree) {
    const runItems = tree.filter(item => {
        if (item.type !== 'tree') return false;
        const parts = item.path.split('/');
        return parts[0] === 'derivatives' && parts.length === 6;
    });

    const blobsByRun = {};
    for (const item of tree) {
        if (item.type !== 'blob') continue;
        const parts = item.path.split('/');
        if (parts.length < 7 || parts[0] !== 'derivatives') continue;
        const runPath = parts.slice(0, 6).join('/');
        if (!blobsByRun[runPath]) blobsByRun[runPath] = [];
        blobsByRun[runPath].push(item.path);
    }

    return runItems.map(item => ({
        ...parseRunPath(item.path),
        files: blobsByRun[item.path] || [],
    }));
}

function parseRunPath(runPath) {
    const parts   = runPath.split('/');
    //  parts[0] = 'derivatives'
    //  parts[1] = 'dandiset-XXXXXX'
    //  parts[2] = 'sub-NAME'
    //  parts[3] = 'sub-NAME_ses-SESSION'
    //  parts[4] = 'pipeline-NAME_version-VER'
    //  parts[5] = 'params-PROFILE_date-YYYY+MM+DD_attempt-N'

    const dandisetId = parts[1].replace(/^dandiset-/, '');
    const subject    = parts[2].replace(/^sub-/, '');

    const sesMatch   = parts[3].match(/_ses-(.+)$/);
    const session    = sesMatch ? sesMatch[1] : parts[3];

    const pipeMatch  = parts[4].match(/^pipeline-(.+?)_version-(.+)$/);
    const pipelineName    = pipeMatch ? pipeMatch[1] : parts[4].replace(/^pipeline-/, '');
    const pipelineVersion = pipeMatch ? pipeMatch[2] : '';

    const runMatch  = parts[5].match(/^params-(.+?)_date-(.+?)_attempt-(\d+)$/);
    const paramsProfile = runMatch ? runMatch[1] : parts[5];
    const runDate       = runMatch ? runMatch[2].replace(/\+/g, '-') : '';
    const attempt       = runMatch ? parseInt(runMatch[3], 10) : 1;

    return { path: runPath, dandisetId, subject, session, pipelineName, pipelineVersion, paramsProfile, runDate, attempt };
}

/* ─── Trace parsing ─────────────────────────────────────────── */
function parseTrace(text) {
    if (!text) return { status: 'unknown', tasks: [] };
    const lines = text.trim().split('\n').filter(Boolean);
    if (lines.length < 2) return { status: 'unknown', tasks: [] };

    const headers   = lines[0].split('\t');
    const idx = h  => headers.indexOf(h);

    const tasks = lines.slice(1).map(line => {
        const cols = line.split('\t');
        return {
            name:     cols[idx('name')]      ?? '',
            status:   cols[idx('status')]    ?? '',
            exit:     cols[idx('exit')]      ?? '',
            duration: cols[idx('duration')]  ?? '',
            realtime: cols[idx('realtime')]  ?? '',
            nativeId: cols[idx('native_id')] ?? '',
        };
    });

    const anyFailed    = tasks.some(t => t.status === 'FAILED');
    const allCompleted = tasks.every(t => t.status === 'COMPLETED');
    const status = anyFailed ? 'failed' : allCompleted ? 'success' : 'partial';
    return { status, tasks };
}

/* ─── Rendering ─────────────────────────────────────────────── */
function renderSummary(runs) {
    const total    = runs.length;
    const success  = runs.filter(r => r.status === 'success').length;
    const failed   = runs.filter(r => r.status === 'failed').length;
    const partial  = runs.filter(r => r.status === 'partial').length;
    const unknown  = total - success - failed - partial;

    document.getElementById('summary').innerHTML = `
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
            ${partial ? `<div class="stat-item stat-partial">
                <span class="stat-value">${partial}</span>
                <span class="stat-label">Partial</span>
            </div>` : ''}
            ${unknown ? `<div class="stat-item">
                <span class="stat-value">${unknown}</span>
                <span class="stat-label">Unknown</span>
            </div>` : ''}
        </div>`;
}

/* Pretty-print a log file name */
const LOG_LABELS = {
    'dag.html':      'Pipeline DAG',
    'nextflow.log':  'Nextflow Log',
    'report.html':   'Execution Report',
    'timeline.html': 'Execution Timeline',
    'trace.txt':     'Task Trace',
};
function logLabel(fileName) {
    if (LOG_LABELS[fileName]) return LOG_LABELS[fileName];
    if (fileName.includes('_slurm.log')) return 'SLURM Job Log';
    return fileName;
}

/* Pretty-print a visualization image name */
function vizLabel(fileName) {
    return fileName
        .replace(/\.png$/i, '')
        .replace(/_/g, ' ')
        .replace(/\bseg(\d+)\b/g, 'Seg $1')
        .replace(/\bfull\b/i, 'Full')
        .replace(/\bproc\b/i, 'Processed')
        .replace(/\btraces\b/i, 'Traces')
        .replace(/\bdrift map\b/i, 'Drift Map')
        .replace(/\bmotion\b/i, 'Motion')
        // title-case first letter
        .replace(/^\w/, c => c.toUpperCase());
}

/* Build a raw CDN URL for a repo file path */
function cdnUrl(filePath) {
    return `${CDN_BASE}/${filePath.split('/').map(encodeURIComponent).join('/')}`;
}

/* Build a GitHub blob URL for a repo file path */
function blobUrl(filePath) {
    return `https://github.com/${OWNER}/${REPO}/blob/${BRANCH}/${filePath.split('/').map(encodeURIComponent).join('/')}`;
}

function renderRunCard(run) {
    const sc   = run.status === 'success' ? 'status-success'
               : run.status === 'failed'  ? 'status-failed'
               : run.status === 'partial' ? 'status-partial'
               : 'status-unknown';
    const slbl = run.status === 'success' ? '✓ Success'
               : run.status === 'failed'  ? '✗ Failed'
               : run.status === 'partial' ? '⚠ Partial'
               : '? Unknown';

    const logFiles = run.files
        .filter(f => f.includes('/logs/'))
        .map(f => f.split('/').pop());

    const vizByRecording = {};
    for (const f of run.files) {
        if (!f.includes('/visualization/') || !f.endsWith('.png')) continue;
        const parts  = f.split('/');
        // …/visualization/{recording}/{file.png}
        const recIdx = parts.indexOf('visualization');
        if (recIdx < 0 || recIdx + 1 >= parts.length - 1) continue;
        const rec    = parts[recIdx + 1];
        const fname  = parts[parts.length - 1];
        if (!vizByRecording[rec]) vizByRecording[rec] = [];
        vizByRecording[rec].push({ path: f, name: fname });
    }

    const hasViz  = Object.keys(vizByRecording).length > 0;
    const hasLogs = logFiles.length > 0;
    const hasTasks = run.tasks && run.tasks.length > 0;

    return `
<div class="run-card ${sc}">
    <div class="run-header">
        <span class="status-badge ${sc}">${slbl}</span>
        <div class="run-meta">
            <div class="run-identity">
                <a class="run-dandiset-link" href="https://dandiarchive.org/dandiset/${e(run.dandisetId)}"
                   target="_blank" rel="noopener">Dandiset ${e(run.dandisetId)}</a>
                <span class="run-sep">·</span>
                <span class="run-subject">Sub: <strong>${e(run.subject)}</strong></span>
                <span class="run-sep">·</span>
                <span class="run-session">Ses: <strong>${e(run.session)}</strong></span>
            </div>
            <div class="run-pipeline-info">
                <span class="pipeline-name">${e(run.pipelineName.replace(/\+/g, '-'))}</span>
                <span class="pipeline-version">${e(run.pipelineVersion)}</span>
                <span class="run-sep">·</span>
                <span class="run-date">${e(run.runDate)}</span>
                <span class="run-sep">·</span>
                <span class="run-attempt">Attempt&nbsp;${e(String(run.attempt))}</span>
                ${run.paramsProfile !== 'default' ? `<span class="run-sep">·</span><span class="run-params">Params: ${e(run.paramsProfile)}</span>` : ''}
            </div>
        </div>
    </div>

    ${hasTasks ? renderTraceSection(run.tasks) : ''}
    ${hasLogs  ? renderLogSection(run.path, logFiles) : ''}
    ${hasViz   ? renderVizSection(vizByRecording) : ''}
</div>`;
}

function renderTraceSection(tasks) {
    const rows = tasks.map(t => {
        const sc = t.status === 'COMPLETED' ? 'task-ok'
                 : t.status === 'FAILED'    ? 'task-fail'
                 : 'task-other';
        return `<tr>
            <td>${e(t.name)}</td>
            <td><span class="task-status ${sc}">${e(t.status)}</span></td>
            <td class="mono">${e(t.exit)}</td>
            <td class="mono">${e(t.duration)}</td>
            <td class="mono">${e(t.realtime)}</td>
        </tr>`;
    }).join('');

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
    const links = logFiles.map(fname => {
        const filePath = `${runPath}/logs/${fname}`;
        const isHtml   = fname.endsWith('.html');
        const href     = isHtml ? blobUrl(filePath) : cdnUrl(filePath);
        return `<a href="${e(href)}" class="log-link" target="_blank" rel="noopener">${e(logLabel(fname))}</a>`;
    }).join('');

    return `
<details class="run-section">
    <summary class="run-section-title">
        Logs
        <span class="count-badge">${logFiles.length}</span>
    </summary>
    <div class="log-links">${links}</div>
</details>`;
}

function renderVizSection(vizByRecording) {
    const recordings = Object.keys(vizByRecording).sort();
    const sections   = recordings.map(rec => {
        const images = vizByRecording[rec]
            .sort((a, b) => a.name.localeCompare(b.name))
            .map(img => {
                const src = cdnUrl(img.path);
                const lbl = vizLabel(img.name);
                return `<figure class="viz-figure">
                    <a href="${e(src)}" target="_blank" rel="noopener">
                        <img src="${e(src)}" alt="${e(lbl)}" loading="lazy" class="viz-img">
                    </a>
                    <figcaption>${e(lbl)}</figcaption>
                </figure>`;
            }).join('');

        const recLabel = rec
            .replace(/block(\d+)_acquisition-(\w+)_recording(\d+)/, 'Block $1 · $2 · Recording $3')
            .replace(/ElectricalSeriesRaw/g, 'Electrical Series (Raw)');

        return `<div class="viz-recording">
            <div class="viz-recording-label">${e(recLabel)}</div>
            <div class="viz-grid">${images}</div>
        </div>`;
    }).join('');

    return `
<details class="run-section" open>
    <summary class="run-section-title">
        Visualizations
        <span class="count-badge">${Object.values(vizByRecording).reduce((s, a) => s + a.length, 0)}</span>
    </summary>
    ${sections}
</details>`;
}

/* ─── Page state helpers ────────────────────────────────────── */
function showLoading() {
    document.getElementById('loading').style.display = '';
    document.getElementById('error').style.display   = 'none';
    document.getElementById('summary').style.display = 'none';
    document.getElementById('runs').style.display    = 'none';
}

function showError(msg) {
    document.getElementById('loading').style.display = 'none';
    const el = document.getElementById('error');
    el.style.display = '';
    el.innerHTML = `<p class="error-icon">⚠</p><p>${e(msg)}</p>`;
}

function showResults() {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('summary').style.display = '';
    document.getElementById('runs').style.display    = '';
}

/* ─── Utility ───────────────────────────────────────────────── */
function e(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

/* ─── Main ──────────────────────────────────────────────────── */
async function init() {
    initTheme();
    showLoading();

    try {
        const tree = await fetchRepoTree();
        const runs = parseRuns(tree);

        if (runs.length === 0) {
            showError('No pipeline runs found in the repository.');
            return;
        }

        // Fetch trace.txt for all runs in parallel
        const runsWithStatus = await Promise.all(runs.map(async run => {
            const text   = await fetchTraceText(run.path);
            const parsed = parseTrace(text);
            return { ...run, ...parsed };
        }));

        // Newest first by date, then attempt
        runsWithStatus.sort((a, b) => {
            const d = b.runDate.localeCompare(a.runDate);
            return d !== 0 ? d : b.attempt - a.attempt;
        });

        renderSummary(runsWithStatus);
        document.getElementById('runs').innerHTML = runsWithStatus.map(renderRunCard).join('');
        showResults();

    } catch (err) {
        showError(err.message || 'An unexpected error occurred.');
    }
}

document.addEventListener('DOMContentLoaded', init);
