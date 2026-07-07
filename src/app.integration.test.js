const {
    cancelHydration,
    hydrationIdle,
    initHydrationPromotion,
    loadQueueData,
    parseQueueEntries,
    queueStateCacheKey,
    renderFilterBanner,
    renderSummary,
    showDiffResults,
    showError,
    showLoading,
    showResults,
} = require("./app");

beforeEach(() => {
    document.body.innerHTML = `
        <div id="loading"></div>
        <div id="error"></div>
        <div id="filter-banner"></div>
        <div id="summary"></div>
        <div id="layout-bar"></div>
        <div id="runs"></div>
    `;
});

describe("app integration behavior", () => {
    it("renders filter banner with active filter crumb and available options", () => {
        const failedRun = {
            dandisetId: "001697",
            subject: "a",
            session: "s1",
            pipelineVersion: "v1",
            paramsProfile: "params-a",
            configHash: "config-a",
            generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "abc1234" }],
            status: "failed",
            failureStep: "pre-processing",
        };
        const successfulRun = {
            dandisetId: "001697",
            subject: "b",
            session: "s2",
            pipelineVersion: "v2",
            paramsProfile: "params-b",
            configHash: "config-b",
            generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "def5678" }],
            status: "success",
            failureStep: null,
        };
        renderFilterBanner(
            {
                // renderFilterBanner filters use paramsType/configType query keys.
                dandisetId: "001697",
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: failedRun.paramsProfile,
                configType: failedRun.configHash,
                failureStep: "pre-processing",
            },
            [failedRun, successfulRun]
        );

        const banner = document.getElementById("filter-banner");
        expect(banner.style.display).toBe("");
        expect(banner.innerHTML).toContain("Filtered view:");
        expect(banner.innerHTML).toContain("Failed in pre-processing");
        expect(banner.innerHTML).toContain(`Params:&nbsp;${failedRun.paramsProfile}`);
        expect(banner.innerHTML).toContain(`Config:&nbsp;${failedRun.configHash}`);
        expect(banner.innerHTML).toContain(`option value="${failedRun.paramsProfile}"`);
        expect(banner.innerHTML).toContain(`option value="${successfulRun.paramsProfile}"`);
        expect(banner.innerHTML).toContain(`option value="${failedRun.configHash}"`);
        expect(banner.innerHTML).toContain(`option value="${successfulRun.configHash}"`);
    });

    it("updates page state displays and escapes error content", () => {
        showLoading();
        expect(document.getElementById("loading").style.display).toBe("");
        expect(document.getElementById("summary").style.display).toBe("none");

        showError('<script>alert("xss")</script>');
        expect(document.getElementById("error").innerHTML).toContain("&lt;script&gt;");
        expect(document.getElementById("loading").style.display).toBe("none");

        showResults();
        expect(document.getElementById("summary").style.display).toBe("");
        expect(document.getElementById("runs").style.display).toBe("");
    });

    it("shows summed bytes in the summary when runs include size metadata", () => {
        renderSummary([
            { status: "success", assetSizeBytes: 10 },
            { status: "failed", assetSizeBytes: 20 },
            { status: "queued" },
        ]);

        expect(document.getElementById("summary").innerHTML).toContain("DATA PROCESSED");
        expect(document.getElementById("summary").innerHTML).toContain("10 B");
        expect(document.getElementById("summary").innerHTML).not.toContain("30 B");
    });

    it("hides summary DATA PROCESSED when only failed runs have byte metadata", () => {
        renderSummary([
            { status: "failed", assetSizeBytes: 20 },
            { status: "queued", assetSizeBytes: 30 },
        ]);
        expect(document.getElementById("summary").innerHTML).not.toContain("DATA PROCESSED");
    });

    it("shows running counter in summary when running runs are present", () => {
        renderSummary([
            { status: "success", assetSizeBytes: 10 },
            { status: "running" },
            { status: "running" },
            { status: "queued" },
        ]);

        const summaryHtml = document.getElementById("summary").innerHTML;
        expect(summaryHtml).toContain("stat-running");
        expect(summaryHtml).toContain("Running");
    });

    it("shows running counter at 0 when no running runs are present", () => {
        renderSummary([{ status: "success", assetSizeBytes: 10 }, { status: "queued" }]);

        const summaryHtml = document.getElementById("summary").innerHTML;
        expect(summaryHtml).toContain('class="stat-item stat-running"');
        expect(summaryHtml).toContain('<span class="stat-value">0</span>');
    });

    it("shows stalled counter when running runs exceed 24 hours", () => {
        const oldCreatedAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString();
        const recentCreatedAt = new Date(Date.now() - 1 * 60 * 60 * 1000).toISOString();
        renderSummary([
            { status: "running", createdAt: oldCreatedAt },
            { status: "running", createdAt: recentCreatedAt },
        ]);

        const summaryHtml = document.getElementById("summary").innerHTML;
        expect(summaryHtml).toContain("stat-stalled");
        expect(summaryHtml).toContain("Stalled");
        expect(summaryHtml).toContain("⚠ 1");
        expect(summaryHtml).toContain("status=stalled");
    });

    it("does not double count stalled runs as running", () => {
        const stalledCreatedAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString();
        const recentCreatedAt = new Date(Date.now() - 1 * 60 * 60 * 1000).toISOString();
        renderSummary([
            { status: "running", createdAt: stalledCreatedAt },
            { status: "running", createdAt: stalledCreatedAt },
            { status: "running", createdAt: recentCreatedAt },
        ]);

        const summary = document.getElementById("summary");
        const runningValue = summary.querySelector(".stat-running .stat-value").textContent;
        const stalledValue = summary.querySelector(".stat-stalled .stat-value").textContent;
        expect(runningValue).toBe("1");
        expect(stalledValue).toBe("⚠ 2");
        expect(summary.innerHTML).not.toContain("Unknown");
    });

    it("hides stalled counter when no running runs exceed 24 hours", () => {
        const recentCreatedAt = new Date(Date.now() - 1 * 60 * 60 * 1000).toISOString();
        renderSummary([{ status: "running", createdAt: recentCreatedAt }]);

        const summaryHtml = document.getElementById("summary").innerHTML;
        expect(summaryHtml).not.toContain("stat-stalled");
        expect(summaryHtml).not.toContain("Stalled");
    });

    it("formats large byte counts with appropriate decimal units", () => {
        renderSummary([{ status: "success", assetSizeBytes: 2_500_000_000_000 }]);
        expect(document.getElementById("summary").innerHTML).toContain("2.5 TB");
    });

    it("renders clickable success and failed summary counters that apply status filtering", () => {
        window.history.replaceState(
            null,
            "",
            "/?layout=flat&sort=attempt&sortDir=desc&dandiset=001697&failureStep=pre-processing"
        );
        renderSummary([
            { status: "success", assetSizeBytes: 100 },
            { status: "failed", assetSizeBytes: 200 },
        ]);

        const summaryHtml = document.getElementById("summary").innerHTML;
        expect(summaryHtml).toContain(
            'class="stat-item stat-success" href="?layout=flat&amp;sort=attempt&amp;sortDir=desc&amp;dandiset=001697&amp;status=success"'
        );
        expect(summaryHtml).toContain(
            'class="stat-item stat-failed" href="?layout=flat&amp;sort=attempt&amp;sortDir=desc&amp;dandiset=001697&amp;failureStep=pre-processing&amp;status=failed"'
        );
    });

    it("shows only the diff content region on the diff page", () => {
        showDiffResults();
        expect(document.getElementById("loading").style.display).toBe("none");
        expect(document.getElementById("filter-banner").style.display).toBe("none");
        expect(document.getElementById("summary").style.display).toBe("none");
        expect(document.getElementById("layout-bar").style.display).toBe("none");
        expect(document.getElementById("runs").style.display).toBe("");
    });

    it("includes layout, sort mode, and sort direction in filter form and clear links for shareable URLs", () => {
        window.history.replaceState(null, "", "/?layout=flat&sort=created_at&sortDir=asc");
        renderFilterBanner(
            {
                dandisetId: null,
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: null,
                configType: null,
                dandiCodebaseHash: null,
                failureStep: null,
            },
            []
        );

        const banner = document.getElementById("filter-banner");
        expect(banner.innerHTML).toContain('name="layout" value="flat"');
        expect(banner.innerHTML).toContain('name="sort" value="created_at"');
        expect(banner.innerHTML).toContain('name="sortDir" value="asc"');
        expect(banner.innerHTML).toContain('href="?layout=flat&amp;sort=created_at&amp;sortDir=asc"');
    });

    it("preserves status filter in the filter form when active", () => {
        window.history.replaceState(null, "", "/?layout=flat&sort=created_at&sortDir=asc&status=failed");
        renderFilterBanner(
            {
                dandisetId: null,
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: null,
                configType: null,
                failureStep: null,
                status: "failed",
            },
            [
                { status: "failed", dandisetId: "001697", subject: "sub-1", session: "ses-1", pipelineVersion: "v1" },
                { status: "success", dandisetId: "001697", subject: "sub-2", session: "ses-2", pipelineVersion: "v1" },
            ]
        );

        const banner = document.getElementById("filter-banner");
        expect(banner.innerHTML).toContain('name="status" value="failed"');
        expect(banner.innerHTML).toContain("Filtered view:");
        expect(banner.innerHTML).toContain("Failed");
    });

    it("scopes subject and session options based on selected dandiset and subject", () => {
        renderFilterBanner(
            {
                dandisetId: "000363",
                subject: "sub-a",
                session: null,
                pipelineVersion: null,
                paramsType: null,
                configType: null,
                dandiCodebaseHash: null,
                failureStep: null,
            },
            [
                {
                    dandisetId: "000363",
                    subject: "sub-a",
                    session: "ses-1",
                    pipelineVersion: "v1",
                    paramsProfile: "4af6a25",
                    configHash: "0d4bf36",
                    status: "success",
                },
                {
                    dandisetId: "000363",
                    subject: "sub-a",
                    session: "ses-2",
                    pipelineVersion: "v1",
                    paramsProfile: "4af6a25",
                    configHash: "0d4bf36",
                    status: "success",
                },
                {
                    dandisetId: "000363",
                    subject: "sub-b",
                    session: "ses-3",
                    pipelineVersion: "v1",
                    paramsProfile: "98fd947",
                    configHash: "6568dda",
                    status: "success",
                },
                {
                    dandisetId: "999999",
                    subject: "other-sub",
                    session: "other-ses",
                    pipelineVersion: "v1",
                    paramsProfile: "98fd947",
                    configHash: "beef123",
                    status: "success",
                },
            ]
        );

        const subjectOptions = [...document.querySelectorAll("#filter-options-subject option")].map(
            (option) => option.value
        );
        const sessionOptions = [...document.querySelectorAll("#filter-options-session option")].map(
            (option) => option.value
        );

        expect(subjectOptions).toEqual(["sub-a", "sub-b"]);
        expect(sessionOptions).toEqual(["ses-1", "ses-2"]);
    });

    it("updates subject and session options immediately when filter inputs change", () => {
        renderFilterBanner(
            {
                dandisetId: null,
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: null,
                configType: null,
                dandiCodebaseHash: null,
                failureStep: null,
            },
            [
                {
                    dandisetId: "000363",
                    subject: "sub-a",
                    session: "ses-1",
                    pipelineVersion: "v1",
                    paramsProfile: "4af6a25",
                    configHash: "0d4bf36",
                    status: "success",
                },
                {
                    dandisetId: "000363",
                    subject: "sub-b",
                    session: "ses-2",
                    pipelineVersion: "v1",
                    paramsProfile: "98fd947",
                    configHash: "6568dda",
                    status: "success",
                },
                {
                    dandisetId: "000364",
                    subject: "sub-x",
                    session: "ses-9",
                    pipelineVersion: "v1",
                    paramsProfile: "aa073df",
                    configHash: "beef123",
                    status: "success",
                },
            ]
        );

        const dandisetInput = document.querySelector('input[name="dandiset"]');
        const subjectInput = document.querySelector('input[name="subject"]');

        dandisetInput.value = "000363";
        dandisetInput.dispatchEvent(new Event("input", { bubbles: true }));

        let subjectOptions = [...document.querySelectorAll("#filter-options-subject option")].map(
            (option) => option.value
        );
        expect(subjectOptions).toEqual(["sub-a", "sub-b"]);

        subjectInput.value = "sub-a";
        subjectInput.dispatchEvent(new Event("input", { bubbles: true }));

        const sessionOptions = [...document.querySelectorAll("#filter-options-session option")].map(
            (option) => option.value
        );
        expect(sessionOptions).toEqual(["ses-1"]);
    });
});

describe("progressive queue loading", () => {
    const QUEUE_KEY = queueStateCacheKey();
    let blobCounter = 0;
    let originalFetch;

    const newBlobId = () => `zz${String(blobCounter++).padStart(30, "0")}`;
    const blobUrlFor = (id) => `https://dandiarchive.s3.amazonaws.com/blobs/${id.slice(0, 3)}/${id.slice(3, 6)}/${id}`;

    function deferred() {
        let resolve;
        const promise = new Promise((r) => (resolve = r));
        return { promise, resolve };
    }

    function makeEntry(overrides = {}) {
        return {
            dandiset_id: "000777",
            subject: "sub1",
            session: "ses1",
            pipeline: "aind+ephys",
            version: "v1.2.4",
            params: "1cbdbee",
            config: "7940dfd",
            attempt: 1,
            has_code: true,
            has_been_submitted: true,
            has_output: true,
            has_logs: true,
            ...overrides,
        };
    }

    // Attach blob-backed artifacts under the entry's computed run path and
    // return { entry, urls } with the resolvable blob URL per artifact.
    function withArtifacts(entry, ids = {}) {
        const [probe] = parseQueueEntries([entry]);
        const p = probe.path;
        const urls = {};
        entry.output_paths = { ...(entry.output_paths ?? {}) };
        if (ids.trace) {
            entry.output_paths[`${p}/logs/trace.txt`] = ids.trace;
            urls.trace = blobUrlFor(ids.trace);
        }
        if (ids.png) entry.output_paths[`${p}/derivatives/visualization/summary/psd.png`] = ids.png;
        if (ids.qc) {
            entry.output_paths[`${p}/derivatives/visualization/quality_control.json`] = ids.qc;
            urls.qc = blobUrlFor(ids.qc);
        }
        if (ids.viz) {
            entry.output_paths[`${p}/derivatives/visualization/visualization_output.json`] = ids.viz;
            urls.viz = blobUrlFor(ids.viz);
        }
        if (ids.dd) {
            entry.dataset_description_path = { [`${p}/dataset_description.json`]: ids.dd };
            urls.dd = blobUrlFor(ids.dd);
        }
        return { entry, urls, path: p };
    }

    function seedQueueState(entries) {
        sessionStorage.setItem(
            QUEUE_KEY,
            JSON.stringify({ etag: '"state-etag"', body: entries.map((x) => JSON.stringify(x)).join("\n") })
        );
    }

    // Route the app's fetches: queue state revalidates against the seeded
    // sessionStorage entry (304), registries resolve to a minimal fixture, and
    // S3 blob URLs are served by the per-test handler map.
    function installFetch(blobHandlers) {
        global.fetch = vi.fn(async (url) => {
            const u = String(url);
            if (u.includes("state.jsonl.gz")) return new Response(null, { status: 304 });
            if (u.includes("registered_params.json") || u.includes("registered_configs.json")) {
                return new Response(
                    JSON.stringify({ default: { path: "p.json", md5: "0d4bf36ddb61418ae7714e7d6e5ff8b8" } }),
                    { status: 200 }
                );
            }
            const handler = blobHandlers.get(u);
            if (handler) return handler();
            return new Response(null, { status: 404 });
        });
    }

    const blobRequests = () =>
        global.fetch.mock.calls
            .map(([u]) => String(u))
            .filter((u) => new URL(u).hostname === "dandiarchive.s3.amazonaws.com");

    const TRACE_OK =
        "task_id\tname\tstatus\texit\n1\tjob_dispatch (1)\tCOMPLETED\t0\n2\tpreprocessing (1)\tCOMPLETED\t0";
    const TRACE_FAILED_PRE =
        "task_id\tname\tstatus\texit\n1\tjob_dispatch (1)\tCOMPLETED\t0\n2\tpreprocessing (1)\tFAILED\t1";

    beforeEach(() => {
        cancelHydration();
        sessionStorage.clear();
        // Earlier tests in this file set filter query params without resetting.
        window.history.replaceState(null, "", "/");
        originalFetch = global.fetch;
    });

    afterEach(async () => {
        cancelHydration();
        global.fetch = originalFetch;
        sessionStorage.clear();
        localStorage.clear();
        window.history.replaceState(null, "", "/");
    });

    it("renders instantly from the state file while blob fetches are still pending", async () => {
        const { entry, urls } = withArtifacts(makeEntry(), { trace: newBlobId() });
        seedQueueState([entry]);
        installFetch(new Map([[urls.trace, () => new Promise(() => {})]])); // trace never resolves

        await loadQueueData();

        const card = document.querySelector("#runs .run-entry");
        expect(card).not.toBeNull();
        expect(card.className).toContain("status-success"); // has_output flag, no trace needed
        expect(card.dataset.runKey).toBeTruthy();
        expect(document.getElementById("runs").style.display).toBe("");
        expect(document.getElementById("summary").innerHTML).toContain("Total Runs");
    });

    it("refines status via hydration without collapsing open sections", async () => {
        const { entry, urls } = withArtifacts(makeEntry(), { trace: newBlobId(), png: newBlobId() });
        seedQueueState([entry]);
        const trace = deferred();
        installFetch(new Map([[urls.trace, () => trace.promise]]));

        await loadQueueData();

        const before = document.querySelector("#runs .run-entry");
        expect(before.className).toContain("status-success");
        expect(before.querySelector('details[data-section="trace"]')).toBeNull(); // no tasks yet
        const viz = before.querySelector('details[data-section="viz"]');
        expect(viz).not.toBeNull(); // sync-derived from output_paths
        viz.open = true;

        trace.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await hydrationIdle();

        const after = document.querySelector("#runs .run-entry");
        expect(after.className).toContain("status-failed"); // trace refined success → failed
        expect(after.querySelector('details[data-section="trace"]')).not.toBeNull();
        expect(after.querySelector('details[data-section="viz"]').open).toBe(true); // preserved
        expect(after.dataset.runKey).toBe(before.dataset.runKey);
        expect(document.querySelector("#summary .stat-failed .stat-value").textContent.trim()).toBe("1");
    });

    it("treats an explicit upstream status as authoritative over the trace", async () => {
        const { entry, urls } = withArtifacts(makeEntry({ status: "failed", failure_step: "post-processing" }), {
            trace: newBlobId(),
        });
        seedQueueState([entry]);
        installFetch(new Map([[urls.trace, () => new Response(TRACE_OK, { status: 200 })]]));

        await loadQueueData();
        expect(document.querySelector("#runs .run-entry").className).toContain("status-failed");

        await hydrationIdle();
        // The all-COMPLETED trace must not override the upstream verdict.
        expect(document.querySelector("#runs .run-entry").className).toContain("status-failed");
    });

    it("promotes an expanded run to the front of the hydration queue", async () => {
        const handlers = new Map();
        const entries = [];
        const runsMeta = [];
        for (let i = 0; i < 6; i++) {
            const { entry, urls, path } = withArtifacts(
                makeEntry({ subject: `sub${i}`, has_output: false, has_logs: true }),
                { trace: newBlobId() }
            );
            const gate = deferred();
            handlers.set(urls.trace, () => gate.promise);
            entries.push(entry);
            runsMeta.push({ path, url: urls.trace, gate });
        }
        seedQueueState(entries);
        installFetch(handlers);
        initHydrationPromotion();

        await loadQueueData();
        await Promise.resolve(); // let the first worker wave issue its fetches

        const started = blobRequests();
        expect(started).toHaveLength(3); // HYDRATION_CONCURRENCY
        const pending = runsMeta.filter((m) => !started.includes(m.url));
        const promoted = pending[pending.length - 1];

        // Open a section on the promoted run's card through the real toggle path.
        const card = Array.from(document.querySelectorAll("[data-run-key]")).find(
            (el) => el.dataset.runKey === promoted.path
        );
        expect(card).not.toBeNull();
        const section = card.querySelector("details");
        section.open = true;
        section.dispatchEvent(new Event("toggle"));

        // Free one worker; it must pick up the promoted run next.
        const firstStarted = runsMeta.find((m) => m.url === started[0]);
        firstStarted.gate.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await vi.waitFor(() => {
            if (!blobRequests().includes(promoted.url)) throw new Error("promoted run not yet fetched");
        });
        expect(blobRequests()[3]).toBe(promoted.url);

        for (const m of runsMeta) m.gate.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await hydrationIdle();
    });

    it("brings runs into an active failure-step filter as hydration lands", async () => {
        window.history.replaceState(null, "", "/?failureStep=pre-processing");
        const { entry, urls } = withArtifacts(makeEntry({ has_output: false, has_logs: true }), {
            trace: newBlobId(),
        });
        seedQueueState([entry]);
        const trace = deferred();
        installFetch(new Map([[urls.trace, () => trace.promise]]));

        await loadQueueData();

        // failureStep is unknown pre-hydration, so nothing matches yet.
        expect(document.getElementById("error").innerHTML).toContain("No pipeline runs match");
        expect(document.getElementById("runs").style.display).toBe("none");

        trace.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await hydrationIdle();

        expect(document.getElementById("error").style.display).toBe("none");
        expect(document.getElementById("runs").style.display).toBe("");
        expect(document.querySelector("#runs .run-entry").className).toContain("status-failed");
    });

    it("drops runs out of an active status filter when hydration refutes it", async () => {
        window.history.replaceState(null, "", "/?status=success");
        const { entry, urls } = withArtifacts(makeEntry(), { trace: newBlobId() });
        seedQueueState([entry]);
        const trace = deferred();
        installFetch(new Map([[urls.trace, () => trace.promise]]));

        await loadQueueData();
        expect(document.querySelector("#runs .run-entry")).not.toBeNull(); // flag status matches

        trace.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await hydrationIdle();

        expect(document.getElementById("error").innerHTML).toContain("No pipeline runs match");
        expect(document.getElementById("runs").style.display).toBe("none");
    });

    it("fetches every trace before any detail artifact and never fetches QC eagerly", async () => {
        const entries = [];
        const meta = [];
        const handlers = new Map();
        for (let i = 0; i < 3; i++) {
            const { entry, urls } = withArtifacts(makeEntry({ subject: `sub${i}` }), {
                trace: newBlobId(),
                dd: newBlobId(),
                viz: newBlobId(),
                qc: newBlobId(),
            });
            entries.push(entry);
            meta.push(urls);
            handlers.set(urls.trace, () => new Response(TRACE_OK, { status: 200 }));
            handlers.set(urls.dd, () => new Response("{}", { status: 200 }));
            handlers.set(urls.viz, () => new Response("{}", { status: 200 }));
            handlers.set(urls.qc, () => new Response('{"metrics":[{"name":"drift","stage":"pre"}]}', { status: 200 }));
        }
        seedQueueState(entries);
        installFetch(handlers);

        await loadQueueData();
        await hydrationIdle();

        const order = blobRequests();
        const traceUrls = new Set(meta.map((m) => m.trace));
        const lastTraceIdx = Math.max(...order.map((u, i) => (traceUrls.has(u) ? i : -1)));
        const firstDetailIdx = order.findIndex((u) => !traceUrls.has(u));
        expect(order.filter((u) => traceUrls.has(u))).toHaveLength(3);
        expect(firstDetailIdx).toBeGreaterThan(lastTraceIdx); // status pass fully precedes details
        expect(order.some((u) => meta.some((m) => m.qc === u))).toBe(false); // QC never in background
    });

    it("fetches QC only when a card section is expanded", async () => {
        const { entry, urls } = withArtifacts(makeEntry(), {
            trace: newBlobId(),
            qc: newBlobId(),
            png: newBlobId(),
        });
        seedQueueState([entry]);
        installFetch(
            new Map([
                [urls.trace, () => new Response(TRACE_OK, { status: 200 })],
                [urls.qc, () => new Response('{"metrics":[{"name":"drift","stage":"pre"}]}', { status: 200 })],
            ])
        );
        initHydrationPromotion();

        await loadQueueData();
        await hydrationIdle();
        expect(blobRequests()).not.toContain(urls.qc);
        expect(document.querySelector('details[data-section="qc"]')).toBeNull();

        // Expand a section on the card through the real toggle path.
        const section = document.querySelector("#runs .run-entry details[data-section]");
        section.open = true;
        section.dispatchEvent(new Event("toggle"));
        await hydrationIdle();

        expect(blobRequests()).toContain(urls.qc);
        expect(document.querySelector('details[data-section="qc"]')).not.toBeNull();
    });

    it("marks optimistic successes as provisional until the trace confirms them", async () => {
        const { entry, urls } = withArtifacts(makeEntry(), { trace: newBlobId() });
        seedQueueState([entry]);
        const trace = deferred();
        installFetch(new Map([[urls.trace, () => trace.promise]]));

        await loadQueueData();

        const badge = document.querySelector("#runs .run-entry .status-badge");
        expect(badge.className).toContain("status-provisional");
        expect(document.querySelector(".gbadge-success").className).toContain("status-provisional");

        trace.resolve(new Response(TRACE_OK, { status: 200 }));
        await hydrationIdle();

        const confirmed = document.querySelector("#runs .run-entry .status-badge");
        expect(confirmed.className).toContain("status-success");
        expect(confirmed.className).not.toContain("status-provisional");
        expect(document.querySelector(".gbadge-success").className).not.toContain("status-provisional");
    });

    it("fetches inline report content only when its section is revealed", async () => {
        const rafOriginal = global.requestAnimationFrame;
        global.requestAnimationFrame = (cb) => setTimeout(cb, 0);
        try {
            const { entry, urls } = withArtifacts(makeEntry(), { trace: newBlobId() });
            const reportId = newBlobId();
            const [probe] = parseQueueEntries([entry]);
            entry.output_paths[`${probe.path}/logs/report.html`] = reportId;
            const reportUrl = blobUrlFor(reportId);
            seedQueueState([entry]);
            installFetch(
                new Map([
                    [urls.trace, () => new Response(TRACE_OK, { status: 200 })],
                    [
                        reportUrl,
                        () => new Response("<html><head></head><body>report row</body></html>", { status: 200 }),
                    ],
                ])
            );

            await loadQueueData();
            await hydrationIdle();

            // The Reports section rendered an iframe shell, but the (potentially
            // multi-MB) report content must not have been fetched yet.
            const iframe = document.querySelector("iframe[data-srcdoc-url]");
            expect(iframe).not.toBeNull();
            expect(blobRequests()).not.toContain(reportUrl);
            expect(iframe.getAttribute("srcdoc")).toBeNull();

            // Reveal the Reports section through the real toggle path.
            const section = document.querySelector('details[data-section="reports"]');
            section.open = true;
            section.dispatchEvent(new Event("toggle"));
            await new Promise((r) => setTimeout(r, 0)); // rAF stub tick
            await new Promise((r) => setTimeout(r, 0)); // fetch + patch settle
            await new Promise((r) => setTimeout(r, 0));

            expect(blobRequests()).toContain(reportUrl);
            expect(iframe.getAttribute("srcdoc")).toContain("report row");
        } finally {
            global.requestAnimationFrame = rafOriginal;
        }
    });

    it("supersedes stale hydration when the queue is reloaded mid-flight", async () => {
        const first = withArtifacts(makeEntry(), { trace: newBlobId() });
        seedQueueState([first.entry]);
        const staleTrace = deferred();
        const handlers = new Map([[first.urls.trace, () => staleTrace.promise]]);
        installFetch(handlers);
        await loadQueueData(); // generation 1: trace hangs

        // The run re-ran upstream: same identity, new content-addressed blob.
        const second = withArtifacts(makeEntry(), { trace: newBlobId() });
        handlers.set(second.urls.trace, () => new Response(TRACE_OK, { status: 200 }));
        seedQueueState([second.entry]);
        await loadQueueData(); // generation 2
        await hydrationIdle();
        expect(document.querySelector("#runs .run-entry").className).toContain("status-success");

        // The stale generation-1 result must not touch the fresh DOM.
        staleTrace.resolve(new Response(TRACE_FAILED_PRE, { status: 200 }));
        await new Promise((r) => setTimeout(r, 0));
        expect(document.querySelector("#runs .run-entry").className).toContain("status-success");
    });
});
