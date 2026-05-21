const {
    applyFilter,
    buildRunPath,
    buildPipelineDiffPairs,
    classifyFailedTaskStep,
    collectJsonDiffs,
    collectTextDiffs,
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
    renderDiffPage,
    renderDandisets,
    renderParamsGroup,
    renderRegistryLink,
    renderFlatList,
    renderVisualizationSection,
    runFailureStep,
    uniquePipelineEntries,
} = require("./app");

const QUEUE_STATE_CACHE_KEY =
    "aind_etag:https://raw.githubusercontent.com/dandi-compute/queue/compressed/state.jsonl.gz";

/** A passthrough TransformStream that stands in for DecompressionStream in tests. */
class MockDecompressionStream {
    constructor() {
        const ts = new TransformStream();
        this.readable = ts.readable;
        this.writable = ts.writable;
    }
}

/** Wrap plain text in a ReadableStream so it can be used as a Response body. */
function makeReadableStream(text) {
    return new ReadableStream({
        start(controller) {
            controller.enqueue(new TextEncoder().encode(text));
            controller.close();
        },
    });
}

beforeEach(() => {
    document.body.innerHTML = "";
});

afterEach(() => {
    document.body.innerHTML = "";
    localStorage.clear();
    window.history.replaceState(null, "", "/");
});

describe("app unit behavior", () => {
    it("parses layout mode from URL query when present", () => {
        localStorage.setItem("layoutMode", "tree");
        window.history.replaceState(null, "", "/?layout=flat");
        expect(parseLayoutMode()).toBe("flat");
    });

    it("falls back to localStorage layout mode when URL query is absent", () => {
        localStorage.setItem("layoutMode", "flat");
        window.history.replaceState(null, "", "/");
        expect(parseLayoutMode()).toBe("flat");
    });

    it("classifies failure steps from task names", () => {
        expect(classifyFailedTaskStep("dispatch workflow")).toBe("job-dispatch");
        expect(classifyFailedTaskStep("pre_process data")).toBe("pre-processing");
        expect(classifyFailedTaskStep("post-processing upload")).toBe("post-processing");
        expect(classifyFailedTaskStep("other step")).toBe("other");
    });

    it("parses trace status and tasks", () => {
        const trace = [
            "name\tstatus\texit\tduration\trealtime\tnative_id",
            "step-one\tCOMPLETED\t0\t1m\t1m\t101",
            "step-two\tFAILED\t1\t2m\t2m\t102",
        ].join("\n");

        const parsed = parseTrace(trace);
        expect(parsed.status).toBe("failed");
        expect(parsed.tasks).toHaveLength(2);
        expect(parsed.tasks[1]).toMatchObject({ name: "step-two", status: "FAILED" });
    });

    it("derives run failure step priority from failed tasks", () => {
        expect(
            runFailureStep({
                status: "failed",
                tasks: [
                    { name: "dispatch workflow", status: "FAILED" },
                    { name: "post_process", status: "FAILED" },
                ],
            })
        ).toBe("post-processing");
    });

    it("filters runs by failure-step preset", () => {
        const runs = [
            { status: "failed", failureStep: "job-dispatch" },
            { status: "failed", failureStep: "pre-processing" },
            { status: "success", failureStep: null },
        ];

        const filtered = applyFilter(runs, { failureStep: "exclude-job-dispatch" });
        expect(filtered).toEqual([{ status: "failed", failureStep: "pre-processing" }]);
    });

    it("filters runs by params/config types and dandi codebase hash", () => {
        const runs = [
            {
                paramsProfile: "4af6a25",
                configHash: "0d4bf36",
                generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "1.0.0+abc1234" }],
            },
            {
                paramsProfile: "98fd947",
                configHash: "6568dda",
                generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code/tree/main", Version: "def5678" }],
            },
            {
                paramsProfile: "4af6a25",
                configHash: "0d4bf36",
                generatedBy: [{ CodeURL: "https://github.com/other/repo", Version: "xyz9876" }],
            },
            {
                paramsProfile: "aa073df",
                configHash: "6568dda",
                generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "release-tag" }],
            },
            {
                paramsProfile: "unknown-params",
                configHash: "unknown-config",
                generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: null }],
            },
        ];

        expect(applyFilter(runs, { paramsType: "deterministic" })).toEqual([runs[0], runs[2]]);
        expect(applyFilter(runs, { configType: "v1" })).toEqual([runs[0], runs[2]]);
        expect(applyFilter(runs, { paramsType: "4af6a25" })).toEqual([runs[0], runs[2]]);
        expect(applyFilter(runs, { configType: "0d4bf36" })).toEqual([runs[0], runs[2]]);
        expect(applyFilter(runs, { dandiCodebaseHash: "abc1234" })).toEqual([runs[0]]);
        expect(applyFilter(runs, { dandiCodebaseHash: "def5678" })).toEqual([runs[1]]);
        expect(applyFilter(runs, { dandiCodebaseHash: "release-tag" })).toEqual([runs[3]]);
        expect(applyFilter(runs, { dandiCodebaseHash: "missing" })).toEqual([]);
    });

    it("parses run path segments", () => {
        const parsed = parseRunPath(
            "derivatives/dandiset-001697/sub-123/ses-456/pipeline-ephys/version-v2/params-fast_config-abcdef_attempt-3"
        );
        expect(parsed).toMatchObject({
            dandisetId: "001697",
            subject: "123",
            session: "456",
            pipelineName: "ephys",
            pipelineVersion: "v2",
            paramsProfile: "fast",
            configHash: "abcdef",
            attempt: 3,
        });
    });

    it("parses flattened version+capsule run path segments", () => {
        const parsed = parseRunPath(
            "derivatives/dandiset-214527/sub-test/ses-aind+sample/pipeline-aind+ephys/version-v1.1.1+b268fd2+5d20fd2_params-4af6a25_config-0d4bf36_date-2026+05+14_attempt-2"
        );
        expect(parsed).toMatchObject({
            dandisetId: "214527",
            subject: "test",
            session: "aind+sample",
            pipelineName: "aind+ephys",
            pipelineVersion: "v1.1.1+b268fd2+5d20fd2",
            paramsProfile: "4af6a25",
            configHash: "0d4bf36_date-2026+05+14",
            attempt: 2,
        });
    });

    it("builds run path with session from JSONL entry", () => {
        const path = buildRunPath({
            dandiset_id: "000233",
            subject: "CGM3",
            session: "CGM3",
            pipeline: "aind+ephys",
            version: "v1.0.0+fixes+20abeb6",
            params: "98fd947",
            config: "6568dda",
            attempt: 1,
        });
        expect(path).toBe(
            "derivatives/dandiset-000233/sub-CGM3/ses-CGM3/pipeline-aind+ephys/version-v1.0.0+fixes+20abeb6_params-98fd947_config-6568dda_attempt-1"
        );
    });

    it("builds run path without session when session is null", () => {
        const path = buildRunPath({
            dandiset_id: "001469",
            subject: "Chronic-Implant-2",
            session: null,
            pipeline: "aind+ephys",
            version: "v1.0.0+fixes+20abeb6",
            params: "98fd947",
            config: "6568dda",
            attempt: 1,
        });
        expect(path).toBe(
            "derivatives/dandiset-001469/sub-Chronic-Implant-2/pipeline-aind+ephys/version-v1.0.0+fixes+20abeb6_params-98fd947_config-6568dda_attempt-1"
        );
    });

    it("builds run path from dandi_path when subject/session fields are absent", () => {
        const path = buildRunPath({
            dandiset_id: "001747",
            dandi_path: "sub-chip19894/ses-recording049",
            pipeline: "aind+ephys",
            version: "v1.1.1+b268fd2",
            params: "98fd947",
            config: "6568dda",
            attempt: 1,
        });
        expect(path).toBe(
            "derivatives/dandiset-001747/sub-chip19894/ses-recording049/pipeline-aind+ephys/version-v1.1.1+b268fd2_params-98fd947_config-6568dda_attempt-1"
        );
    });

    it("parses JSONL queue entries into run objects", () => {
        const entries = [
            {
                dandiset_id: "000233",
                subject: "CGM3",
                session: "CGM3",
                pipeline: "aind+ephys",
                version: "v1.0.0+fixes+20abeb6",
                params: "98fd947",
                config: "6568dda",
                attempt: 1,
                has_code: true,
                has_output: false,
                has_logs: true,
            },
            {
                dandiset_id: "001469",
                subject: "Chronic-Implant-2",
                session: null,
                pipeline: "aind+ephys",
                version: "v1.0.0+fixes+20abeb6",
                params: "aa073df",
                config: "6568dda",
                attempt: 1,
                has_code: true,
                has_output: false,
                has_logs: false,
            },
        ];
        const runs = parseQueueEntries(entries);
        expect(runs).toHaveLength(2);
        expect(runs[0]).toMatchObject({
            dandisetId: "000233",
            subject: "CGM3",
            session: "CGM3",
            pipelineName: "aind+ephys",
            pipelineVersion: "v1.0.0+fixes+20abeb6",
            paramsProfile: "98fd947",
            configHash: "6568dda",
            attempt: 1,
            hasCode: true,
            hasOutput: false,
            hasLogs: true,
        });
        expect(runs[1].session).toBeNull();
        expect(runs[1].hasLogs).toBe(false);
        // null session should not appear in path
        expect(runs[1].path).not.toContain("ses-");
    });

    it("parses content_id from JSONL entries into run objects", () => {
        const entries = [
            {
                dandiset_id: "000233",
                subject: "CGM3",
                session: "CGM3",
                pipeline: "aind+ephys",
                version: "v1.0.0+fixes+20abeb6",
                params: "98fd947",
                config: "6568dda",
                attempt: 1,
                has_code: true,
                has_output: true,
                has_logs: true,
                content_id: "abcdef1234567890abcdef1234567890abcdef12",
            },
            {
                dandiset_id: "001469",
                subject: "Chronic-Implant-2",
                session: null,
                pipeline: "aind+ephys",
                version: "v1.0.0+fixes+20abeb6",
                params: "aa073df",
                config: "6568dda",
                attempt: 1,
                has_code: true,
                has_output: false,
                has_logs: false,
            },
        ];
        const runs = parseQueueEntries(entries);
        expect(runs[0].contentHash).toBe("abcdef1234567890abcdef1234567890abcdef12");
        expect(runs[1].contentHash).toBeNull();
    });

    it("parses subject and session from dandi_path when queue entry omits fields", () => {
        const entries = [
            {
                dandiset_id: "001747",
                dandi_path: "sub-chip19894/ses-recording049",
                pipeline: "aind+ephys",
                version: "v1.1.1+b268fd2",
                params: "98fd947",
                config: "6568dda",
                attempt: 1,
                has_code: true,
                has_output: false,
                has_logs: true,
            },
        ];

        const runs = parseQueueEntries(entries);
        expect(runs).toHaveLength(1);
        expect(runs[0]).toMatchObject({
            subject: "chip19894",
            session: "recording049",
            path: "derivatives/dandiset-001747/sub-chip19894/ses-recording049/pipeline-aind+ephys/version-v1.1.1+b268fd2_params-98fd947_config-6568dda_attempt-1",
        });
    });
});

describe("Neurosift URL helpers", () => {
    it("neurosiftBlobUrl builds correct S3 blob URL", () => {
        const hash = "abcdef1234567890abcdef1234567890abcdef12";
        const url = neurosiftBlobUrl(hash);
        expect(url).toBe(
            "https://neurosift.app/nwb?url=" +
                encodeURIComponent(
                    "https://dandiarchive.s3.amazonaws.com/blobs/abc/def/abcdef1234567890abcdef1234567890abcdef12"
                )
        );
    });

    it("neurosiftDandisetUrl builds correct dandiset URL", () => {
        expect(neurosiftDandisetUrl("000233")).toBe("https://neurosift.app/dandiset/000233");
    });

    it("neurosiftSessionUrl prefers blob URL when contentHash is present", () => {
        const hash = "abcdef1234567890abcdef1234567890abcdef12";
        const url = neurosiftSessionUrl("000233", hash, "some-asset-id");
        expect(url).toContain("neurosift.app/nwb?url=");
        expect(url).toContain(encodeURIComponent("dandiarchive.s3.amazonaws.com/blobs/abc/def/"));
    });

    it("neurosiftSessionUrl falls back to DANDI API URL when contentHash is absent", () => {
        const url = neurosiftSessionUrl("000233", null, "some-asset-id");
        expect(url).toContain("dandiarchive.org");
        expect(url).toContain("some-asset-id");
    });

    it("neurosiftSessionUrl returns null when neither contentHash nor assetId is available", () => {
        expect(neurosiftSessionUrl("000233", null, null)).toBeNull();
    });
});

describe("fetchQueueState ETag caching", () => {
    const SAMPLE_ENTRY = {
        dandiset_id: "001697",
        subject: "sub1",
        session: "ses1",
        pipeline: "ephys",
        version: "v1",
        params: "abc",
        config: "def",
        attempt: 1,
        has_code: true,
        has_output: false,
        has_logs: false,
    };
    const JSONL_TEXT = JSON.stringify(SAMPLE_ENTRY);

    let originalFetch;

    beforeEach(() => {
        sessionStorage.clear();
        originalFetch = global.fetch;
        global.DecompressionStream = MockDecompressionStream;
    });

    afterEach(() => {
        global.fetch = originalFetch;
        delete global.DecompressionStream;
        sessionStorage.clear();
    });

    it("fetches, decompresses, parses, and caches ETag on first load", async () => {
        global.fetch = vi.fn().mockResolvedValue(
            new Response(makeReadableStream(JSONL_TEXT), {
                status: 200,
                headers: { ETag: '"etag-v1"' },
            })
        );

        const result = await fetchQueueState();

        expect(result).toHaveLength(1);
        expect(result[0].dandiset_id).toBe("001697");

        // ETag and decompressed body must be stored in sessionStorage
        const stored = JSON.parse(sessionStorage.getItem(QUEUE_STATE_CACHE_KEY));
        expect(stored.etag).toBe('"etag-v1"');
        expect(stored.body).toBe(JSONL_TEXT);

        // First request must not include If-None-Match
        const [, init] = global.fetch.mock.calls[0];
        expect(init.headers.get("If-None-Match")).toBeNull();
    });

    it("sends If-None-Match and returns cached body on 304", async () => {
        sessionStorage.setItem(QUEUE_STATE_CACHE_KEY, JSON.stringify({ etag: '"etag-v1"', body: JSONL_TEXT }));

        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 304 }));

        const result = await fetchQueueState();

        expect(result).toHaveLength(1);
        expect(result[0].dandiset_id).toBe("001697");

        // Must send the cached ETag
        const [, init] = global.fetch.mock.calls[0];
        expect(init.headers.get("If-None-Match")).toBe('"etag-v1"');
    });

    it("skips DecompressionStream entirely on 304 cache hit", async () => {
        sessionStorage.setItem(QUEUE_STATE_CACHE_KEY, JSON.stringify({ etag: '"etag-v1"', body: JSONL_TEXT }));

        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 304 }));
        const decompressionSpy = vi.fn();
        global.DecompressionStream = decompressionSpy;

        await fetchQueueState();

        expect(decompressionSpy).not.toHaveBeenCalled();
    });

    it("throws rate-limit error on HTTP 403", async () => {
        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 403 }));
        await expect(fetchQueueState()).rejects.toThrow("rate limit");
    });

    it("throws rate-limit error on HTTP 429", async () => {
        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 429 }));
        await expect(fetchQueueState()).rejects.toThrow("rate limit");
    });

    it("throws generic error on other HTTP failures", async () => {
        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 500 }));
        await expect(fetchQueueState()).rejects.toThrow("HTTP 500");
    });

    it("throws when DecompressionStream is unavailable", async () => {
        delete global.DecompressionStream;
        global.fetch = vi.fn().mockResolvedValue(new Response(makeReadableStream(JSONL_TEXT), { status: 200 }));
        await expect(fetchQueueState()).rejects.toThrow("DecompressionStream");
    });
});

describe("renderVisualizationSection", () => {
    it("renders a gallery section with recording and image data", () => {
        const recordings = [
            {
                name: "block0_acquisition-ElectricalSeriesRaw_recording1",
                images: [
                    { name: "drift_map.png", url: "https://cdn.example.com/drift_map.png" },
                    { name: "motion.png", url: "https://cdn.example.com/motion.png" },
                ],
            },
        ];

        const html = renderVisualizationSection(recordings);

        expect(html).toContain("Visualizations");
        expect(html).toContain("2"); // count badge
        expect(html).toContain("block0_acquisition-ElectricalSeriesRaw_recording1");
        expect(html).toContain("viz-recording");
        expect(html).toContain("viz-grid");
        expect(html).toContain("viz-figure");
        expect(html).toContain("viz-img");
        expect(html).toContain("drift_map.png");
        expect(html).toContain("motion.png");
        // images open in a modal, not a new tab
        expect(html).toContain("viz-link");
        expect(html).toContain("data-viz-url=");
        expect(html).toContain("data-viz-label=");
        expect(html).not.toContain('target="_blank"');
    });

    it("renders captions with underscores replaced by spaces", () => {
        const recordings = [
            {
                name: "recording1",
                images: [{ name: "traces_full_seg0.png", url: "https://cdn.example.com/traces_full_seg0.png" }],
            },
        ];

        const html = renderVisualizationSection(recordings);
        expect(html).toContain("traces full seg0");
    });

    it("escapes HTML in recording names and image names", () => {
        const recordings = [
            {
                name: '<script>alert("xss")</script>',
                images: [{ name: "evil<img>.png", url: "https://cdn.example.com/evil.png" }],
            },
        ];

        const html = renderVisualizationSection(recordings);
        expect(html).not.toContain("<script>");
        expect(html).toContain("&lt;script&gt;");
    });

    it("sums image counts across multiple recordings", () => {
        const recordings = [
            {
                name: "rec1",
                images: [
                    { name: "a.png", url: "url1" },
                    { name: "b.png", url: "url2" },
                ],
            },
            { name: "rec2", images: [{ name: "c.png", url: "url3" }] },
        ];

        const html = renderVisualizationSection(recordings);
        expect(html).toContain(">3<"); // total image count badge
    });
});

describe("fetchVisualizationData", () => {
    let originalFetch;

    beforeEach(() => {
        sessionStorage.clear();
        originalFetch = global.fetch;
    });

    afterEach(() => {
        global.fetch = originalFetch;
        sessionStorage.clear();
    });

    it("returns null when the visualization directory fetch fails", async () => {
        global.fetch = vi.fn().mockResolvedValue(new Response(null, { status: 404 }));
        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );
        expect(result).toBeNull();
    });

    it("returns null when the visualization directory has no subdirectories", async () => {
        const vizDirItems = [{ type: "file", name: "visualization_output.json", sha: "abc123" }];
        global.fetch = vi.fn().mockResolvedValue(
            new Response(JSON.stringify(vizDirItems), {
                status: 200,
                headers: { "Content-Type": "application/json" },
            })
        );
        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );
        expect(result).toBeNull();
    });

    it("returns recordings with image data from the GitHub API", async () => {
        const vizDirItems = [
            { type: "dir", name: "recording1", sha: "dir-sha-1" },
            { type: "file", name: "visualization_output.json", sha: "file-sha" },
        ];
        const treeData = {
            tree: [
                { type: "blob", path: "drift_map.png" },
                { type: "blob", path: "motion.png" },
                { type: "tree", path: "subdir" },
            ],
        };

        global.fetch = vi
            .fn()
            .mockResolvedValueOnce(
                new Response(JSON.stringify(vizDirItems), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            )
            .mockResolvedValueOnce(
                new Response(JSON.stringify(treeData), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            );

        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );

        expect(result).not.toBeNull();
        expect(result).toHaveLength(1);
        expect(result[0].name).toBe("recording1");
        expect(result[0].images).toHaveLength(2);
        expect(result[0].images[0].name).toBe("drift_map.png");
        expect(result[0].images[1].name).toBe("motion.png");
        // Images should be CDN URLs
        expect(result[0].images[0].url).toContain("raw.githubusercontent.com");
        expect(result[0].images[0].url).toContain("/derivatives/visualization/");
        expect(result[0].images[0].url).toContain("drift_map.png");
    });

    it("falls back to legacy top-level visualization directory when derivatives layout is missing", async () => {
        const vizDirItems = [{ type: "dir", name: "recording1", sha: "dir-sha-1" }];
        const treeData = { tree: [{ type: "blob", path: "drift_map.png" }] };

        global.fetch = vi
            .fn()
            .mockResolvedValueOnce(new Response(null, { status: 404 }))
            .mockResolvedValueOnce(
                new Response(JSON.stringify(vizDirItems), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            )
            .mockResolvedValueOnce(
                new Response(JSON.stringify(treeData), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            );

        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );

        expect(result).not.toBeNull();
        expect(result[0].images[0].url).toContain("raw.githubusercontent.com");
        expect(result[0].images[0].url).toContain("/visualization/recording1/drift_map.png");
        expect(global.fetch.mock.calls[0][0]).toContain("/derivatives/visualization?");
        expect(global.fetch.mock.calls[1][0]).toContain("/visualization?ref=");
    });

    it("returns null when all recordings have no PNG images", async () => {
        const vizDirItems = [{ type: "dir", name: "recording1", sha: "dir-sha-1" }];
        const treeData = { tree: [{ type: "blob", path: "readme.txt" }] };

        global.fetch = vi
            .fn()
            .mockResolvedValueOnce(
                new Response(JSON.stringify(vizDirItems), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            )
            .mockResolvedValueOnce(
                new Response(JSON.stringify(treeData), { status: 200, headers: { "Content-Type": "application/json" } })
            );

        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );
        expect(result).toBeNull();
    });

    it("returns null on network error", async () => {
        global.fetch = vi.fn().mockRejectedValue(new Error("network error"));
        const result = await fetchVisualizationData(
            "derivatives/dandiset-000001/sub-A/pipeline-test/version-v1/params-abc_attempt-1"
        );
        expect(result).toBeNull();
    });
});

describe("renderFlatList", () => {
    const baseRun = {
        status: "success",
        attempt: 1,
        runDate: null,
        tasks: [],
        generatedBy: [],
        vizData: null,
        hasLogs: false,
        hasOutput: true,
        hasCode: true,
        path: "derivatives/dandiset-001697/sub-A/ses-S1/pipeline-ephys/version-v1/params-fast_config-abc_attempt-1",
        dandisetId: "001697",
        subject: "A",
        session: "S1",
        pipelineName: "ephys",
        pipelineVersion: "v1",
        paramsProfile: "fast",
        configHash: "abc",
        assetId: null,
        inSourcedata: false,
        failureStep: null,
    };

    it("wraps runs in a flat-list container", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain('class="flat-list"');
    });

    it("includes dandiset ID in each flat run entry", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("001697");
    });

    it("includes subject in each flat run entry", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("Sub:");
        expect(html).toContain(">A<");
    });

    it("includes session in each flat run entry when session is not null", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("Ses:");
        expect(html).toContain(">S1<");
    });

    it("omits session context when session is null", () => {
        const run = { ...baseRun, session: null };
        const html = renderFlatList([run]);
        expect(html).not.toContain("Ses:");
    });

    it("omits pipeline/version context from flat run header", () => {
        const html = renderFlatList([baseRun]);
        expect(html).not.toContain("flat-ctx-pipeline");
    });

    it("includes params profile in each flat run entry", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("Params:");
        expect(html).toContain("fast");
    });

    it("aliases known params hash to explicit registry name with source link", () => {
        const run = { ...baseRun, paramsProfile: "4af6a25" };
        const html = renderFlatList([run]);
        expect(html).toContain("Params:");
        expect(html).toContain(">deterministic<");
        expect(html).toContain(
            'href="https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json"'
        );
    });

    it("aliases known config hash to explicit registry name with source link", () => {
        const run = { ...baseRun, configHash: "0d4bf36" };
        const html = renderFlatList([run]);
        const container = document.createElement("div");
        container.innerHTML = html;

        const configLink = [...container.querySelectorAll(".flat-ctx-text .src-link")].find(
            (link) => link.textContent === "v1"
        );

        expect(container.textContent).toContain("Config:");
        expect(configLink).toBeTruthy();
        expect(configLink?.href).toMatch(
            /^https:\/\/github\.com\/dandi-compute\/code\/blob\/main\/src\/dandi_compute_code\/aind_ephys_pipeline\/configs\/.+\.config$/
        );
    });

    it("shows attempt number", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("Attempt");
        expect(html).toContain("1");
    });

    it("renders multiple runs", () => {
        const run2 = { ...baseRun, dandisetId: "000233", subject: "B", attempt: 2 };
        const html = renderFlatList([baseRun, run2]);
        expect(html).toContain("001697");
        expect(html).toContain("000233");
    });

    it("applies correct status class for success", () => {
        const html = renderFlatList([baseRun]);
        expect(html).toContain("status-success");
    });

    it("applies correct status class for failed", () => {
        const run = { ...baseRun, status: "failed" };
        const html = renderFlatList([run]);
        expect(html).toContain("status-failed");
    });
});

describe("renderDandisets", () => {
    it("lists jobs directly under a session without version/config nested groups", () => {
        const run1 = {
            status: "success",
            attempt: 1,
            runDate: null,
            tasks: [],
            generatedBy: [],
            vizData: null,
            hasLogs: false,
            hasOutput: true,
            hasCode: true,
            path: "derivatives/dandiset-001697/sub-A/ses-S1/pipeline-ephys/version-v1/params-fast_config-abc_attempt-1",
            dandisetId: "001697",
            subject: "A",
            session: "S1",
            pipelineName: "ephys",
            pipelineVersion: "v1",
            paramsProfile: "fast",
            configHash: "abc",
            assetId: null,
            inSourcedata: false,
            failureStep: null,
        };
        const run2 = {
            ...run1,
            status: "failed",
            attempt: 2,
            path: "derivatives/dandiset-001697/sub-A/ses-S1/pipeline-spike/version-v2/params-slow_config-def_attempt-2",
            pipelineName: "spike",
            pipelineVersion: "v2",
            paramsProfile: "slow",
            configHash: "def",
        };

        const html = renderDandisets([run1, run2]);
        expect(html).toContain("2&nbsp;jobs");
        expect(html).not.toContain("pipeline-version-group");
        expect(html).not.toContain("params-group");
        expect((html.match(/class="run-entry status-/g) || []).length).toBe(2);
    });
});

describe("renderParamsGroup", () => {
    it("aliases params and config hashes to explicit registry names with source links", () => {
        const html = renderParamsGroup("4af6a25", "0d4bf36", []);
        expect(html).toContain(">deterministic<");
        expect(html).toContain(">v1<");
        expect(html).toContain(
            'href="https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json"'
        );
        expect(html).toContain(
            'href="https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-1.config"'
        );
    });
});

describe("renderRegistryLink", () => {
    it("falls back to escaped hash display for unknown entries", () => {
        expect(renderRegistryLink("Params", '<script>alert("xss")</script>', [], "params")).toBe(
            "Params:&nbsp;&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;"
        );
    });
});

describe("diff page helpers", () => {
    it("builds GitHub compare URLs and summaries from expanded pipeline commit refs", async () => {
        const originalFetch = global.fetch;
        try {
            global.fetch = vi
                .fn()
                .mockResolvedValueOnce(
                    new Response(JSON.stringify({ sha: "20abeb66850ec6ce0127c1489c22bd949d9bb642" }), {
                        status: 200,
                        headers: { "Content-Type": "application/json" },
                    })
                )
                .mockResolvedValueOnce(
                    new Response(JSON.stringify({ sha: "b268fd207886905b40a956e7f6a839884ce9835f" }), {
                        status: 200,
                        headers: { "Content-Type": "application/json" },
                    })
                )
                .mockResolvedValueOnce(
                    new Response(
                        JSON.stringify({
                            ahead_by: 1,
                            behind_by: 0,
                            total_commits: 1,
                            commits: [
                                {
                                    sha: "b268fd207886905b40a956e7f6a839884ce9835f",
                                    commit: { message: "Minor 1.1.1 release (#102)" },
                                },
                            ],
                            files: [{ filename: "pyproject.toml", status: "modified" }],
                        }),
                        {
                            status: 200,
                            headers: { "Content-Type": "application/json" },
                        }
                    )
                );

            const pairs = await buildPipelineDiffPairs([
                { pipelineName: "aind+ephys", pipelineVersion: "v1.0.1+20abeb6" },
                { pipelineName: "aind+ephys", pipelineVersion: "v1.0.1+20abeb6" },
                { pipelineName: "aind+ephys", pipelineVersion: "v1.1.1+b268fd2+5d20fd2" },
            ]);

            expect(pairs).toEqual([
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.0.1+20abeb6",
                    headVersion: "v1.1.1+b268fd2+5d20fd2",
                    compareUrl:
                        "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/20abeb66850ec6ce0127c1489c22bd949d9bb642...b268fd207886905b40a956e7f6a839884ce9835f",
                    modalHtml: expect.stringContaining("Pipeline version"),
                },
            ]);
            expect(pairs[0].modalHtml).toContain("Minor 1.1.1 release (#102)");
            expect(pairs[0].modalHtml).toContain('<th scope="col">v1.0.1-20abeb6</th>');
            expect(pairs[0].modalHtml).toContain('<th scope="col">v1.1.1-b268fd2-5d20fd2</th>');

            expect(global.fetch).toHaveBeenCalledTimes(3);
        } finally {
            global.fetch = originalFetch;
        }
    });

    it("omits pipeline compares when version suffixes resolve to the same pipeline commit", async () => {
        const originalFetch = global.fetch;
        try {
            global.fetch = vi.fn().mockResolvedValue(
                new Response(JSON.stringify({ sha: "b268fd207886905b40a956e7f6a839884ce9835f" }), {
                    status: 200,
                    headers: { "Content-Type": "application/json" },
                })
            );

            const pairs = await buildPipelineDiffPairs([
                { pipelineName: "aind+ephys", pipelineVersion: "v1.1.1+b268fd2" },
                { pipelineName: "aind+ephys", pipelineVersion: "v1.1.1+b268fd2+3fac55c" },
            ]);

            expect(pairs).toEqual([]);
        } finally {
            global.fetch = originalFetch;
        }
    });

    it("lists unique pipeline entries in sorted order", () => {
        expect(
            uniquePipelineEntries([
                { pipelineName: "aind+ephys", pipelineVersion: "v1.1.0+def5678" },
                { pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
                { pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
            ])
        ).toEqual([
            { key: "v1.0.0+abc1234", pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
            { key: "v1.1.0+def5678", pipelineName: "aind+ephys", pipelineVersion: "v1.1.0+def5678" },
        ]);
    });

    it("handles empty and single-entry pipeline grids", () => {
        expect(uniquePipelineEntries([])).toEqual([]);
        expect(uniquePipelineEntries([{ pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" }])).toEqual([
            { key: "v1.0.0+abc1234", pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
        ]);
    });

    it("collects nested JSON differences with stable paths", () => {
        expect(
            collectJsonDiffs(
                { sorter: { detect_sign: false }, streams: ["ap"] },
                { sorter: { detect_sign: true }, streams: ["ap", "lf"] }
            )
        ).toEqual([
            { path: "sorter.detect_sign", left: false, right: true },
            { path: "streams.1", left: undefined, right: "lf" },
        ]);
    });

    it("expands config text diffs to include a +/- 3 line context window", () => {
        expect(
            collectTextDiffs(
                ["line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8"].join("\n"),
                ["line 1", "line 2", "line 3", "line 4 changed", "line 5", "line 6", "line 7", "line 8"].join("\n")
            )
        ).toEqual([
            {
                path: "lines 1-7",
                left: [
                    "1   line 1",
                    "2   line 2",
                    "3   line 3",
                    "4 - line 4",
                    "5   line 5",
                    "6   line 6",
                    "7   line 7",
                ].join("\n"),
                right: [
                    "1   line 1",
                    "2   line 2",
                    "3   line 3",
                    "4 + line 4 changed",
                    "5   line 5",
                    "6   line 6",
                    "7   line 7",
                ].join("\n"),
            },
        ]);
    });

    it("handles context windows at boundaries and across multiple changes", () => {
        expect(
            collectTextDiffs(
                ["line 1", "line 2", "line 3", "line 4", "line 5"].join("\n"),
                ["line 1 changed", "line 2", "line 3", "line 4", "line 5 changed"].join("\n")
            )
        ).toEqual([
            {
                path: "lines 1-5",
                left: ["1 - line 1", "2   line 2", "3   line 3", "4   line 4", "5 - line 5"].join("\n"),
                right: ["1 + line 1 changed", "2   line 2", "3   line 3", "4   line 4", "5 + line 5 changed"].join(
                    "\n"
                ),
            },
        ]);
    });

    it("renders pipeline compare links and params diff summaries", () => {
        const html = renderDiffPage({
            pipelineEntries: [
                { key: "v1.0.0+abc1234", pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
                { key: "v1.1.0+def5678", pipelineName: "aind+ephys", pipelineVersion: "v1.1.0+def5678" },
                { key: "v1.2.0+fedcba9", pipelineName: "aind+ephys", pipelineVersion: "v1.2.0+fedcba9" },
            ],
            pipelinePairs: [
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.0.0+abc1234",
                    headVersion: "v1.1.0+def5678",
                    compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                    modalHtml: "<p>1 commit · 1 file</p>",
                },
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.0.0+abc1234",
                    headVersion: "v1.2.0+fedcba9",
                    compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...fedcba9",
                    modalHtml: "<p>2 commits · 2 files</p>",
                },
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.1.0+def5678",
                    headVersion: "v1.2.0+fedcba9",
                    compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/def5678...fedcba9",
                    modalHtml: "<p>1 commit · 1 file</p>",
                },
            ],
            pipelinePairMap: new Map([
                [
                    "v1.0.0+abc1234\x00v1.1.0+def5678",
                    {
                        compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                        modalHtml: "<p>1 commit · 1 file</p>",
                    },
                ],
                [
                    "v1.0.0+abc1234\x00v1.2.0+fedcba9",
                    {
                        compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...fedcba9",
                        modalHtml: "<p>2 commits · 2 files</p>",
                    },
                ],
                [
                    "v1.1.0+def5678\x00v1.2.0+fedcba9",
                    {
                        compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/def5678...fedcba9",
                        modalHtml: "<p>1 commit · 1 file</p>",
                    },
                ],
            ]),
            paramsEntries: [
                {
                    key: "deterministic",
                    alias: "deterministic",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json",
                },
                {
                    key: "original",
                    alias: "original",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-original.json",
                },
            ],
            paramsPairs: [
                {
                    baseAlias: "deterministic",
                    headAlias: "original",
                    baseSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json",
                    headSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-original.json",
                    changes: [{ path: "sorter.detect_sign", left: false, right: true }],
                },
            ],
            paramsPairMap: new Map([
                [
                    "deterministic\x00original",
                    {
                        baseAlias: "deterministic",
                        headAlias: "original",
                        changes: [{ path: "sorter.detect_sign", left: false, right: true }],
                    },
                ],
            ]),
        });

        expect(html).toContain("Pipeline GitHub compares");
        expect(html).toContain('class="diff-matrix"');
        expect(html).toContain('class="diff-section-banner"');
        expect(html).toContain('class="diff-cell-trigger"');
        expect(html).toContain("Registered params JSON diffs");
        expect(html).not.toContain("Quick links for pipeline GitHub comparisons");
        expect(html).toContain("View 1 change");
        expect(html).not.toContain('<details class="run-section" open>');
        expect(html).toContain('class="diff-matrix-cell diff-matrix-cell-empty"');
        expect(html).not.toContain('class="count-badge"');
        expect((html.match(/class="diff-matrix-col-header"/g) ?? []).length).toBe(3);

        document.body.innerHTML = html;
        const pipelineRows = document.querySelector(".diff-matrix").querySelectorAll("tbody tr");
        expect(pipelineRows[0].querySelectorAll(".diff-matrix-cell-empty")).toHaveLength(2);
        expect(pipelineRows[0].querySelectorAll(".diff-matrix-cell .diff-cell-trigger")).toHaveLength(0);
        expect(pipelineRows[1].querySelectorAll(".diff-matrix-cell-empty")).toHaveLength(1);
        expect(pipelineRows[1].querySelectorAll(".diff-matrix-cell .diff-cell-trigger")).toHaveLength(1);
        expect(pipelineRows[2].querySelectorAll(".diff-matrix-cell-empty")).toHaveLength(0);
        expect(pipelineRows[2].querySelectorAll(".diff-matrix-cell .diff-cell-trigger")).toHaveLength(2);
    });

    it("renders config diff summaries in a comparison matrix", () => {
        const html = renderDiffPage({
            pipelineEntries: [],
            pipelinePairs: [],
            pipelinePairMap: new Map(),
            paramsEntries: [],
            paramsPairs: [],
            paramsPairMap: new Map(),
            configEntries: [
                {
                    key: "v0",
                    alias: "v0",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-0.config",
                },
                {
                    key: "v1",
                    alias: "v1",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-1.config",
                },
            ],
            configPairs: [
                {
                    baseAlias: "v0",
                    headAlias: "v1",
                    baseSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-0.config",
                    headSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-1.config",
                    changes: [
                        {
                            path: "lines 22-26",
                            left: [
                                "22   process {",
                                "23       withName:foo {",
                                "24 -         cpus=4",
                                "25       }",
                                "26   }",
                            ].join("\n"),
                            right: [
                                "22   process {",
                                "23       withName:foo {",
                                "24 +         cpus=1",
                                "25       }",
                                "26   }",
                            ].join("\n"),
                        },
                    ],
                },
            ],
            configPairMap: new Map([
                [
                    "v0\x00v1",
                    {
                        baseAlias: "v0",
                        headAlias: "v1",
                        changes: [
                            {
                                path: "lines 22-26",
                                left: [
                                    "22   process {",
                                    "23       withName:foo {",
                                    "24 -         cpus=4",
                                    "25       }",
                                    "26   }",
                                ].join("\n"),
                                right: [
                                    "22   process {",
                                    "23       withName:foo {",
                                    "24 +         cpus=1",
                                    "25       }",
                                    "26   }",
                                ].join("\n"),
                            },
                        ],
                    },
                ],
            ]),
        });

        expect(html).toContain("Registered config diffs");
        expect(html).toContain("View 1 change");
        expect(html).toContain("No registered params files were found.");
    });
});

describe("diff modal interactions", () => {
    beforeEach(() => {
        document.body.innerHTML = `
            <div id="runs"></div>
            <div id="log-modal" class="log-modal-overlay" hidden>
                <div class="log-modal-box" role="dialog" aria-modal="true" aria-labelledby="log-modal-title">
                    <div class="log-modal-header">
                        <span id="log-modal-title" class="log-modal-title"></span>
                        <div class="log-modal-actions">
                            <a id="log-modal-external" href="#" class="log-modal-btn-external" target="_blank" rel="noopener"
                                >↗ Open</a
                            >
                            <button id="log-modal-close" class="log-modal-btn-close" aria-label="Close">✕</button>
                        </div>
                    </div>
                    <div id="log-modal-body" class="log-modal-body"></div>
                </div>
            </div>
        `;
    });

    it("opens diff cell content inside the shared modal", () => {
        document.getElementById("runs").innerHTML = renderDiffPage({
            pipelineEntries: [
                { key: "v1.0.0+abc1234", pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
                { key: "v1.1.0+def5678", pipelineName: "aind+ephys", pipelineVersion: "v1.1.0+def5678" },
            ],
            pipelinePairs: [
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.0.0+abc1234",
                    headVersion: "v1.1.0+def5678",
                    compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                    modalHtml: "<p>1 commit · 1 file</p>",
                },
            ],
            pipelinePairMap: new Map([
                [
                    "v1.0.0+abc1234\x00v1.1.0+def5678",
                    {
                        compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                        modalHtml: "<p>1 commit · 1 file</p>",
                    },
                ],
            ]),
            paramsEntries: [
                {
                    key: "deterministic",
                    alias: "deterministic",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json",
                },
                {
                    key: "original",
                    alias: "original",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-original.json",
                },
            ],
            paramsPairs: [
                {
                    baseAlias: "deterministic",
                    headAlias: "original",
                    baseSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json",
                    headSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-original.json",
                    changes: [{ path: "sorter.detect_sign", left: false, right: true }],
                },
            ],
            paramsPairMap: new Map([
                [
                    "deterministic\x00original",
                    {
                        baseAlias: "deterministic",
                        headAlias: "original",
                        changes: [{ path: "sorter.detect_sign", left: false, right: true }],
                    },
                ],
            ]),
        });

        initModal();
        document.querySelectorAll(".diff-cell-trigger")[1].click();

        expect(document.getElementById("log-modal").hidden).toBe(false);
        expect(document.getElementById("log-modal-title").hidden).toBe(true);
        expect(document.getElementById("log-modal-body").innerHTML).not.toContain("Registered params");
        expect(document.getElementById("log-modal-body").innerHTML).toContain(
            '<th scope="col"><a class="diff-inline-link" href="https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-deterministic.json" target="_blank" rel="noopener">deterministic</a></th>'
        );
        expect(document.getElementById("log-modal-body").innerHTML).toContain(
            '<th scope="col"><a class="diff-inline-link" href="https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/params/name-original.json" target="_blank" rel="noopener">original</a></th>'
        );
        expect(document.getElementById("log-modal-body").innerHTML).toContain('<th scope="col">Parameter</th>');
        expect(document.getElementById("log-modal-body").textContent).toContain("sorter.detect_sign");
        expect(document.getElementById("log-modal-body").textContent).not.toContain("− false");
        expect(document.getElementById("log-modal-body").textContent).not.toContain("+ true");
        expect(document.getElementById("log-modal-body").textContent).toContain("false");
        expect(document.getElementById("log-modal-body").textContent).toContain("true");
        expect(document.getElementById("log-modal-body").querySelectorAll("table")).toHaveLength(1);
        expect(document.getElementById("log-modal-external").hidden).toBe(true);
    });

    it("opens config diff content inside the shared modal", () => {
        document.getElementById("runs").innerHTML = renderDiffPage({
            pipelineEntries: [],
            pipelinePairs: [],
            pipelinePairMap: new Map(),
            paramsEntries: [],
            paramsPairs: [],
            paramsPairMap: new Map(),
            configEntries: [
                {
                    key: "v0",
                    alias: "v0",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-0.config",
                },
                {
                    key: "v1",
                    alias: "v1",
                    sourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-1.config",
                },
            ],
            configPairs: [
                {
                    baseAlias: "v0",
                    headAlias: "v1",
                    baseSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-0.config",
                    headSourceUrl:
                        "https://github.com/dandi-compute/code/blob/main/src/dandi_compute_code/aind_ephys_pipeline/configs/name-mit+engaging_revision-1.config",
                    changes: [
                        {
                            path: "lines 22-26",
                            left: [
                                "22   process {",
                                "23       withName:foo {",
                                "24 -         cpus=4",
                                "25       }",
                                "26   }",
                            ].join("\n"),
                            right: [
                                "22   process {",
                                "23       withName:foo {",
                                "24 +         cpus=1",
                                "25       }",
                                "26   }",
                            ].join("\n"),
                        },
                    ],
                },
            ],
            configPairMap: new Map([
                [
                    "v0\x00v1",
                    {
                        baseAlias: "v0",
                        headAlias: "v1",
                        changes: [
                            {
                                path: "lines 22-26",
                                left: [
                                    "22   process {",
                                    "23       withName:foo {",
                                    "24 -         cpus=4",
                                    "25       }",
                                    "26   }",
                                ].join("\n"),
                                right: [
                                    "22   process {",
                                    "23       withName:foo {",
                                    "24 +         cpus=1",
                                    "25       }",
                                    "26   }",
                                ].join("\n"),
                            },
                        ],
                    },
                ],
            ]),
        });

        initModal();
        document.querySelector(".diff-cell-trigger").click();

        expect(document.getElementById("log-modal").hidden).toBe(false);
        expect(document.getElementById("log-modal-body").innerHTML).toContain('<th scope="col">Config snippet</th>');
        expect(document.getElementById("log-modal-body").textContent).toContain("lines 22-26");
        expect(document.getElementById("log-modal-body").textContent).toContain("cpus=4");
        expect(document.getElementById("log-modal-body").textContent).toContain("cpus=1");
        expect(document.getElementById("log-modal-body").querySelectorAll(".diff-config-line-changed")).toHaveLength(2);
        expect(
            document
                .getElementById("log-modal-body")
                .querySelectorAll(".diff-config-line:not(.diff-config-line-changed)").length
        ).toBeGreaterThan(0);
    });

    it("shows pipeline compare modal details in tables", () => {
        document.getElementById("runs").innerHTML = renderDiffPage({
            pipelineEntries: [
                { key: "v1.0.0+abc1234", pipelineName: "aind+ephys", pipelineVersion: "v1.0.0+abc1234" },
                { key: "v1.1.0+def5678", pipelineName: "aind+ephys", pipelineVersion: "v1.1.0+def5678" },
            ],
            pipelinePairs: [
                {
                    pipelineName: "aind+ephys",
                    baseVersion: "v1.0.0+abc1234",
                    headVersion: "v1.1.0+def5678",
                    compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                    modalHtml: "",
                },
            ],
            pipelinePairMap: new Map([
                [
                    "v1.0.0+abc1234\x00v1.1.0+def5678",
                    {
                        compareUrl: "https://github.com/CodyCBakerPhD/aind-ephys-pipeline/compare/abc1234...def5678",
                        modalHtml:
                            '<div class="diff-pair-card"><table class="diff-detail-table diff-detail-table-pair"><thead><tr><th class="diff-detail-corner" aria-hidden="true"></th><th scope="col">v1.0.0-abc1234</th><th scope="col">v1.1.0-def5678</th></tr></thead><tbody><tr><th scope="row" class="diff-detail-key">Pipeline version</th><td>v1.0.0-abc1234</td><td>v1.1.0-def5678</td></tr></tbody></table><table class="diff-detail-table"><thead><tr><th scope="col">Metric</th><th scope="col">Value</th></tr></thead><tbody><tr><th scope="row" class="diff-detail-key">Commits</th><td>1 commit</td></tr></tbody></table></div>',
                    },
                ],
            ]),
            paramsEntries: [],
            paramsPairs: [],
            paramsPairMap: new Map(),
        });

        initModal();
        document.querySelector(".diff-cell-trigger").click();

        expect(document.getElementById("log-modal").hidden).toBe(false);
        expect(document.getElementById("log-modal-title").hidden).toBe(true);
        expect(document.getElementById("log-modal-body").innerHTML).toContain("diff-detail-table");
        expect(document.getElementById("log-modal-body").innerHTML).toContain("v1.0.0-abc1234");
        expect(document.getElementById("log-modal-body").innerHTML).toContain("v1.1.0-def5678");
        expect(document.getElementById("log-modal-body").textContent).toContain("Commits");
    });

    it("hides the external action when opening inline-only modal content", () => {
        openHtmlModal("Params diff", "<p>Only modal content</p>");

        expect(document.getElementById("log-modal").hidden).toBe(false);
        expect(document.getElementById("log-modal-body").innerHTML).toContain("Only modal content");
        expect(document.getElementById("log-modal-external").hidden).toBe(true);
    });
});

describe("initLayoutToggle", () => {
    beforeEach(() => {
        document.body.innerHTML = '<div id="layout-bar"></div><div id="runs"></div>';
        window.history.replaceState(null, "", "/?view=tests&dandiset=001697");
    });

    it("updates URL layout query param while preserving other URL state", () => {
        initLayoutToggle();
        document.querySelector('[data-layout="tree"]').click();
        document.querySelector('[data-layout="flat"]').click();

        const params = new URLSearchParams(window.location.search);
        expect(params.get("layout")).toBe("flat");
        expect(params.get("view")).toBe("tests");
        expect(params.get("dandiset")).toBe("001697");
        expect(localStorage.getItem("layoutMode")).toBe("flat");
    });
});
