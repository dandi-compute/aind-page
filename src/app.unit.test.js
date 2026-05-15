const {
    applyFilter,
    buildRunPath,
    classifyFailedTaskStep,
    fetchQueueState,
    fetchVisualizationData,
    parseQueueEntries,
    parseRunPath,
    parseTrace,
    renderVisualizationSection,
    runFailureStep,
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

describe("app unit behavior", () => {
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
                new Response(JSON.stringify(treeData), { status: 200, headers: { "Content-Type": "application/json" } })
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
        expect(result[0].images[0].url).toContain("drift_map.png");
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
