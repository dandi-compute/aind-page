const { applyFilter, buildRunPath, classifyFailedTaskStep, parseQueueEntries, parseRunPath, parseTrace, runFailureStep } = require("./app");

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
            "derivatives/dandiset-000233/sub-CGM3/ses-CGM3/pipeline-aind+ephys/version-v1.0.0+fixes+20abeb6/params-98fd947_config-6568dda_attempt-1"
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
            "derivatives/dandiset-001469/sub-Chronic-Implant-2/pipeline-aind+ephys/version-v1.0.0+fixes+20abeb6/params-98fd947_config-6568dda_attempt-1"
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
