const { renderFilterBanner, renderSummary, showDiffResults, showError, showLoading, showResults } = require("./app");

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
                dandiCodebaseHash: "abc1234",
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
        expect(banner.innerHTML).toContain("Codebase:&nbsp;abc1234");
        expect(banner.innerHTML).toContain(`option value="${failedRun.paramsProfile}"`);
        expect(banner.innerHTML).toContain(`option value="${successfulRun.paramsProfile}"`);
        expect(banner.innerHTML).toContain(`option value="${failedRun.configHash}"`);
        expect(banner.innerHTML).toContain(`option value="${successfulRun.configHash}"`);
        expect(banner.innerHTML).toContain('option value="abc1234"');
        expect(banner.innerHTML).toContain('option value="def5678"');
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
                dandiCodebaseHash: null,
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
