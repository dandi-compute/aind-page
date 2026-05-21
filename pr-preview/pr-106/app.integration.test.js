const { renderFilterBanner, showDiffResults, showError, showLoading, showResults } = require("./app");

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
        renderFilterBanner(
            {
                dandisetId: "001697",
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: "4af6a25",
                configType: "0d4bf36",
                dandiCodebaseHash: "abc1234",
                failureStep: "pre-processing",
            },
            [
                {
                    dandisetId: "001697",
                    subject: "a",
                    session: "s1",
                    pipelineVersion: "v1",
                    paramsProfile: "4af6a25",
                    configHash: "0d4bf36",
                    generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "abc1234" }],
                    status: "failed",
                    failureStep: "pre-processing",
                },
                {
                    dandisetId: "001697",
                    subject: "b",
                    session: "s2",
                    pipelineVersion: "v2",
                    paramsProfile: "98fd947",
                    configHash: "6568dda",
                    generatedBy: [{ CodeURL: "https://github.com/dandi-compute/code", Version: "def5678" }],
                    status: "success",
                    failureStep: null,
                },
            ]
        );

        const banner = document.getElementById("filter-banner");
        expect(banner.style.display).toBe("");
        expect(banner.innerHTML).toContain("Filtered view:");
        expect(banner.innerHTML).toContain("Failed in pre-processing");
        expect(banner.innerHTML).toContain("Params:&nbsp;4af6a25");
        expect(banner.innerHTML).toContain("Config:&nbsp;0d4bf36");
        expect(banner.innerHTML).toContain("Codebase:&nbsp;abc1234");
        expect(banner.innerHTML).toContain('option value="4af6a25"');
        expect(banner.innerHTML).toContain('option value="98fd947"');
        expect(banner.innerHTML).toContain('option value="0d4bf36"');
        expect(banner.innerHTML).toContain('option value="6568dda"');
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
