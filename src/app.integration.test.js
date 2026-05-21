const {
    loadAindPipelineRegistries,
    renderFilterBanner,
    showDiffResults,
    showError,
    showLoading,
    showResults,
} = require("./app");

const REGISTERED_PARAMS_FIXTURE = {
    deterministic: { path: "name-deterministic.json", md5: "4af6a25e20e376c81895ce9350a9cbd4" },
    default: { path: "name-deterministic.json", md5: "4af6a25e20e376c81895ce9350a9cbd4" },
    original: { path: "name-original.json", md5: "98fd947595f60b65812a4b0ea29b7141" },
};
const REGISTERED_CONFIGS_FIXTURE = {
    v1: { path: "name-mit+engaging_revision-1.config", md5: "0d4bf36ddb61418ae7714e7d6e5ff8b8" },
    default: { path: "name-mit+engaging_revision-1.config", md5: "0d4bf36ddb61418ae7714e7d6e5ff8b8" },
    v0: { path: "name-mit+engaging_revision-0.config", md5: "6568ddacdedabc7b855769340ed8874f" },
};

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

beforeEach(async () => {
    const originalFetch = global.fetch;
    global.fetch = vi
        .fn()
        .mockResolvedValueOnce(new Response(JSON.stringify(REGISTERED_PARAMS_FIXTURE), { status: 200 }))
        .mockResolvedValueOnce(new Response(JSON.stringify(REGISTERED_CONFIGS_FIXTURE), { status: 200 }));
    try {
        await loadAindPipelineRegistries();
    } finally {
        global.fetch = originalFetch;
    }
});

describe("app integration behavior", () => {
    it("renders filter banner with active filter crumb and available options", () => {
        renderFilterBanner(
            {
                dandisetId: "001697",
                subject: null,
                session: null,
                pipelineVersion: null,
                paramsType: "deterministic",
                configType: "v1",
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
        expect(banner.innerHTML).toContain("Params:&nbsp;deterministic");
        expect(banner.innerHTML).toContain("Config:&nbsp;v1");
        expect(banner.innerHTML).toContain("Codebase:&nbsp;abc1234");
        expect(banner.innerHTML).toContain('option value="v1"');
        expect(banner.innerHTML).toContain('option value="deterministic"');
        expect(banner.innerHTML).toContain('option value="original"');
        expect(banner.innerHTML).toContain('option value="v0"');
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
