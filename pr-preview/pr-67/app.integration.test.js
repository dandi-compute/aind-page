const { renderFilterBanner, showError, showLoading, showResults } = require("./app");

beforeEach(() => {
    document.body.innerHTML = `
        <div id="loading"></div>
        <div id="error"></div>
        <div id="filter-banner"></div>
        <div id="summary"></div>
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
                failureStep: "pre-processing",
            },
            [
                {
                    dandisetId: "001697",
                    subject: "a",
                    session: "s1",
                    pipelineVersion: "v1",
                    status: "failed",
                    failureStep: "pre-processing",
                },
                {
                    dandisetId: "001697",
                    subject: "b",
                    session: "s2",
                    pipelineVersion: "v2",
                    status: "success",
                    failureStep: null,
                },
            ]
        );

        const banner = document.getElementById("filter-banner");
        expect(banner.style.display).toBe("");
        expect(banner.innerHTML).toContain("Filtered view:");
        expect(banner.innerHTML).toContain("Failed in pre-processing");
        expect(banner.innerHTML).toContain('option value="v1"');
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
});
