const { defineConfig } = require("vitest/config");

module.exports = defineConfig({
    test: {
        environment: "jsdom",
        globals: true,
        include: ["src/**/*.test.js"],
        coverage: {
            provider: "v8",
            reporter: ["text", "lcov", "json-summary"],
            reportsDirectory: "coverage",
        },
    },
});
