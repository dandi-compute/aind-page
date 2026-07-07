---
name: run-dashboard
description: Launch and drive this static dashboard in a real browser with stubbed external data. Use when running or screenshotting the app locally, or when verifying that a change to src/app.js works in the running app (not just in vitest).
---

# Running and driving the aind-page dashboard in a browser

The app is a static page (`src/index.html` + `src/app.js`, no build step). All data
comes from external hosts fetched client-side, so running it locally = serve `src/`,
open it in Chromium, and stub the external endpoints at the network boundary.

## Recipe

1. Serve the page: any static server over `src/` (e.g. `python3 -m http.server 8123 -d src`,
   or a small Node `http` server). `localhost` is a secure context, so the Cache API works.
2. Drive with Playwright against the preinstalled browser (do NOT `playwright install`):
   - `npm i playwright-core` in a scratch dir.
   - `chromium.launchPersistentContext(profileDir, { headless: true, executablePath: "/opt/pw-browsers/chromium_headless_shell-1194/chrome-linux/headless_shell" })`
     (adjust the revision dir to what's in `/opt/pw-browsers/`). A persistent
     profile lets you simulate a browser restart (close + relaunch, same dir)
     to test cross-session persistence (Cache API, localStorage).
3. Stub external hosts with `page.route(/raw\.githubusercontent\.com|dandiarchive\.s3\.amazonaws\.com|api\.github\.com/, handler)`
   and `route.fulfill(...)`. Endpoints the queue dashboard hits:
   - `.../queue/compressed/state.jsonl.gz` — gzip JSONL (`zlib.gzipSync` of newline-joined entries).
   - `.../queue/main/archive_state.jsonl` — plain JSONL (archive view).
   - `.../queue/main/queue_config.json` — priorities banner.
   - `.../code/main/...registered_params.json` / `registered_configs.json` — registries.
   - `https://dandiarchive.s3.amazonaws.com/blobs/<3>/<3>/<id>` — per-run artifacts
     (trace.txt, dataset_description.json, quality_control.json, visualization_output.json).
4. Load `http://localhost:8123/?view=dashboard` and wait for `#summary .summary-stats`;
   run cards are `.run-entry`. Count/inspect stubbed requests in the route handler
   to assert network behavior (e.g. blob requests are cache-hits on reload).

## Fixture gotchas (cost real debugging time)

- The queue view is `?view=dashboard` — `view=main` silently falls back to the landing page.
- A JSONL entry's blob lookups go through `run.path` built by `buildRunPath(entry)`
  from `dandiset_id`/`subject`/`pipeline`/`version`/`params`/`config`/`attempt`.
  The keys in `output_paths` MUST match that computed path exactly
  (`derivatives/dandiset-<id>/sub-<subject>/pipeline-<pipeline>/version-<version>_params-<params>_config-<config>_attempt-<n>/...`)
  or every artifact resolves to null and nothing is fetched.
- `dataset_description.json` is only fetched when the entry has a
  `dataset_description_path` field (`{ "<repo-path>": "<blob-id>" }` map), not
  merely a matching `output_paths` key.
- Fulfilled ETags on cross-origin fetches aren't exposed to page JS without
  `Access-Control-Expose-Headers: ETag`, so If-None-Match assertions against
  stubs need that header (the real GitHub CDN sends it).
- A single-dandiset fixture auto-expands its group (`autoExpand`), so "clicking to
  open" it actually closes it — check `details.open` before assuming click direction.
- Run cards live inside collapsed `<details>` groups (tree layout), so Playwright's
  default "visible" waits time out on them — use `waitForSelector(..., { state: "attached" })`.
- The Google Fonts request fails offline — harmless, ignore it.

A complete working example of this recipe (server + stubs + phased
cold/reload/restart/refresh assertions) was used for the blob-cache overhaul;
its shape is worth copying for future network-behavior checks.
