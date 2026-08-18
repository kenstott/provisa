// Copyright (c) 2026 Kenneth Stott
// Canary: c0c6b0cc-f9ba-4c43-a361-b831d3088363
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test as base, expect, type Page } from "playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { TOUR_SEEN_KEY, TOUR_DEMO_RESET_KEY } from "../src/tour/tourKeys";

export { expect } from "playwright/test";

// Single source of truth for the isolated e2e backend's URL — matches
// playwright.config.ts's PROVISA_E2E_API_PORT. Specs must reference this instead
// of hardcoding a port, so the harness stays collision-proof with the dev backend.
//
// Each worker gets its OWN backend process (playwright.config.ts starts one per worker and
// publishes the port list here). They cannot share one: `state.schema_version` is a
// process-global counter, and src/apolloClient.ts resets the entire Apollo store whenever the
// X-Schema-Version header changes — so one worker registering a table blanked the lists every
// other worker was asserting on.
//
// TEST_PARALLEL_INDEX, not TEST_WORKER_INDEX: both are set in worker processes, but only the
// parallel index is bounded by the worker count and stable across retries (a retry spawns a
// replacement worker with a fresh workerIndex and the same parallelIndex). Indexing the port
// list by workerIndex would run off the end on the first retry.
// REQ-1472: the same specs run against two targets. `local` is the self-provisioned harness
// described above — one backend process per worker on its own port. `cloud` is a deployment that
// already exists and that this run does not own: one origin serves both the SPA and the API, there
// are no per-worker backends to index, and nothing here may start or seed a server.
export const TARGET = process.env.PROVISA_E2E_TARGET ?? "local";
if (!["local", "cloud"].includes(TARGET)) {
  throw new Error(`PROVISA_E2E_TARGET must be local|cloud, got: ${TARGET}`);
}
const CLOUD_URL = process.env.PROVISA_E2E_CLOUD_URL ?? "";
if (TARGET === "cloud" && !CLOUD_URL) {
  throw new Error("PROVISA_E2E_TARGET=cloud requires PROVISA_E2E_CLOUD_URL");
}

const BACKEND_PORTS = (process.env.PROVISA_E2E_BACKEND_PORTS ?? "8901").split(",");
const PARALLEL_INDEX = Number(process.env.TEST_PARALLEL_INDEX ?? "0");
if (TARGET === "local" && !BACKEND_PORTS[PARALLEL_INDEX]) {
  throw new Error(
    `No backend for parallel index ${PARALLEL_INDEX} in PROVISA_E2E_BACKEND_PORTS=` +
      `${BACKEND_PORTS.join(",")} — playwright.config.ts must start one backend per worker.`,
  );
}
export const BACKEND_PORT = BACKEND_PORTS[PARALLEL_INDEX];
export const BACKEND_URL = TARGET === "cloud" ? CLOUD_URL : `http://localhost:${BACKEND_PORT}`;

// UI dev server port — matches playwright.config.ts's E2E_UI_PORT.
// The Apollo client uses relative URLs (/admin/graphql) so the browser sends
// API calls to this origin; Vite then proxies them server-side to the backend.
// Tests that redirect API traffic (e.g. sharepoint/splunk) must intercept at
// this origin, not at the backend port.
export const UI_PORT = process.env.PROVISA_E2E_UI_PORT ?? "3901";
export const UI_URL = TARGET === "cloud" ? CLOUD_URL : `http://localhost:${UI_PORT}`;

// Trino-backed backend URL for sharepoint/splunk specs.  These tests redirect the UI's
// API calls to this backend (via page.route()) so that register_source() creates a real
// Trino catalog and the schema dropdown populates.
export const TRINO_BACKEND_PORT = process.env.PROVISA_E2E_TRINO_API_PORT ?? "8990";
export const TRINO_BACKEND_URL = `http://localhost:${TRINO_BACKEND_PORT}`;

// Same rationale as BACKEND_PORT — matches playwright.config.ts's PROVISA_E2E_FLIGHT_PORT.
export const FLIGHT_PORT = process.env.PROVISA_E2E_FLIGHT_PORT ?? "8903";
export const FLIGHT_URL = `http://localhost:${FLIGHT_PORT}`;

// WCAG 2.1 Level AA gate (REQ-1013, REQ-1014). `expectNoA11yViolations` runs
// axe-core against the current page state, scoped to the tags that constitute
// AA conformance, and fails the test with a readable rule/target breakdown.
export async function expectNoA11yViolations(page: Page, context?: string) {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  const summary = results.violations
    .map(
      (v) =>
        `${v.id} (${v.impact}): ${v.help} [${v.nodes.map((n) => n.target.join(" ")).join(", ")}]`,
    )
    .join("\n");
  expect(
    results.violations,
    `${context ? context + " — " : ""}axe found ${results.violations.length} accessibility violation(s):\n${summary}`,
  ).toEqual([]);
}

export const test = base.extend<{
  expectNoA11yViolations: typeof expectNoA11yViolations;
  /**
   * Console-error substrings this spec EXPECTS the browser to log.
   *
   * The default is none, and that is the point: an uncaught error is a failure even when the
   * assertions pass. The exception is a spec whose subject IS a refused response — an expired
   * credential, an unreachable server — where the browser logs the failed fetch no matter how
   * correctly the app handles it. Such a spec declares exactly what it expects:
   *
   *     test.use({ allowedBrowserErrors: ["status of 401"] });
   *
   * Anything not matching a declared substring still fails, so the guard keeps its teeth.
   */
  allowedBrowserErrors: string[];
}>({
  allowedBrowserErrors: [[], { option: true }],
  // Names this worker's backend on every request the browser makes to the Vite dev server,
  // which routes the proxied API paths accordingly (vite.config.ts). One Vite server serves all
  // workers — the alternative, one dev server per worker, costs a 4 GB Node heap each.
  // Overriding the built-in option this way rather than calling test.use(): test.use() is
  // illegal outside a spec/describe body, and this module is imported by every spec.
  extraHTTPHeaders: [{ "x-e2e-worker": String(PARALLEL_INDEX) }, { option: true }],
  page: async ({ page, allowedBrowserErrors }, use) => {
    // A fresh profile is offered the guided tour, and that offer is a modal whose overlay swallows
    // every click a spec tries to make. No spec here is testing the offer, so each page starts
    // already-offered: the seen flag suppresses the modal, and the demo-reset marker spends the
    // once-per-session reset App.tsx performs in demo mode (resetTourStateForDemoSession), which
    // would otherwise erase the seen flag before the offer is decided. The marker lives in
    // sessionStorage, which Playwright's storageState cannot carry, so both are written here,
    // before any page script runs. tour-page-preload.spec.ts opts back in with ?tour=1, which
    // starts the tour regardless of the flag.
    await page.addInitScript(
      ([seenKey, resetKey]) => {
        sessionStorage.setItem(resetKey, "true");
        localStorage.setItem(seenKey, "true");
      },
      [TOUR_SEEN_KEY, TOUR_DEMO_RESET_KEY],
    );
    const errors: string[] = [];
    page.on("pageerror", (err) => errors.push(err.message));
    page.on("console", (msg) => {
      if (msg.type() === "error") errors.push(msg.text());
    });
    // eslint-disable-next-line react-hooks/rules-of-hooks -- `use` here is the Playwright fixture callback, not a React hook
    await use(page);
    const unexpected = errors.filter(
      (e) => !allowedBrowserErrors.some((allowed) => e.includes(allowed)),
    );
    expect(unexpected, `Uncaught browser errors: ${unexpected.join("; ")}`).toHaveLength(0);
  },
  // Exposed as a fixture so specs can call `await expectNoA11yViolations(page)`
  // without importing it separately; the standalone export remains available.
  // Playwright parses the first parameter to resolve fixture dependencies and rejects anything
  // that is not an object destructuring pattern, so a fixture with no dependencies spells it `{}`.
  // eslint-disable-next-line no-empty-pattern -- required by Playwright's fixture signature parser
  expectNoA11yViolations: async ({}, use) => {
    // eslint-disable-next-line react-hooks/rules-of-hooks -- Playwright fixture callback, not a React hook
    await use(expectNoA11yViolations);
  },
});
