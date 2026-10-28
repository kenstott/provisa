// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a5e91-8d3c-4b7a-9e1f-2c6d4a8b5f70
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1386: admin Reports viewer — every seeded ops-domain management report view
// must load through the governed SQL pipeline without error. Regression coverage
// for the class of bug where a report view's generated/expanded SQL no longer
// matches the physical schema (e.g. a sample-wrapper or column mismatch), which
// previously only surfaced by manually running each report in the UI.

import { test, expect, type Page } from "./coverage";

// Kept in sync with provisa/api/_meta_views.py::_OPS_REPORT_VIEWS.
const STANDARD_REPORTS = [
  "usage_ranking",
  "deprecated_usage",
  "pii_access",
  "policy_denials",
  "surface_mix",
  "query_health",
  "stale_metadata",
  "join_hotspots",
];

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
});

/**
 * Open the Reports tab and wait out the app's start: the pre-React splash (index.html) stands until
 * the app has mounted, and the code-split admin page's fallback (page-loading) until its modules
 * arrive -- both served cold by the lane's dev server, which in run 40692080291 mounted the app 25 s
 * after the goto began and asked for the admin page 14 s after that. With both gone, the checks
 * that follow measure the reports list, not the dev server.
 */
async function openReports(page: Page) {
  await page.goto("/admin/reports");
  await expect(page.locator("#provisa-splash")).toHaveCount(0, { timeout: 60000 });
  await expect(page.getByTestId("page-loading")).toHaveCount(0, { timeout: 60000 });
}

for (const reportName of STANDARD_REPORTS) {
  test(`REQ-1386: standard report "${reportName}" loads without error`, async ({ page }) => {
    await openReports(page);
    const item = page.getByTestId(`report-item-${reportName}`);
    await expect(item).toBeVisible({ timeout: 15000 });
    await item.click();

    // Either the results grid renders (rows, possibly zero) or the empty-state
    // text shows — never a query error.
    await expect(page.getByTestId("table-preview-error")).toHaveCount(0, { timeout: 15000 });
    const grid = page.getByTestId("download-csv-btn");
    const noRowsText = page.getByText("No rows", { exact: false });
    // A zero-row report renders BOTH: the results panel keeps its download button and shows
    // "No rows." beside it, so the union locator matches two elements and strict mode rejects it.
    // Either one being visible is the pass condition.
    await expect(grid.or(noRowsText).first()).toBeVisible({ timeout: 15000 });
  });
}

test("REQ-1386: reports list is seeded with every standard report on install", async ({ page }) => {
  await openReports(page);
  for (const reportName of STANDARD_REPORTS) {
    await expect(page.getByTestId(`report-item-${reportName}`)).toBeVisible({ timeout: 15000 });
  }
});
