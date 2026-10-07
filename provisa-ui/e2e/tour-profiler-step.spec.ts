// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e1d64-7a2f-4c85-b0e3-5f8d2a6c1e97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import { TOUR_STEPS } from "../src/tour/tourSteps";

// Derived so inserting a step re-points the test instead of walking it onto the wrong surface.
const QUALITY_TABLE_STEP = TOUR_STEPS.findIndex((s) => s.key === "stepQualityTable");

test.describe.configure({ timeout: 180_000 });

// REQ-1934 / REQ-1443: the quality steps open the dq-checker's results table on
// /tables?source=dq-checker; the profiler steps then move, on the same mounted page, to
// /tables?source=pet-store-sqlite and open a plain table whose editor carries the Data Profiler
// panel. Two defects stranded the tour on a spinner here:
//  - the filter box kept "dq-checker" across that in-place route change, so "Profile a table"
//    expanded the checker's table again, whose editor has no profiler panel;
//  - resuming from another page, the first row on screen was a row of the page being left
//    (/sources), so the tour clicked that one instead of a table row.
test("tour walks from the quality steps through to the profiler panel", async ({ page }) => {
  // Resume at the first quality step from /sources (see tour-page-preload.spec.ts): "seen" keeps
  // TourAutoStart from restarting at step 0; the navbar tour button resumes at the saved index.
  await page.addInitScript((step: number) => {
    localStorage.setItem("provisa_tour_seen", "true");
    localStorage.setItem("provisa_tour_progress", String(step));
  }, QUALITY_TABLE_STEP);
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();

  const title = page.locator(".driver-popover-title");
  const next = page.locator(".driver-popover-next-btn");
  const editButton = page.locator('[data-testid="table-read-view-edit"]');

  // Resuming navigates to /tables and pays that route's first-visit chunk fetch and data load.
  await expect(title).toHaveText("A quality checker is just a source", { timeout: 60000 });
  await expect(page).toHaveURL(/\/tables\?source=dq-checker$/);
  await expect(
    page.locator('[data-table-row="dq-checker.pets_scan"] + tr.list-expand'),
  ).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Build a quality contract", { timeout: 30000 });
  await expect(page.locator('[data-tour="dq-panel"]')).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Profile a table", { timeout: 30000 });
  await expect(page).toHaveURL(/\/tables\?source=pet-store-sqlite$/);
  // The filter follows the new ?source=, and the row opened is the pet-store table.
  await expect(page.locator('[data-table-row^="dq-checker."]')).toHaveCount(0);
  await expect(
    page.locator('[data-table-row="pet-store-sqlite.pets"] + tr.list-expand'),
  ).toBeVisible();
  await expect(editButton).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Add it to a profiler", { timeout: 30000 });
  await expect(page.locator('[data-tour="profiler-panel"]')).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Drift and checks", { timeout: 30000 });

  // "Declare a fake" switches the column list to its Test data mode.
  await next.click();
  await expect(title).toHaveText("Declare a fake", { timeout: 30000 });
  await expect(page.locator('[data-testid="testdata-columns"]')).toBeVisible();
  await expect(
    page.locator('[data-tour="table-columns-mode"] input[value="testdata"]'),
  ).toBeChecked();
});
