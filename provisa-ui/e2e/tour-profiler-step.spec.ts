// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e1d64-7a2f-4c85-b0e3-5f8d2a6c1e97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

test.describe.configure({ timeout: 180_000 });

// REQ-1934 / REQ-1443: the quality steps open the dq-checker's results table on
// /tables?source=dq-checker; the profiler steps then move, on the same mounted page, to
// /tables?source=pet-store-sqlite and open a plain table whose editor carries the Data Profiler
// panel. Two defects stranded the tour on a spinner here:
//  - the filter box kept "dq-checker" across that in-place route change, so "Profile a table"
//    expanded the checker's table again, whose editor has no profiler panel;
//  - resuming from another page, the first row on screen was a row of the page being left
//    (/sources), so the tour clicked that one instead of a table row.
test("the Data quality topic walks from the checker through to the profiler panel", async ({
  page,
}) => {
  // REQ-1945: these steps are the "Data quality" topic. The core tour has been seen ("seen"), so the
  // navbar tour button opens the Deep Dives menu and the card runs the topic.
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();
  await page.locator('[data-testid="tour-topic-quality"]').click();

  const title = page.locator(".driver-popover-title");
  const next = page.locator(".driver-popover-next-btn");

  // Starting the topic navigates to /tables and pays that route's first-visit chunk fetch and data load.
  // The table the step opens is named in the URL after the source (REQ-2241: an open panel is its
  // URL), so the source is matched up to the next parameter.
  await expect(title).toHaveText("A quality checker is just a source", { timeout: 60000 });
  await expect(page).toHaveURL(/\/tables\?source=dq-checker(&|$)/);
  // The opened table is the selected row, and its detail is in the panel on the right.
  await expect(page.locator('[data-table-row="dq-checker.pets_quality_dq_checker_dq"].row-selected')).toBeVisible();
  await expect(page.getByTestId("tables-split-panel")).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Build a quality contract", { timeout: 30000 });
  await expect(page.locator('[data-tour="dq-panel"]')).toBeVisible();

  await next.click();
  await expect(title).toHaveText("Profile a table", { timeout: 30000 });
  await expect(page).toHaveURL(/\/tables\?source=pet-store-sqlite(&|$)/);
  // The filter follows the new ?source=, and the row opened is the pet-store table with its editor.
  await expect(page.locator('[data-table-row^="dq-checker."]')).toHaveCount(0);
  // The opened table is the selected row, and its detail is in the panel on the right.
  await expect(page.locator('[data-table-row="pet-store-sqlite.pets"].row-selected')).toBeVisible();
  await expect(page.getByTestId("tables-split-panel")).toBeVisible();
  // The card opens the editor on its Quality tab, where the profiler is, open.
  await expect(page.getByTestId("table-section-quality")).toHaveAttribute("aria-selected", "true");
  await expect(page.locator('[data-tour="profiler-panel"]')).toBeVisible();
  await expect(page.getByTestId("profiler-panel-toggle")).toHaveCount(0);

  await next.click();
  await expect(title).toHaveText("Checks on profile results", { timeout: 30000 });
  await expect(page.locator('[data-tour="profiler-panel"]')).toBeVisible();
});

test("the Test data topic walks from the map through the profiler to a fake", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();
  await page.locator('[data-testid="tour-topic-testdata"]').click();

  const title = page.locator(".driver-popover-title");
  const next = page.locator(".driver-popover-next-btn");

  await expect(title).toHaveText("How the parts fit", { timeout: 60000 });
  await expect(page).toHaveURL(/\/tables\?source=pet-store-sqlite(&|$)/);
  await expect(page.locator('[data-testid="table-read-view-edit"]')).toBeVisible();

  await next.click();
  await expect(title).toHaveText("What fakes and synthetic data are built from", {
    timeout: 30000,
  });
  await expect(page.locator('[data-tour="profiler-panel"]')).toBeVisible();

  // "Declare a fake" opens the Testing tab of the table's Columns.
  await next.click();
  await expect(title).toHaveText("Declare a fake", { timeout: 30000 });
  await expect(page.locator('[data-testid="testdata-columns"]')).toBeVisible();
  await expect(page.locator('[data-testid="column-tab-testing"]')).toHaveAttribute(
    "aria-selected",
    "true",
  );
});
