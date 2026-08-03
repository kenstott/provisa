// Copyright (c) 2026 Kenneth Stott
// Canary: d0aa31c0-43a6-4273-91eb-196a29af3439
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-963/966: an operator can define a materialized view and its NRT debounce
// (quiet / max_delay) from the Tables admin surface. The debounce inputs are visible
// when a view table is materialized; the values persist across a save round-trip.
// Uses the pre-seeded `dim_pet` view (source_id: __derived__, materialize: true)
// which has viewSql set — source tables without viewSql do not expose the MV toggle.
test("define a materialized view with NRT debounce config", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, {
    timeout: 15000,
  });

  // Expand the dim_pet view row and open its edit form.
  const row = page.locator("tr").filter({ hasText: "dim_pet" }).first();
  await row.waitFor({ timeout: 10000 });
  await row.click();
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 5000 });
  await editBtn.click();
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  // dim_pet starts with materialize: true. The Materialized View checkbox (only visible on
  // view tables with viewSql) and the NRT debounce inputs should already be rendered (REQ-963).
  // Mantine's Checkbox renders <input> and <label> as siblings (label uses a `for` attribute
  // rather than wrapping the input), so getByLabel is required here; label-then-descendant-input
  // never matches.
  const matCheckbox = page.getByLabel(/Materialized View/i);
  await expect(matCheckbox).toBeVisible({ timeout: 5000 });
  const quiet = page.getByTestId("mv-debounce-quiet");
  const maxDelay = page.getByTestId("mv-debounce-max-delay");
  await expect(quiet).toBeVisible();
  await expect(maxDelay).toBeVisible();

  // Set quiet=2s, max_delay=10s (REQ-963).
  await quiet.fill("2");
  await maxDelay.fill("10");

  // Save the table config; wait for the read view's edit button to reappear — it only renders
  // once handleSaveEdit completes the full sequential mutation chain (cascade comment, REQ-966).
  const saveBtn = page.getByRole("button", { name: /save/i }).first();
  await saveBtn.click();
  await editBtn.waitFor({ timeout: 10000 });

  // Re-open the edit form and assert the debounce values round-tripped (persisted).
  // Navigate to /tables first so the row starts collapsed; clicking an already-expanded row
  // would toggle it closed, hiding the edit button.
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, { timeout: 15000 });
  const rowAgain = page.locator("tr").filter({ hasText: "dim_pet" }).first();
  await rowAgain.waitFor({ timeout: 10000 });
  await rowAgain.click();
  const editAgain = page.getByTestId("table-read-view-edit").first();
  await editAgain.waitFor({ timeout: 5000 });
  await editAgain.click();
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  await expect(page.getByTestId("mv-debounce-quiet")).toHaveValue("2");
  await expect(page.getByTestId("mv-debounce-max-delay")).toHaveValue("10");

  // Revert debounce to zero so the run leaves shared config as found.
  // Keep materialize=true since dim_pet is originally materialized.
  await page.getByTestId("mv-debounce-quiet").fill("0");
  await page.getByTestId("mv-debounce-max-delay").fill("0");
  await page.getByRole("button", { name: /save/i }).first().click();
  // Wait for save chain to complete before the test ends (avoids uncaught-browser-error on teardown).
  await page.getByTestId("table-read-view-edit").first().waitFor({ timeout: 10000 });
});
