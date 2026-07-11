// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-963/966: an operator can define a materialized view and its NRT debounce
// (quiet / max_delay) from the Tables admin surface. Marking a table Materialized
// reveals the debounce inputs; the values persist across a save round-trip.
test("define a materialized view with NRT debounce config", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, {
    timeout: 15000,
  });

  // Expand the first data table row and open its edit form.
  const row = page.locator("tr").filter({ hasText: "pet-store-pg" }).first();
  await row.waitFor({ timeout: 10000 });
  await row.click();
  const editBtn = page.getByTitle("Edit").first();
  await editBtn.waitFor({ timeout: 5000 });
  await editBtn.click();
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  // Mark it a Materialized View — this reveals the refresh + debounce controls.
  const matLabel = page.locator("label").filter({ hasText: /Materialized View/i });
  const matCheckbox = matLabel.locator("input[type='checkbox']");
  if (!(await matCheckbox.isChecked())) {
    await matCheckbox.check();
  }

  // The NRT debounce inputs appear (REQ-963); set quiet=2s, max_delay=10s.
  const quiet = page.getByTestId("mv-debounce-quiet");
  const maxDelay = page.getByTestId("mv-debounce-max-delay");
  await expect(quiet).toBeVisible();
  await expect(maxDelay).toBeVisible();
  await quiet.fill("2");
  await maxDelay.fill("10");

  // Save the table config.
  const saveBtn = page.getByRole("button", { name: /save/i }).first();
  await saveBtn.click();

  // Re-open the edit form and assert the debounce values round-tripped (persisted).
  await page.waitForTimeout(500);
  const rowAgain = page.locator("tr").filter({ hasText: "pet-store-pg" }).first();
  await rowAgain.click();
  const editAgain = page.getByTitle("Edit").first();
  await editAgain.waitFor({ timeout: 5000 });
  await editAgain.click();
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  await expect(page.getByTestId("mv-debounce-quiet")).toHaveValue("2");
  await expect(page.getByTestId("mv-debounce-max-delay")).toHaveValue("10");

  // Revert materialization so the run leaves shared config as it was found.
  const matLabel2 = page.locator("label").filter({ hasText: /Materialized View/i });
  const matCheckbox2 = matLabel2.locator("input[type='checkbox']");
  if (await matCheckbox2.isChecked()) {
    await matCheckbox2.uncheck();
    await page.getByRole("button", { name: /save/i }).first().click();
  }
});
