// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-962/1166/1169: an operator can create a snapshot-boundary calendar and bind it (with a grain)
// to a materialized view from the Tables admin surface — the periodic-snapshot config round-trip.
// Create calendar -> pick it -> choose grain + lateness -> save -> re-open and assert it persisted.
test("create a calendar and configure an MV snapshot schedule", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, {
    timeout: 15000,
  });

  const openEdit = async () => {
    const row = page.locator("tr").filter({ hasText: "pet-store-pg" }).first();
    await row.waitFor({ timeout: 10000 });
    await row.click();
    const editBtn = page.getByTitle("Edit").first();
    await editBtn.waitFor({ timeout: 5000 });
    await editBtn.click();
    await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });
  };

  await openEdit();

  // Mark it a Materialized View — reveals the MV config panels.
  const matCheckbox = page
    .locator("label")
    .filter({ hasText: /Materialized View/i })
    .locator("input[type='checkbox']");
  if (!(await matCheckbox.isChecked())) await matCheckbox.check();

  // Expand the (collapsed-by-default) Snapshot Schedule panel.
  await page.getByTestId("mv-snapshot-panel-toggle").click();

  // Create a new snapshot calendar through the modal.
  await page.getByTestId("mv-calendar-new").click();
  await page.getByTestId("calendar-name").fill("e2e-fiscal");
  await expect(page.getByTestId("calendar-version")).toHaveValue("v1");
  await page.getByTestId("calendar-create-submit").click();

  // On success the modal closes and the picker auto-selects the new calendar.
  await expect(page.getByTestId("calendar-name")).toHaveCount(0);
  await expect(page.getByTestId("mv-calendar")).toContainText("e2e-fiscal", { timeout: 10000 });

  // Choose a monthly grain and an allowed-lateness.
  await page.getByTestId("mv-grain").click();
  await page.getByRole("option", { name: /Monthly/i }).click();
  await page.getByTestId("mv-allowed-lateness").fill("3600");

  await page.getByRole("button", { name: /save/i }).first().click();
  await page.waitForTimeout(500);

  // Re-open and assert the snapshot schedule round-tripped.
  await openEdit();
  await page.getByTestId("mv-snapshot-panel-toggle").click();
  await expect(page.getByTestId("mv-calendar")).toContainText("e2e-fiscal");
  await expect(page.getByTestId("mv-grain")).toContainText("Monthly");
  await expect(page.getByTestId("mv-allowed-lateness")).toHaveValue("3600");

  // Revert: clear the schedule and unmaterialize so shared config is left as found.
  await page.getByTestId("mv-calendar").getByRole("button").first().click(); // clearable "×"
  const matAgain = page
    .locator("label")
    .filter({ hasText: /Materialized View/i })
    .locator("input[type='checkbox']");
  if (await matAgain.isChecked()) await matAgain.uncheck();
  await page.getByRole("button", { name: /save/i }).first().click();
});
