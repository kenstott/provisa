// Copyright (c) 2026 Kenneth Stott
// Canary: 3a2e90c4-95f2-42f0-980d-367dd777ce49
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-962/1166/1169: an operator can create a snapshot-boundary calendar and bind it (with a grain)
// to a materialized view from the Tables admin surface — the periodic-snapshot config round-trip.
// Create calendar -> pick it -> choose grain + lateness -> save -> re-open and assert it persisted.
// Uses the pre-seeded `dim_pet` view (source_id: __derived__, materialize: true) which has viewSql
// set — source tables without viewSql do not expose the MV snapshot schedule panel.
test("create a calendar and configure an MV snapshot schedule", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, {
    timeout: 15000,
  });

  const openEdit = async () => {
    const row = page.locator("tr").filter({ hasText: "dim_pet" }).first();
    await row.waitFor({ timeout: 10000 });
    await row.click();
    const editBtn = page.getByTestId("table-read-view-edit").first();
    await editBtn.waitFor({ timeout: 5000 });
    await editBtn.click();
    await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });
  };

  await openEdit();

  // dim_pet is already materialized (materialize: true) — verify the MV checkbox is present
  // and checked. Mantine's Checkbox renders <input> and <label> as siblings; getByLabel resolves
  // the `for` association — label-then-descendant-input never matches.
  const matCheckbox = page.getByLabel(/Materialized View/i);
  await expect(matCheckbox).toBeVisible({ timeout: 5000 });
  if (!(await matCheckbox.isChecked())) await matCheckbox.check();

  // Expand the Snapshot Schedule panel only if it is not already open. The panel auto-opens
  // when mvCalendar is non-null (e.g. on a retry where the first attempt already persisted the
  // calendar). Clicking the toggle when already open would close it instead.
  const calendarInput = page.getByTestId("mv-calendar");
  if (!(await calendarInput.isVisible())) {
    await page.getByTestId("mv-snapshot-panel-toggle").click();
    // Wait for the Mantine Collapse animation to complete (overflow:hidden during animation).
    await calendarInput.waitFor({ state: "visible", timeout: 5000 });
  }

  // Create a new snapshot calendar through the modal.
  await page.getByTestId("mv-calendar-new").click();
  await page.getByTestId("calendar-name").fill("e2e-fiscal");
  await expect(page.getByTestId("calendar-version")).toHaveValue("v1");
  await page.getByTestId("calendar-create-submit").click();

  // On success the modal closes and the picker auto-selects the new calendar.
  await expect(page.getByTestId("calendar-name")).toHaveCount(0);
  // mv-calendar is a Mantine Select <input>; toContainText checks text content (empty for inputs),
  // so use toHaveValue which checks the input's value attribute.
  await expect(page.getByTestId("mv-calendar")).toHaveValue(/e2e-fiscal/, { timeout: 10000 });

  // Choose a monthly grain and an allowed-lateness.
  await page.getByTestId("mv-grain").click();
  await page.getByRole("option", { name: /Monthly/i }).click();
  await page.getByTestId("mv-allowed-lateness").fill("3600");

  await page.getByRole("button", { name: /save/i }).first().click();
  await page.getByTestId("table-read-view-edit").first().waitFor({ timeout: 10000 });

  // Re-open: navigate away first so the row starts collapsed; clicking an already-expanded row
  // toggles it closed, hiding the edit button.
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.waitForFunction(() => document.querySelectorAll("tr").length > 2, { timeout: 15000 });

  // Re-open and assert the snapshot schedule round-tripped. The panel auto-opens when
  // mvCalendar is persisted — only click the toggle if the panel is still closed.
  await openEdit();
  const calendarInputRt = page.getByTestId("mv-calendar");
  if (!(await calendarInputRt.isVisible())) {
    await page.getByTestId("mv-snapshot-panel-toggle").click();
    await calendarInputRt.waitFor({ state: "visible", timeout: 5000 });
  }
  await expect(page.getByTestId("mv-calendar")).toHaveValue(/e2e-fiscal/);
  await expect(page.getByTestId("mv-grain")).toHaveValue(/Monthly/i);
  await expect(page.getByTestId("mv-allowed-lateness")).toHaveValue("3600");

  // Revert: clear the schedule so shared config is left as found; keep materialize=true
  // since dim_pet is originally materialized.
  // mv-calendar data-testid is on the inner <input>; the Mantine clearable "×" CloseButton is
  // inside the Input rightSection wrapper. Use `.locator('button')` (not getByRole) because
  // Mantine marks the CloseButton aria-hidden, and force:true bypasses opacity/pointer-events.
  await page.locator('[data-testid="mv-calendar"]').locator("xpath=..").locator("button").first().click({ force: true });
  await page.getByRole("button", { name: /save/i }).first().click();
  // Wait for save chain to complete before the test ends (avoids uncaught-browser-error on teardown).
  await page.getByTestId("table-read-view-edit").first().waitFor({ timeout: 10000 });
});
