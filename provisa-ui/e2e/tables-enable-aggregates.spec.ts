// Copyright (c) 2026 Kenneth Stott
// Canary: 729412ed-6bfe-4666-932c-857e547db62e
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// Demo mode auto-starts the guided tour in a fresh browser profile (App.tsx), which
// navigates away from /tables — mark it seen before the app boots.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
});

test("pets table edit shows enableAggregates and enableGroupBy checked", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  // Wait for tables to render
  await page.waitForFunction(
    () => document.querySelectorAll("tr").length > 2,
    { timeout: 15000 },
  );

  // Click the pets row to expand it (first row with pet-store-sqlite source)
  const petsRow = page.locator("tr").filter({ hasText: "pet-store-sqlite" }).filter({ hasText: "pets" }).first();
  await petsRow.waitFor({ timeout: 10000 });
  await petsRow.click();

  // Click the Edit button that appears after expansion
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 5000 });
  await editBtn.click();

  // Wait for the edit form's own checkbox. A bare input[type='checkbox'] wait resolves to the
  // first of ~10 on the page — the table's row-selection controls, which sit inside a collapsed
  // container and are therefore never "visible" — so it timed out while the form was already open.
  await page.getByLabel(/Enable Aggregates/i).waitFor({ state: "attached", timeout: 15000 });

  // Enable Aggregates checkbox must be checked
  await expect(page.getByLabel(/Enable Aggregates/i)).toBeChecked();

  // Enable Group By checkbox must be checked
  await expect(page.getByLabel(/Enable Group By/i)).toBeChecked();

  // REQ-1360: metadata-only implicit measure/dimension badges. `price` (double) is numeric
  // so it qualifies as an implicit measure; with enableAggregates/enableGroupBy both on,
  // every typed column also carries the implicit dimension badge.
  const priceRow = page.getByTestId("column-row-price");
  await expect(priceRow.getByText("Measure", { exact: true })).toBeVisible();
  await expect(priceRow.getByText("Dim", { exact: true })).toBeVisible();
});
