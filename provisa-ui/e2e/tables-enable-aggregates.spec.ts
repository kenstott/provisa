// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

test("pets table edit shows enableAggregates and enableGroupBy checked", async ({ page }) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  // Wait for tables to render
  await page.waitForFunction(
    () => document.querySelectorAll("tr").length > 2,
    { timeout: 15000 },
  );

  // Click the pets row to expand it (first row with pet-store-pg source)
  const petsRow = page.locator("tr").filter({ hasText: "pet-store-pg" }).filter({ hasText: "pets" }).first();
  await petsRow.waitFor({ timeout: 10000 });
  await petsRow.click();

  // Click the Edit button that appears after expansion
  const editBtn = page.getByTitle("Edit").first();
  await editBtn.waitFor({ timeout: 5000 });
  await editBtn.click();

  // Wait for edit form with checkboxes
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  // Enable Aggregates checkbox must be checked
  const enableAggregatesLabel = page.locator("label").filter({ hasText: /Enable Aggregates/i });
  const enableAggregatesCheckbox = enableAggregatesLabel.locator("input[type='checkbox']");
  await expect(enableAggregatesCheckbox).toBeChecked();

  // Enable Group By checkbox must be checked
  const enableGroupByLabel = page.locator("label").filter({ hasText: /Enable Group By/i });
  const enableGroupByCheckbox = enableGroupByLabel.locator("input[type='checkbox']");
  await expect(enableGroupByCheckbox).toBeChecked();
});
