// Copyright (c) 2026 Kenneth Stott
// Canary: 9f2a8b0c-6f6d-4e60-a12e-5e9d0c6a2f31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

test("openapi-backed table preview renders real rows (get_inventory materialization)", async ({
  page,
}) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  const row = page.locator("tr.clickable", { hasText: "inventory" }).first();
  await row.scrollIntoViewIfNeeded();
  await row.click();

  const previewBtn = page.getByTestId("table-read-view-preview");
  await previewBtn.waitFor({ timeout: 10000 });
  await previewBtn.click();

  const modal = page.getByTestId("table-preview-modal");
  await modal.waitFor({ timeout: 10000 });

  await expect(modal.getByTestId("table-preview-error")).toHaveCount(0);

  const grid = modal.locator("table.sql-results-table");
  await grid.locator("tbody tr").first().waitFor({ timeout: 10000 });

  await expect(grid).toContainText("delivered");
  await expect(grid).toContainText("approved");
  await expect(grid).toContainText("placed");
});
