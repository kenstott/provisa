// Copyright (c) 2026 Kenneth Stott
// Canary: 4427b2a0-92ff-43ed-bc01-1e09f2c37099
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written

// REQ-1937: the SQL explorer's results grid filters each column by its type. A numeric column
// reads >100 as a comparison, a text column offers a checklist of its values with counts, a chip
// names each filter and removes it, and filters on several columns combine with AND.

import { test, expect } from "./coverage";
import { runSqlOnPage } from "./source-to-query-helpers";

const SQL =
  "SELECT * FROM (VALUES (50, 'ok'), (150, 'error'), (500, 'timeout'), (700, 'ok')) AS t(amount, status)";

test.describe("SQL explorer typed column filters", () => {
  test("filters a numeric column with the quick syntax and a text column from its checklist", async ({
    page,
  }) => {
    await runSqlOnPage(page, SQL);

    await page.getByLabel("Filter rows… amount").fill(">100");
    await expect(page.getByTestId("filter-chip-amount")).toBeVisible();
    const rows = page.locator(".sql-results-table tbody tr");
    await expect(rows).toHaveCount(3);

    await page.getByTestId("col-filter-status-btn").click();
    await page.getByTestId("col-filter-status-op").click();
    await page.getByRole("option", { name: "is one of" }).click();
    await expect(page.getByText("ok (2)")).toBeVisible();
    await page.getByTestId("col-filter-status-value-error").check();
    await page.getByTestId("col-filter-status-value-timeout").check();
    await page.getByTestId("col-filter-status-apply").click();
    await expect(rows).toHaveCount(2);

    await page.getByRole("button", { name: "Remove the filter on status" }).click();
    await expect(rows).toHaveCount(3);
    await page.getByTestId("clear-filters-btn").click();
    await expect(rows).toHaveCount(4);
  });
});
