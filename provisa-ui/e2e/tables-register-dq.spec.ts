// Copyright (c) 2026 Kenneth Stott
// Canary: 4d9b1f27-8e35-4c6a-b0d2-5f7a3e9c1b84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1663: registering a results table on each data-quality checker source through the UI.
//
// The harness boots from config/provisa-install.yaml, which registers both checkers (dq-checker is
// Great Expectations, dq-soda is Soda) connecting back through Provisa's own pgwire. Neither checker
// library has to be installed to REGISTER: the contract is YAML, the dataset resolves against the
// governed tables, and the results envelope ships with Provisa. What this proves end to end is the
// form's claim — pick the table to scan, add a rule, submit — and that the registered row comes back
// carrying the checker it runs under.

import { test, expect, type Page } from "./coverage";

async function openAddForm(page: Page) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.getByRole("button", { name: /\+ Table/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
}

/** Mantine Select: open it, then pick the option by its rendered label. */
async function pickOption(page: Page, testId: string, label: string) {
  await page.getByTestId(testId).click();
  await page.getByRole("option", { name: label, exact: true }).click();
}

const CHECKERS = [
  {
    sourceId: "dq-checker",
    checker: "Great Expectations",
    scanned: "pet_store.inquiries",
    column: /^message/,
    check: "expect_column_values_to_not_be_null",
  },
  {
    sourceId: "dq-soda",
    checker: "Soda",
    scanned: "pet_store.pet_visits",
    column: /^visit_date/,
    check: "missing",
  },
] as const;

for (const c of CHECKERS) {
  test(`registers a ${c.checker} results table by picking the table to scan and adding a rule`, async ({
    page,
  }) => {
    const resultsTable = `${c.sourceId.replace("-", "_")}_e2e_${Date.now()}`;
    await openAddForm(page);

    await page.getByTestId("register-table-source-select").selectOption(c.sourceId);
    await page.getByTestId("register-table-domain-select").selectOption("pet-store");

    // A checker source is not introspected: no schema/table pickers, the contract panel instead.
    await expect(page.getByTestId("register-table-dq")).toBeVisible();
    await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
    await expect(page.getByTestId("register-table-table-select")).toHaveCount(0);
    await expect(page.getByTestId("dq-run-now")).toHaveCount(0);

    // Pick the governed table to scan; the results table's name is derived from it.
    await pickOption(page, "dq-dataset-table-select", c.scanned);
    const [, scannedTable] = c.scanned.split(".");
    await expect(page.getByTestId("register-table-dq-results-table")).toHaveValue(
      `${scannedTable}_scan`,
    );
    await expect(page.getByPlaceholder("Semantic name override")).toHaveValue(
      `${scannedTable}_quality`,
    );
    // The name stays editable — keep the row unique across retries and workers.
    await page.getByTestId("register-table-dq-results-table").fill(resultsTable);

    // Add one rule: the picker offers the scanned table's own columns.
    await page.getByTestId("dq-column").click();
    await page.getByRole("option", { name: c.column }).first().click();
    await pickOption(page, "dq-check-type", c.check);
    // Soda checks carry a threshold (missing must_be 0); GX's not-null takes only `mostly`.
    if (await page.getByTestId("dq-threshold").isVisible()) {
      await page.getByTestId("dq-threshold").fill("0");
    }
    await page.getByTestId("dq-add-check").click();
    await expect(page.getByTestId("dq-check-rows")).toContainText(c.check);

    await page.getByTestId("register-table-submit").click();

    // The registered row lands in the list carrying the checker it runs under.
    const badge = page.getByTestId(`tables-checker-${resultsTable}`);
    await expect(badge).toBeVisible({ timeout: 20000 });
    await expect(badge).toHaveText(c.checker);
  });
}
