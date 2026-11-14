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
// The shipped checkers (dq-checker, dq-soda) are system sources, which the Register Table form's
// source picker leaves out, so each case adds its own checker source through the Sources form, as
// a user would: a Great Expectations one and a Soda one, each connecting back through this
// backend's own pgwire (fillCheckerConnection). Neither checker library has to be installed to
// REGISTER: the contract is YAML, the dataset resolves against the governed tables, and the results
// envelope ships with Provisa. What this proves end to end is the form's claim — pick the table to
// scan, add a rule, submit — and that the registered row comes back carrying the checker it runs
// under. Each case removes the source it added, with its table.

import { test, expect, type Page } from "./coverage";
import {
  fillCheckerConnection,
  openRegisterForm,
  openSourcesForm,
  removeTestSource,
  saveRegistration,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

/** Mantine Select: open it, then pick the option by its rendered label. */
async function pickOption(page: Page, testId: string, label: string) {
  await page.getByTestId(testId).click();
  await page.getByRole("option", { name: label, exact: true }).click();
}

/** Add a checker source of `type` through the Sources form; it opens in edit once created. */
async function addCheckerSource(page: Page, sourceId: string, type: string) {
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption(type);
  await fillCheckerConnection(page);
  await submitSourceAndExpectListed(page, sourceId);
}

const CHECKERS = [
  {
    type: "great_expectations",
    short: "gx",
    checker: "Great Expectations",
    scanned: "pet_store.inquiries",
    column: /^message/,
    check: "expect_column_values_to_not_be_null",
  },
  {
    type: "soda",
    short: "soda",
    checker: "Soda",
    scanned: "pet_store.pet_visits",
    column: /^visit_date/,
    check: "missing",
  },
] as const;

// The source the running case added, removed after it whatever its outcome.
let addedSource: string | undefined;

test.afterEach(async () => {
  if (addedSource === undefined) return;
  const sourceId = addedSource;
  addedSource = undefined;
  await removeTestSource(sourceId);
});

for (const c of CHECKERS) {
  test(`registers a ${c.checker} results table by picking the table to scan and adding a rule`, async ({
    page,
  }) => {
    test.setTimeout(120000);
    const stamp = Date.now();
    // e2e_<kind>_<13-digit stamp>: the shape removeTestSources tells a test's source by.
    const sourceId = `e2e_${c.type}_${stamp}`;
    // Unique across retries and workers; kept short, as the result relation appends the source id.
    const resultsTable = `${c.short}_${stamp}`;

    addedSource = sourceId;
    await addCheckerSource(page, sourceId, c.type);
    await openRegisterForm(page, sourceId);

    // A checker source is not introspected: no schema/table pickers, the contract panel instead.
    await expect(page.getByTestId("register-table-dq")).toBeVisible();
    await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
    await expect(page.getByTestId("register-table-table-select")).toHaveCount(0);
    await expect(page.getByTestId("dq-run-now")).toHaveCount(0);

    // Pick the governed table to scan; the results table's name is derived from it.
    await pickOption(page, "dq-dataset-table-select", c.scanned);
    const [, scannedTable] = c.scanned.split(".");
    await expect(page.getByTestId("register-table-dq-results-table")).toHaveValue(
      `${scannedTable}_quality`,
    );
    await expect(page.getByPlaceholder("Semantic name override")).toHaveValue(
      `${scannedTable}_quality`,
    );
    // The name stays editable — keep the row unique across retries and workers. So does the alias,
    // derived from the scanned table: left as it is, it can meet one already registered in the
    // domain (run 40434830192).
    await page.getByTestId("register-table-dq-results-table").fill(resultsTable);
    await page.getByTestId("register-table-alias").fill(resultsTable);

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

    // The table scans on this interval; each scan's rows are kept (REQ-1443).
    await page.getByTestId("register-table-dq-scan-interval").fill("3600");
    await page.getByTestId("register-table-submit").click();
    // REQ-2109: the chosen table's settings follow in the editor's sections; Save registers it.
    await saveRegistration(page);

    // The registered row lands in the list carrying the checker it runs under, registered under
    // its result relation's name, <name>_<source>_dq.
    const relation = `${resultsTable}_${sourceId}_dq`;
    // The list shows 50 rows a page and every spec before this one has added tables, so the new
    // one can be on any page: filter the list to it.
    await page
      .getByRole("searchbox", { name: /Filter by source, domain, or table/ })
      .fill(relation);
    const badge = page.getByTestId(`tables-checker-${relation}`);
    await expect(badge).toBeVisible({ timeout: 20000 });
    await expect(badge).toHaveText(c.checker);
  });
}
