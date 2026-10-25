// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1426: a derived view's columns are the ones it is registered with, typed. The table editor
// reads each column's type from the server's column listing, and a derived view's listing was
// empty on the native engine (it read "memory".information_schema, which no native engine has),
// so the live-delivery watermark picker offered every column of the view, a varchar among them.
// The pre-seeded `dim_pet` view (source_id: __derived__) has two columns a watermark can be kept
// in -- available and id, both bigint -- and four it cannot.

import { test, expect } from "./coverage";

test.beforeEach(async ({ page }) => {
  // Demo mode auto-starts the guided tour in a fresh profile, which navigates away from /tables.
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
});

test("a derived view's watermark picker offers only the columns its types allow", async ({
  page,
}) => {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 30000 });
  const row = page.locator("tr").filter({ hasText: "dim_pet" }).first();
  await row.waitFor({ timeout: 15000 });
  await row.click();
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 15000 });
  await editBtn.click();
  await page.getByTestId("table-section-load").click({ timeout: 30000 });

  const live = page.getByTestId("live-delivery-enable");
  if (!(await live.isChecked())) await live.check();
  const picker = page.getByTestId("watermark-column-select");
  await expect(picker).toBeVisible({ timeout: 15000 });
  await picker.click();
  const options = page.getByRole("listbox").getByRole("option");
  // "None" first, then the view's watermark-eligible columns.
  await expect(options).toHaveCount(3, { timeout: 15000 });
  const labels = await options.allTextContents();
  expect(labels.slice(1).sort()).toEqual(["available", "id"]);
});
