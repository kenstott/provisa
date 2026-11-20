// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2260: a list page reopens the item whose detail was open when the viewer comes back to it by
// its navigation link, with the address naming it as when it was opened.

import { test, expect } from "./coverage";

// The identity bootstrap (app boot, /setup/status, /auth/me, roles) precedes the list.
test.describe.configure({ timeout: 120_000 });

test("a relationship's detail is open again after a visit to Tables", async ({ page }) => {
  await page.goto("/relationships");
  const rows = page.locator('[data-testid="relationships-list"] tr.list-row');
  await expect(rows.first()).toBeVisible({ timeout: 60_000 });

  await rows.first().click();
  await expect(page).toHaveURL(/[?&]relationship=[^&]+/);
  const opened = new URL(page.url()).searchParams.get("relationship");
  expect(opened).not.toBeNull();
  await expect(page.getByTestId("relationships-split-panel")).toBeVisible();

  await page.locator('[data-tour="nav-tables"]').click();
  await expect(page).toHaveURL(/\/tables(\?|$)/);
  await expect(page.locator('[data-testid="relationships-list"]')).toHaveCount(0);

  await page.locator('[data-tour="nav-relationships"]').click();
  await expect(page).toHaveURL(new RegExp(`/relationships\\?relationship=${opened}$`));
  await expect(page.getByTestId("relationships-split-panel")).toBeVisible();
  const selected = page.locator('[data-testid="relationships-list"] tr.list-row.row-selected');
  await expect(selected).toHaveCount(1);
  await expect(selected).toBeInViewport();

  // Closed, it is not reopened on the next return.
  await selected.click();
  await expect(page).toHaveURL(/\/relationships$/);
  await page.locator('[data-tour="nav-tables"]').click();
  await expect(page).toHaveURL(/\/tables(\?|$)/);
  await page.locator('[data-tour="nav-relationships"]').click();
  await expect(rows.first()).toBeVisible({ timeout: 60_000 });
  await expect(page).toHaveURL(/\/relationships$/);
});
