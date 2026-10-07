// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2b6d14-5c9a-4f37-b1e8-a40c7d3f9e52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import type { Route } from "@playwright/test";

// REQ-1944: a data steward -- the seeded role, holding the hiding rights and not
// table_registration -- reaches the Tables surface from the nav, is offered no registration,
// and opens a table's editor on how its columns are hidden alone. The identity the server
// reports is the real one with its role assignment replaced by the seeded data_steward, so the
// client derives its rights from the role definition the backend serves.
test("a data steward reaches Tables and edits only how columns are hidden", async ({ page }) => {
  await page.route("**/auth/me", async (route: Route) => {
    const real = await route.fetch();
    const me = await real.json();
    await route.fulfill({
      response: real,
      json: { ...me, dev_mode: false, assignments: [{ role_id: "data_steward", domain_id: "*" }] },
    });
  });

  await page.goto("/");
  const nav = page.locator('[data-tour="nav-tables"]');
  await expect(nav).toBeVisible({ timeout: 60000 });
  await nav.click();
  await expect(page).toHaveURL(/\/tables/);
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await expect(page.getByTestId("tables-add-toggle")).toHaveCount(0);
  await expect(page.getByTestId("views-add-toggle")).toHaveCount(0);
  await expect(page.getByTestId("tables-model-toggle")).toHaveCount(0);

  const row = page.locator("tr.clickable").first();
  await row.waitFor({ timeout: 15000 });
  await row.click();
  await expect(page.getByTestId("table-read-view-edit")).toBeVisible();
  await expect(page.getByTestId("table-read-view-delete")).toHaveCount(0);
  await expect(page.getByTestId("table-read-view-profile")).toHaveCount(0);

  await page.getByTestId("table-read-view-edit").click();
  await expect(page.getByTestId("table-edit-hiding-only")).toBeVisible();
  await expect(page.getByTestId("load-management-panel-toggle")).toHaveCount(0);
});
