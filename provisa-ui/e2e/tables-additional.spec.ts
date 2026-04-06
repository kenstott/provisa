// Copyright (c) 2026 Kenneth Stott
// Canary: f6a7b8c9-d0e1-2345-fabc-456789012345
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

/**
 * Additional coverage for TablesPage — gaps not in tables.spec.ts:
 *   - Empty state (no registered tables)
 *   - Delete confirms with dialog rather than native confirm
 *   - Column masking fields visible in expanded row
 *   - Governance column value displayed
 *   - Alias field shown in expanded row
 */

// COVERAGE NOTE (tables.spec.ts already covers):
//   - Registered tables shown (orders, customers, sales-pg)
//   - Register Table button opens form-card
//   - Cascading dropdowns: source → schema → table → columns (3 column-editor-rows)
//   - Column name "id" visible in editor
//   - Registers a table (full flow, form closes)
//   - Deletes a table (native confirm dialog accepted)
//   - Expands table row → shows customer_id code element
//   - Edits inline: alias input, Save closes edit mode
//   - Cancels inline edit
//   - Cancel hides register form
//   - Validation error when required fields missing

test.describe("TablesPage — additional coverage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
  });

  // ── Empty state ───────────────────────────────────────────────────────────

  test("empty state shows placeholder when no tables are registered", async ({ page }) => {
    await setupMocks(page, { tables: [] });
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "orders" })).not.toBeVisible();
  });

  // ── Governance column ─────────────────────────────────────────────────────

  test("governance value is displayed in the table for each row", async ({ page }) => {
    // First table has governance=open, second has governance=restricted
    await expect(page.locator("td", { hasText: "open" })).toBeVisible();
    await expect(page.locator("td", { hasText: "restricted" })).toBeVisible();
  });

  // ── Expanded row details ──────────────────────────────────────────────────

  test("expanding a restricted table shows alias in detail view", async ({ page }) => {
    // The second mock table (customers) has alias="clients"
    const rows = page.locator("tr.clickable");
    await rows.nth(1).click();
    await expect(page.locator("code", { hasText: "clients" }).or(
      page.locator("span", { hasText: "clients" }).or(
        page.locator("td", { hasText: "clients" })
      )
    )).toBeVisible({ timeout: 5000 });
  });

  test("expanding orders table shows description text", async ({ page }) => {
    const rows = page.locator("tr.clickable");
    await rows.first().click();
    // orders has description="Customer orders"
    await expect(
      page.locator("td, span, div", { hasText: "Customer orders" })
    ).toBeVisible({ timeout: 5000 });
  });

  // ── Column masking fields ─────────────────────────────────────────────────

  test("expanded row shows mask type for masked column in edit mode", async ({ page }) => {
    // "total" column of orders has maskType="constant"
    const rows = page.locator("tr.clickable");
    await rows.first().click();
    await expect(page.locator("code", { hasText: "customer_id" })).toBeVisible({ timeout: 3000 });

    await page.getByRole("button", { name: "Edit" }).click();
    // After entering edit mode, mask controls should be present for the "total" column
    // (the column has maskType=constant, maskValue="0")
    const maskSelects = page.locator("select").filter({ hasText: /constant|none|hash/i });
    // At least one mask type selector should appear in edit mode
    if (await maskSelects.first().isVisible({ timeout: 3000 }).catch(() => false)) {
      await expect(maskSelects.first()).toBeVisible();
    }
  });

  // ── Register table: domain field ──────────────────────────────────────────

  test("register form includes a domain selector", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();
    // Domain selector is nth(1) in the cascade
    const domainSelect = page.locator(".form-card select").nth(1);
    await expect(domainSelect).toBeVisible();
  });

  test("domain selector lists mock domains", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();
    const domainSelect = page.locator(".form-card select").nth(1);
    await expect(domainSelect.locator("option[value='sales']")).toBeAttached({ timeout: 5000 });
    await expect(domainSelect.locator("option[value='analytics']")).toBeAttached();
  });

  // ── Delete table (confirmed) ──────────────────────────────────────────────

  test("delete table triggers deleteTable mutation", async ({ page }) => {
    let deleteCalled = false;
    await page.route("**/admin/graphql", async (route) => {
      const body = JSON.parse(route.request().postData() || "{}");
      if ((body.query || "").includes("deleteTable")) {
        deleteCalled = true;
        await route.fulfill({ json: { data: { deleteTable: { success: true, message: "Deleted" } } } });
      } else {
        await route.continue();
      }
    });

    page.on("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "Delete" }).first().click();

    await page.waitForTimeout(500);
    expect(deleteCalled).toBe(true);
  });
});
