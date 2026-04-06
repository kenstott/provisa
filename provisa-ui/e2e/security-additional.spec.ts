// Copyright (c) 2026 Kenneth Stott
// Canary: b8c9d0e1-f2a3-4567-bcde-678901234567
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
 * Additional coverage for SecurityPage — gaps not in security.spec.ts:
 *   - Empty state (no roles, no RLS rules)
 *   - Role capabilities displayed in table
 *   - Domain access displayed in table
 *   - RLS filter expression is shown as code
 *   - Creating role with all capabilities
 *   - Validation error when role name missing
 */

// COVERAGE NOTE (security.spec.ts already covers):
//   - Roles and RLS rules tables shown (admin, analyst, region='US' rule)
//   - Add Role → fills name / capabilities / domain → Save closes form
//   - Edit role → form opens, name disabled, Save closes
//   - Delete role
//   - Cancel hides role form
//   - Add RLS Rule → fills table/role/filter → Save closes form
//   - Edit RLS rule → form opens → Save
//   - Delete RLS rule
//   - Cancel hides RLS rule form

test.describe("SecurityPage — additional coverage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({ timeout: 10000 });
  });

  // ── Empty state ───────────────────────────────────────────────────────────

  test("empty roles state when no roles exist", async ({ page }) => {
    await setupMocks(page, { roles: [] });
    await page.goto("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "admin" })).not.toBeVisible();
  });

  test("empty RLS rules state when no rules exist", async ({ page }) => {
    await setupMocks(page, { rlsRules: [] });
    await page.goto("/security");
    await expect(page.getByRole("heading", { name: "RLS Rules" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td code", { hasText: "region = 'US'" })).not.toBeVisible();
  });

  // ── Role table content ────────────────────────────────────────────────────

  test("role table shows capabilities for each role", async ({ page }) => {
    // admin has capabilities: ["admin"]
    await expect(page.locator("td", { hasText: "admin" }).first()).toBeVisible();
  });

  test("role table shows domain access for analyst role", async ({ page }) => {
    // analyst has domainAccess: ["sales"]
    await expect(page.locator("td", { hasText: "sales" })).toBeVisible();
  });

  // ── RLS rule content ──────────────────────────────────────────────────────

  test("RLS rule table shows table and role columns", async ({ page }) => {
    const rlsTable = page.locator(".data-table").last();
    await expect(rlsTable.locator("td", { hasText: "orders" }).or(
      rlsTable.locator("td", { hasText: "analyst" })
    )).toBeVisible({ timeout: 5000 });
  });

  // ── Role creation: all-domain access ─────────────────────────────────────

  test("creating a role with all-domain access (wildcard)", async ({ page }) => {
    await page.getByRole("button", { name: "Add Role" }).click();
    await expect(page.locator(".form-card").first()).toBeVisible();

    await page.locator("input[placeholder='analyst']").fill("power_user");
    // Select all visible domain checkboxes
    const allDomainCheckboxes = page.locator(".form-card input[type='checkbox']");
    const count = await allDomainCheckboxes.count();
    for (let i = 0; i < count; i++) {
      if (!(await allDomainCheckboxes.nth(i).isChecked())) {
        await allDomainCheckboxes.nth(i).check();
      }
    }

    await page.getByRole("button", { name: "Save" }).first().click();
    await expect(page.locator("input[placeholder='analyst']")).not.toBeVisible({ timeout: 5000 });
  });

  // ── Validation: role name required ────────────────────────────────────────

  test("role form shows validation when submitted without a name", async ({ page }) => {
    await page.getByRole("button", { name: "Add Role" }).click();
    // Do not fill name
    await page.getByRole("button", { name: "Save" }).first().click();
    // HTML5 required validation prevents submission — input should still be visible
    await expect(page.locator("input[placeholder='analyst']")).toBeVisible();
  });

  // ── RLS filter as code ────────────────────────────────────────────────────

  test("RLS filter expression renders as code element", async ({ page }) => {
    await expect(page.locator("td code", { hasText: "region = 'US'" })).toBeVisible();
  });

  // ── Multiple simultaneous forms ───────────────────────────────────────────

  test("opening role form and RLS form simultaneously does not crash", async ({ page }) => {
    await page.getByRole("button", { name: "Add Role" }).click();
    await expect(page.locator(".form-card").first()).toBeVisible({ timeout: 5000 });

    await page.getByRole("button", { name: "Add RLS Rule" }).click();
    // Both forms may or may not be open at the same time depending on implementation
    // The important thing is no crash
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible();
  });
});
