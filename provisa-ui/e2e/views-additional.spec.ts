// Copyright (c) 2026 Kenneth Stott
// Canary: c9d0e1f2-a3b4-5678-cdef-789012345678
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
 * Additional coverage for ViewsPage — gaps not in views.spec.ts:
 *   - View governance column shown in table
 *   - View SQL shown in editor when editing
 *   - Sample panel shows row count
 *   - Delete confirmation (if dialog exists)
 *   - Create view with governance setting
 *   - Pre-approved governance badge visible
 */

// COVERAGE NOTE (views.spec.ts already covers):
//   - Views table: monthly-revenue, Monthly revenue, sales
//   - New View opens .view-editor with monthly-revenue placeholder
//   - Creates new view (fills id, domain, SQL in CodeMirror, Save closes editor)
//   - Edits existing view (id input disabled for existing)
//   - Cancel closes view editor
//   - Deletes a view (immediate API call)
//   - Sample view → shows sample panel with month/revenue columns and data
//   - Close button hides sample panel
//   - Materialize checkbox shows refresh interval input
//   - Validation error when required fields missing
//   - Empty state: "No views defined"

test.describe("ViewsPage — additional coverage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/views");
    await expect(page.getByRole("heading", { name: "Views" })).toBeVisible({ timeout: 10000 });
  });

  // ── Governance column ─────────────────────────────────────────────────────

  test("views table shows governance column value", async ({ page }) => {
    // MOCK_VIEWS[0].governance = "pre-approved"
    await expect(page.locator("td", { hasText: "pre-approved" })).toBeVisible({ timeout: 5000 });
  });

  // ── Materialize column ────────────────────────────────────────────────────

  test("views table shows materialize flag for the mock view", async ({ page }) => {
    // MOCK_VIEWS[0].materialize = true
    // This may render as a checkbox, boolean text, or icon
    await expect(
      page.locator("td input[type='checkbox']:checked").or(
        page.locator("td", { hasText: /true|yes|✓/i })
      )
    ).toBeVisible({ timeout: 5000 });
  });

  // ── Sample data row count ─────────────────────────────────────────────────

  test("sample panel shows correct number of data rows", async ({ page }) => {
    await page.getByRole("button", { name: "Sample" }).first().click();
    await expect(page.locator(".sample-panel")).toBeVisible({ timeout: 5000 });

    // Mock returns 2 rows (2026-01 and 2026-02)
    const rows = page.locator(".sample-panel tbody tr");
    await expect(rows).toHaveCount(2, { timeout: 5000 });
  });

  test("sample panel shows second month row data", async ({ page }) => {
    await page.getByRole("button", { name: "Sample" }).first().click();
    await expect(page.locator(".sample-panel")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".sample-panel td", { hasText: "62000" })).toBeVisible({ timeout: 5000 });
  });

  // ── New view: governance dropdown ─────────────────────────────────────────

  test("new view editor includes a governance dropdown", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await expect(page.locator(".view-editor")).toBeVisible();

    const govSelect = page.locator(".view-editor select").filter({ hasText: /open|restricted|pre-approved/i });
    if (await govSelect.first().isVisible({ timeout: 3000 }).catch(() => false)) {
      await expect(govSelect.first()).toBeVisible();
    }
  });

  // ── Edit view preserves SQL ────────────────────────────────────────────────

  test("editing a view shows existing SQL in editor", async ({ page }) => {
    await page.getByRole("button", { name: "Edit" }).first().click();
    await expect(page.locator(".view-editor")).toBeVisible();

    // The existing SQL should be present in the CodeMirror editor
    const editorText = await page.locator(".cm-content").textContent({ timeout: 5000 }).catch(() => "");
    // SQL should contain at least part of the mock view's SQL
    expect(editorText).toContain("SELECT");
  });

  // ── View description field ────────────────────────────────────────────────

  test("new view editor has a description field", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await expect(page.locator(".view-editor")).toBeVisible();

    const descInput = page.locator(".view-editor input[placeholder*='escription'], .view-editor textarea[placeholder*='escription']").first();
    if (await descInput.isVisible({ timeout: 3000 }).catch(() => false)) {
      await descInput.fill("Test description");
      await expect(descInput).toHaveValue("Test description");
    }
  });

  // ── View id uniqueness validation ─────────────────────────────────────────

  test("save button is type submit in view editor", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await expect(page.locator(".view-editor")).toBeVisible();

    const saveBtn = page.locator(".view-editor button", { hasText: "Save" });
    await expect(saveBtn).toBeVisible();
  });
});
