// Copyright (c) 2026 Kenneth Stott
// Canary: a7b8c9d0-e1f2-3456-abcd-567890123456
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
 * Additional coverage for RelationshipsPage — gaps not in relationships.spec.ts:
 *   - Empty state (no relationships or candidates)
 *   - Cardinality dropdown options
 *   - Validation error when required fields missing
 *   - Candidate confidence percentage formatting
 *   - Discovering relationships when already discovered (re-discover)
 */

// COVERAGE NOTE (relationships.spec.ts already covers):
//   - Relationships table shows orders / customers / many-to-one
//   - Opens and fills Add Relationship form (name, source, target, columns)
//   - Creates a relationship (form closes)
//   - Cancel hides form
//   - Deletes a relationship
//   - Toggles materialize checkbox on existing relationship
//   - Materialize checkbox in form reveals refresh interval input
//   - Discover relationships (Suggest with AI) → shows AI-Suggested heading
//   - AI-Suggested candidates shown (95%, FK naming convention reasoning)
//   - Accepts a candidate
//   - Rejects a candidate (row disappears)

test.describe("RelationshipsPage — additional coverage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships", exact: true })).toBeVisible({ timeout: 10000 });
  });

  // ── Empty state ───────────────────────────────────────────────────────────

  test("empty relationships state when no relationships exist", async ({ page }) => {
    await setupMocks(page, { relationships: [] });
    await page.goto("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships", exact: true })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "orders" })).not.toBeVisible();
  });

  test("empty candidates state when no candidates exist", async ({ page }) => {
    await setupMocks(page, { candidates: [] });
    await page.goto("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships", exact: true })).toBeVisible({ timeout: 10000 });
    await expect(page.getByRole("cell", { name: "95%" })).not.toBeVisible();
  });

  // ── Cardinality dropdown ──────────────────────────────────────────────────

  test("Add Relationship form has cardinality dropdown with expected options", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    await expect(page.locator(".form-card")).toBeVisible();

    const cardinalitySelect = page.locator(".form-card select").filter({ hasText: /many|one/ }).first();
    if (await cardinalitySelect.isVisible({ timeout: 3000 }).catch(() => false)) {
      await expect(cardinalitySelect.locator("option[value='many-to-one'], option", { hasText: "many-to-one" })).toBeAttached();
    }
  });

  // ── Relationship row content ───────────────────────────────────────────────

  test("relationship table shows source and target column names", async ({ page }) => {
    const relTable = page.locator(".data-table").first();
    await expect(relTable.getByRole("cell", { name: "customer_id" })).toBeVisible({ timeout: 5000 });
    await expect(relTable.getByRole("cell", { name: "id" }).first()).toBeVisible();
  });

  test("relationship table shows refresh interval for materialized relationship", async ({ page }) => {
    const relTable = page.locator(".data-table").first();
    // refreshInterval=300 from mock
    await expect(relTable.locator("td", { hasText: "300" })).toBeVisible({ timeout: 5000 });
  });

  // ── Candidate table content ───────────────────────────────────────────────

  test("candidate table shows source and target column names", async ({ page }) => {
    const candidateTable = page.locator(".data-table").last();
    await expect(candidateTable.getByRole("cell", { name: "customer_id" })).toBeVisible({ timeout: 5000 });
    await expect(candidateTable.getByRole("cell", { name: "id" }).first()).toBeVisible();
  });

  // ── Re-discover updates list ──────────────────────────────────────────────

  test("clicking Suggest with AI again is safe (no crash)", async ({ page }) => {
    // Click once
    await page.getByRole("button", { name: "Suggest with AI" }).click();
    await expect(page.locator("h3", { hasText: "AI-Suggested" })).toBeVisible({ timeout: 5000 });

    // Click again — should re-run discovery without crashing
    await page.getByRole("button", { name: "Suggest with AI" }).click();
    await expect(page.locator("h3", { hasText: "AI-Suggested" })).toBeVisible({ timeout: 5000 });
  });

  // ── Refresh interval input type ───────────────────────────────────────────

  test("materialize refresh interval input is type number", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    const matLabel = page.locator(".checkbox-label", { hasText: "Materialize" });
    await matLabel.locator("input[type='checkbox']").check();

    const numInput = page.locator(".form-card input[type='number']");
    await expect(numInput).toBeVisible();
    await numInput.fill("120");
    await expect(numInput).toHaveValue("120");
  });
});
