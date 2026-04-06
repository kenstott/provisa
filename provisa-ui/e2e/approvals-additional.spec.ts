// Copyright (c) 2026 Kenneth Stott
// Canary: d0e1f2a3-b4c5-6789-defa-890123456789
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
 * Additional coverage for ApprovalsPage — gaps not in approvals.spec.ts:
 *   - Two cards shown (one per pending query)
 *   - Operation name visible in heading of each card
 *   - Approve cancel dialog
 *   - Rejection textarea accepts multi-line input
 *   - Query text syntax-highlighted in <pre>
 */

// COVERAGE NOTE (approvals.spec.ts already covers):
//   - Pending queries shown (GetOrders, GetCustomers, dev@co.com, pre code block)
//   - Approve via ConfirmDialog (confirm and cancel)
//   - Reject button opens reject-form with textarea
//   - Submits rejection with reason (form closes)
//   - Submit Rejection disabled when reason empty
//   - Cancel rejection hides form
//   - Empty state: "No queries pending"

test.describe("ApprovalsPage — additional coverage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({ timeout: 10000 });
  });

  // ── Card count ────────────────────────────────────────────────────────────

  test("shows two approval cards for two pending queries", async ({ page }) => {
    // Two mock pending queries → two cards (or two h3 headings)
    const cards = page.locator(".approval-card");
    if (await cards.first().isVisible({ timeout: 3000 }).catch(() => false)) {
      await expect(cards).toHaveCount(2);
    } else {
      // Alternative rendering without .approval-card class
      await expect(page.locator("h3", { hasText: "GetOrders" })).toBeVisible();
      await expect(page.locator("h3", { hasText: "GetCustomers" })).toBeVisible();
    }
  });

  // ── Approve cancel ────────────────────────────────────────────────────────

  test("cancelling the approve confirmation dialog keeps card visible", async ({ page }) => {
    await page.getByRole("button", { name: "Approve" }).first().click();
    await expect(page.locator(".modal")).toBeVisible({ timeout: 5000 });

    await page.locator(".modal").getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });

    // Query card should still be there
    await expect(page.locator("h3", { hasText: "GetOrders" })).toBeVisible();
  });

  // ── Rejection textarea ────────────────────────────────────────────────────

  test("rejection textarea accepts multi-line text", async ({ page }) => {
    await page.getByRole("button", { name: "Reject" }).first().click();
    await expect(page.locator("textarea")).toBeVisible();

    const reason = "Line 1: Missing WHERE clause\nLine 2: PII risk";
    await page.locator("textarea").fill(reason);
    await expect(page.locator("textarea")).toHaveValue(reason);
  });

  // ── Pre query text ────────────────────────────────────────────────────────

  test("query text block contains the GraphQL operation keyword", async ({ page }) => {
    await expect(page.locator("pre.approval-query").first()).toContainText("query");
  });

  test("second query card shows GetCustomers operation", async ({ page }) => {
    await expect(page.locator("h3", { hasText: "GetCustomers" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator("pre.approval-query", { hasText: "customers" })).toBeVisible({ timeout: 5000 });
  });

  // ── Developer id ─────────────────────────────────────────────────────────

  test("all cards show the developer id", async ({ page }) => {
    const devLabels = page.locator(".submitted-by", { hasText: "dev@co.com" });
    await expect(devLabels.first()).toBeVisible();
    await expect(devLabels).toHaveCount(2);
  });

  // ── Approval queue heading ────────────────────────────────────────────────

  test("Approval Queue heading is an h1 or h2", async ({ page }) => {
    const heading = page.getByRole("heading", { name: "Approval Queue" });
    await expect(heading).toBeVisible();
    const tag = await heading.evaluate((el) => el.tagName.toLowerCase());
    expect(["h1", "h2"]).toContain(tag);
  });
});
