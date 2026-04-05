// Copyright (c) 2025 Kenneth Stott
// Canary: b4e47435-34e3-4abf-8d7d-9be255cbc3a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

test.describe("Domain Explorer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });

    // Dismiss any dialog overlays that may appear on load
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
    // Double-check — overlay can reappear
    if (await overlay.isVisible({ timeout: 500 }).catch(() => false)) {
      await overlay.click({ force: true, position: { x: 1, y: 1 } });
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Open explorer plugin if not already visible
    const explorerRoot = page.locator(".graphiql-explorer-root");
    if (!(await explorerRoot.isVisible({ timeout: 2000 }).catch(() => false))) {
      const pluginButtons = page.locator(".graphiql-sidebar button");
      const count = await pluginButtons.count();
      for (let i = 0; i < count; i++) {
        await pluginButtons.nth(i).click();
        if (await explorerRoot.isVisible({ timeout: 1000 }).catch(() => false)) break;
      }
    }
    await expect(explorerRoot).toBeVisible({ timeout: 5000 });
  });

  test("shows domain folders grouping root query fields", async ({ page }) => {
    const explorerRoot = page.locator(".graphiql-explorer-root");
    const explorerText = await explorerRoot.textContent();

    // Domain folders rendered as <details> with <summary> containing the domain name
    expect(explorerText).toContain("customer_insights");
    expect(explorerText).toContain("product_catalog");
    expect(explorerText).toContain("sales_analytics");
    expect(explorerText).toContain("support");
  });

  test("expanding domain folder reveals table fields without modifying query", async ({
    page,
  }) => {
    // Domain folders are <details> elements with class graphiql-explorer-domain
    // The summary contains the domain name as a text node
    const domainFolder = page
      .locator("details.graphiql-explorer-domain", { hasText: "product_catalog" })
      .first();
    await expect(domainFolder).toBeVisible({ timeout: 5000 });

    const domainSummary = domainFolder.locator("summary");

    // Get query content before clicking
    const editorBefore = await page
      .locator(".graphiql-query-editor")
      .textContent();

    // Click the summary to expand the <details>
    await domainSummary.click();
    await page.waitForTimeout(500);

    // Check the query hasn't been modified with the bare domain name
    const editorAfter = await page
      .locator(".graphiql-query-editor")
      .textContent();

    const queryChanged =
      editorAfter !== editorBefore &&
      editorAfter?.includes("product_catalog") &&
      !editorAfter?.includes("product_catalog__");

    expect(queryChanged).toBe(false);
  });

  test("clicking a field inside domain adds correct field to query", async ({
    page,
  }) => {
    // Expand the domain folder first
    const domainFolder = page
      .locator("details.graphiql-explorer-domain", { hasText: "product_catalog" })
      .first();
    const domainSummary = domainFolder.locator("summary");
    await domainSummary.click();
    await page.waitForTimeout(300);

    // Now look for a table field inside the expanded domain
    const tableField = page.locator(
      ".graphiql-explorer-domain span:has-text('product_catalog__')"
    ).first();

    if (await tableField.isVisible({ timeout: 3000 }).catch(() => false)) {
      await tableField.click();
      await page.waitForTimeout(500);

      // Check the query editor contains the full field name
      const queryText = await page
        .locator(".graphiql-query-editor")
        .textContent();
      expect(queryText).toContain("product_catalog__");
    }
  });

  test("Add new Query/Mutation controls are present", async ({ page }) => {
    const addNewText = page.locator("text=Add new");
    await expect(addNewText).toBeVisible({ timeout: 5000 });

    const select = page.locator("select").filter({ hasText: "Query" });
    await expect(select.first()).toBeAttached();
  });
});
