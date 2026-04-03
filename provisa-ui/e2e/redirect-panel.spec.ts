import { test, expect } from "./coverage";

/**
 * Regression test for GitHub issue #1:
 * Changing the Redirect dropdown on the Query page must NOT blank the explorer panel.
 */
test.describe("Redirect dropdown does not blank explorer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });

    // Dismiss any dialog overlays
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
    if (await overlay.isVisible({ timeout: 500 }).catch(() => false)) {
      await overlay.click({ force: true, position: { x: 1, y: 1 } });
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }

    // Ensure explorer plugin is open
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

  test("explorer panel stays visible after changing redirect format", async ({ page }) => {
    const explorerRoot = page.locator(".graphiql-explorer-root");
    const explorerContentBefore = await explorerRoot.textContent();
    expect(explorerContentBefore).toBeTruthy();

    // Change the Redirect dropdown to "Parquet"
    const redirectSelect = page.locator(".query-option select");
    await expect(redirectSelect).toBeVisible();
    await redirectSelect.selectOption("parquet");

    // Explorer panel must remain visible and retain its content
    await expect(explorerRoot).toBeVisible();
    const explorerContentAfter = await explorerRoot.textContent();
    expect(explorerContentAfter).toBeTruthy();
    expect(explorerContentAfter).toBe(explorerContentBefore);
  });

  test("explorer panel survives multiple redirect format changes", async ({ page }) => {
    const explorerRoot = page.locator(".graphiql-explorer-root");
    const explorerContentBefore = await explorerRoot.textContent();

    const redirectSelect = page.locator(".query-option select");
    await expect(redirectSelect).toBeVisible();

    // Cycle through several format options
    for (const format of ["parquet", "csv", "arrow", ""]) {
      await redirectSelect.selectOption(format);
      await expect(explorerRoot).toBeVisible();
    }

    // Content should still match original
    const explorerContentAfter = await explorerRoot.textContent();
    expect(explorerContentAfter).toBe(explorerContentBefore);
  });

  test("query editor retains content after redirect change", async ({ page }) => {
    const queryEditor = page.locator(".graphiql-query-editor");
    await expect(queryEditor).toBeVisible();
    const editorBefore = await queryEditor.textContent();

    // Change redirect format
    const redirectSelect = page.locator(".query-option select");
    await redirectSelect.selectOption("parquet");

    // Query editor should keep its content
    const editorAfter = await queryEditor.textContent();
    expect(editorAfter).toBe(editorBefore);
  });
});
