import { test, expect } from "./coverage";

/**
 * Live e2e test: submit a query from the Query page and approve it from the Approvals page.
 * Requires a running backend with registered tables.
 */
test.describe("Submit and Approve query flow", () => {
  test.describe.configure({ timeout: 60000 });

  test("submit a query via the Query page UI", async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });

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

    // Focus the GraphiQL Monaco editor and replace content
    const queryEditor = page.locator(".graphiql-query-editor");
    await queryEditor.click({ force: true });
    await page.waitForTimeout(300);
    // Select all and delete existing content
    await page.keyboard.press("Meta+a");
    await page.keyboard.press("Backspace");
    await page.waitForTimeout(200);

    // Type a named query
    const queryName = `E2eSubmit_${Date.now()}`;
    await page.keyboard.type(
      `query ${queryName} { sales_analytics__orders(limit: 5) { id amount status } }`,
      { delay: 5 },
    );

    // Open the Provisa plugin panel
    const sidebar = page.locator(".graphiql-sidebar");
    const sidebarButtons = sidebar.locator("button");
    const provisaPanel = page.locator(".provisa-tools");

    const btnCount = await sidebarButtons.count();
    for (let i = 0; i < btnCount; i++) {
      const label = await sidebarButtons.nth(i).getAttribute("aria-label");
      if (label?.toLowerCase().includes("provisa")) {
        await sidebarButtons.nth(i).click();
        break;
      }
    }
    if (!(await provisaPanel.isVisible({ timeout: 3000 }).catch(() => false))) {
      for (let i = 0; i < btnCount; i++) {
        await sidebarButtons.nth(i).click();
        if (await provisaPanel.isVisible({ timeout: 500 }).catch(() => false)) break;
      }
    }
    await expect(provisaPanel).toBeVisible({ timeout: 5000 });

    // Click "Submit for Approval" to open the form
    await provisaPanel.locator("button", { hasText: "Submit for Approval" }).click();
    await expect(page.locator(".provisa-tools-metadata")).toBeVisible({ timeout: 3000 });

    // Fill required business purpose
    await page.locator(".provisa-tools-metadata textarea").first().fill("E2E test: submit flow");

    // Click Submit
    await provisaPanel.locator("button.submit-btn", { hasText: "Submit" }).click();

    // Wait for success message
    await expect(page.locator(".provisa-tools-success")).toBeVisible({ timeout: 15000 });
    const msg = await page.locator(".provisa-tools-success").textContent();
    expect(msg).toContain("submitted for approval");
  });

  test("approve a submitted query via the Approvals page UI", async ({ page, request }) => {
    // Submit a query via the API so this test is independent
    const queryName = `E2eApprove_${Date.now()}`;
    const submitResp = await request.post("http://localhost:8001/data/submit", {
      headers: { "Content-Type": "application/json", "X-Role": "admin" },
      data: {
        query: `query ${queryName} { sales_analytics__orders(limit: 3) { id region } }`,
        operation_name: queryName,
        developer_id: "e2e-test",
        business_purpose: "E2E test: approve flow",
        data_sensitivity: "internal",
      },
    });
    expect(submitResp.ok()).toBeTruthy();

    // Navigate to approvals page
    await page.goto("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({ timeout: 10000 });

    // Find our query card
    const card = page.locator(".approval-card", { hasText: queryName });
    await expect(card).toBeVisible({ timeout: 10000 });

    // Verify query text is shown
    await expect(card.locator("pre.approval-query")).toContainText("sales_analytics__orders");

    // Click Approve
    await card.locator("button.approve").click();

    // Confirm dialog
    await expect(page.locator(".modal")).toBeVisible({ timeout: 3000 });
    await page.locator("button", { hasText: "Confirm" }).click();

    // Card disappears from queue
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
    await expect(card).not.toBeVisible({ timeout: 5000 });
  });
});
