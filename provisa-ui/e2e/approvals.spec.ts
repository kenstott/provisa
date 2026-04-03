import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("ApprovalsPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({ timeout: 10000 });
  });

  test("shows pending queries", async ({ page }) => {
    await expect(page.locator("h3", { hasText: "GetOrders" })).toBeVisible();
    await expect(page.locator("h3", { hasText: "GetCustomers" })).toBeVisible();
    await expect(page.locator(".submitted-by", { hasText: "dev@co.com" }).first()).toBeVisible();
    await expect(page.locator("pre.approval-query", { hasText: "orders" })).toBeVisible();
  });

  test("approves a query via confirm dialog", async ({ page }) => {
    await page.getByRole("button", { name: "Approve" }).first().click();
    await expect(page.locator(".modal")).toBeVisible();
    await expect(page.locator(".consequence")).toContainText("production use");
    await page.getByRole("button", { name: "Confirm" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
  });

  test("opens rejection form", async ({ page }) => {
    await page.getByRole("button", { name: "Reject" }).first().click();
    await expect(page.locator(".reject-form")).toBeVisible();
    await expect(page.locator("textarea")).toBeVisible();
  });

  test("submits rejection with reason", async ({ page }) => {
    await page.getByRole("button", { name: "Reject" }).first().click();
    await page.locator("textarea").fill("Missing WHERE clause for RLS compliance");
    await page.getByRole("button", { name: "Submit Rejection" }).click();
    // Reject form should close
    await expect(page.locator(".reject-form")).not.toBeVisible({ timeout: 5000 });
  });

  test("submit rejection disabled when reason empty", async ({ page }) => {
    await page.getByRole("button", { name: "Reject" }).first().click();
    await expect(page.getByRole("button", { name: "Submit Rejection" })).toBeDisabled();
  });

  test("cancel rejection hides form", async ({ page }) => {
    await page.getByRole("button", { name: "Reject" }).first().click();
    await expect(page.locator(".reject-form")).toBeVisible();
    await page.locator(".reject-form").getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".reject-form")).not.toBeVisible();
  });

  test("empty state shows no queries message", async ({ page }) => {
    await setupMocks(page, { pendingQueries: [] });
    await page.goto("/approvals");
    await expect(page.locator("p", { hasText: "No queries pending" })).toBeVisible({ timeout: 10000 });
  });
});
