import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("ViewsPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/views");
    await expect(page.getByRole("heading", { name: "Views" })).toBeVisible({ timeout: 10000 });
  });

  test("shows views table", async ({ page }) => {
    await expect(page.locator("td code", { hasText: "monthly-revenue" })).toBeVisible();
    await expect(page.locator("td", { hasText: "Monthly revenue" })).toBeVisible();
    await expect(page.locator("td", { hasText: "sales" })).toBeVisible();
  });

  test("opens new view form", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await expect(page.locator(".view-editor")).toBeVisible();
    await expect(page.locator("input[placeholder='monthly-revenue']")).toBeVisible();
  });

  test("creates a new view", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await page.locator("input[placeholder='monthly-revenue']").fill("test-view");

    // Select domain
    await page.locator(".view-editor select").first().selectOption("sales");

    // Type SQL in CodeMirror
    await page.locator(".cm-content").click();
    await page.keyboard.type("SELECT 1 AS val");

    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.locator(".view-editor")).not.toBeVisible({ timeout: 5000 });
  });

  test("edits an existing view", async ({ page }) => {
    await page.getByRole("button", { name: "Edit" }).first().click();
    await expect(page.locator(".view-editor")).toBeVisible();
    // ID should be disabled for existing views
    await expect(page.locator("input[placeholder='monthly-revenue']")).toBeDisabled();
  });

  test("cancels view editor", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await expect(page.locator(".view-editor")).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".view-editor")).not.toBeVisible();
  });

  test("deletes a view", async ({ page }) => {
    await page.getByRole("button", { name: "Delete" }).first().click();
    // Delete triggers API call immediately (no dialog)
  });

  test("samples a view and shows sample data", async ({ page }) => {
    await page.getByRole("button", { name: "Sample" }).first().click();
    await expect(page.locator(".sample-panel")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".sample-panel th", { hasText: "month" })).toBeVisible();
    await expect(page.locator(".sample-panel th", { hasText: "revenue" })).toBeVisible();
    await expect(page.locator(".sample-panel td", { hasText: "50000" })).toBeVisible();
  });

  test("closes sample panel", async ({ page }) => {
    await page.getByRole("button", { name: "Sample" }).first().click();
    await expect(page.locator(".sample-panel")).toBeVisible({ timeout: 5000 });
    await page.getByRole("button", { name: "Close" }).click();
    await expect(page.locator(".sample-panel")).not.toBeVisible();
  });

  test("toggle materialize shows refresh interval", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    // Materialize checkbox
    await page.locator(".checkbox-label", { hasText: "Materialize" }).locator("input[type='checkbox']").check();
    await expect(page.locator("input[type='number']").last()).toBeVisible();
  });

  test("validation error when required fields missing", async ({ page }) => {
    await page.getByRole("button", { name: "New View" }).click();
    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.locator(".error", { hasText: "required" })).toBeVisible();
  });

  test("empty state shows message", async ({ page }) => {
    await setupMocks(page, { views: [] });
    await page.goto("/views");
    await expect(page.locator("td", { hasText: "No views defined" })).toBeVisible({ timeout: 10000 });
  });
});
