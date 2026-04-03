import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("TablesPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
  });

  test("shows registered tables", async ({ page }) => {
    await expect(page.locator("td", { hasText: "orders" }).first()).toBeVisible();
    await expect(page.locator("td", { hasText: "customers" }).first()).toBeVisible();
    await expect(page.locator("td", { hasText: "sales-pg" }).first()).toBeVisible();
  });

  test("opens register table form", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();
    await expect(page.locator(".form-card")).toBeVisible();
  });

  test("cascading dropdowns: source -> schema -> table -> columns", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    // Select source — triggers schema fetch
    await page.locator(".form-card select").first().selectOption("sales-pg");

    // Wait for schema dropdown to have options, then select
    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    // Wait for table dropdown, then select
    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });
    await tableSelect.selectOption("orders");

    // Columns should load
    await expect(page.locator(".column-editor-row")).toHaveCount(3, { timeout: 5000 });
    await expect(page.locator("span.col-name", { hasText: "id" })).toBeVisible();
  });

  test("registers a table", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    await page.locator(".form-card select").first().selectOption("sales-pg");
    await page.locator(".form-card select").nth(1).selectOption("sales");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });
    await tableSelect.selectOption("orders");

    await expect(page.locator(".column-editor-row")).toHaveCount(3, { timeout: 5000 });

    // Click the Register Table button inside the form (not the toggle)
    await page.locator(".form-card button", { hasText: "Register Table" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });
  });

  test("deletes a table", async ({ page }) => {
    page.on("dialog", (dialog) => dialog.accept());
    await page.getByRole("button", { name: "Delete" }).first().click();
  });

  test("expands table row to show column details", async ({ page }) => {
    await page.locator("tr.clickable").first().click();
    await expect(page.locator("code", { hasText: "customer_id" })).toBeVisible({ timeout: 3000 });
  });

  test("edits a table inline", async ({ page }) => {
    await page.locator("tr.clickable").first().click();
    await expect(page.locator("code", { hasText: "customer_id" })).toBeVisible({ timeout: 3000 });

    await page.getByRole("button", { name: "Edit" }).click();
    await expect(page.locator("input[placeholder='GraphQL name override']")).toBeVisible();
    await page.locator("input[placeholder='GraphQL name override']").fill("my_orders");

    await page.getByRole("button", { name: "Save" }).click();
    await expect(page.locator("input[placeholder='GraphQL name override']")).not.toBeVisible({ timeout: 5000 });
  });

  test("cancels inline edit", async ({ page }) => {
    await page.locator("tr.clickable").first().click();
    await expect(page.locator("code", { hasText: "customer_id" })).toBeVisible({ timeout: 3000 });
    await page.getByRole("button", { name: "Edit" }).click();
    await expect(page.locator("input[placeholder='GraphQL name override']")).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator("input[placeholder='GraphQL name override']")).not.toBeVisible();
  });

  test("cancel hides register form", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();
    await expect(page.locator(".form-card")).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible();
  });

  test("validation error when required fields missing", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();
    await page.locator(".form-card button", { hasText: "Register Table" }).click();
    await expect(page.locator(".error", { hasText: "required" })).toBeVisible();
  });
});
