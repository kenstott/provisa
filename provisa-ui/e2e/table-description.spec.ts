// Copyright (c) 2025 Kenneth Stott
// Canary: 2a15a02f-7791-4db8-a4cb-e5c19399f8db
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
 * Tests for auto-populating table descriptions from physical database comments.
 * Verifies GitHub issue #7.
 */
test.describe("Table description auto-populate", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
  });

  test("prefills description from table comment when table is selected", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    // Select source
    await page.locator(".form-card select").first().selectOption("sales-pg");

    // Select schema
    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    // Select table with a comment
    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });
    await tableSelect.selectOption("orders");

    // Verify description was auto-populated from the mock comment
    const descInput = page.locator("input[placeholder='Appears in SDL docs']");
    await expect(descInput).toHaveValue("Customer purchase orders", { timeout: 5000 });
  });

  test("leaves description empty for tables without comments", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    await page.locator(".form-card select").first().selectOption("sales-pg");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });
    await tableSelect.selectOption("products");

    // Products has no comment — description should be empty
    const descInput = page.locator("input[placeholder='Appears in SDL docs']");
    await expect(descInput).toHaveValue("");
  });

  test("updates description when switching tables", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    await page.locator(".form-card select").first().selectOption("sales-pg");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });

    // Select orders
    await tableSelect.selectOption("orders");
    const descInput = page.locator("input[placeholder='Appears in SDL docs']");
    await expect(descInput).toHaveValue("Customer purchase orders", { timeout: 5000 });

    // Switch to customers
    await tableSelect.selectOption("customers");
    await expect(descInput).toHaveValue("Registered customer accounts", { timeout: 5000 });
  });

  test("clears description when schema changes", async ({ page }) => {
    await page.getByRole("button", { name: "Register Table" }).click();

    await page.locator(".form-card select").first().selectOption("sales-pg");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 5000 });
    await schemaSelect.selectOption("public");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 5000 });
    await tableSelect.selectOption("orders");

    const descInput = page.locator("input[placeholder='Appears in SDL docs']");
    await expect(descInput).toHaveValue("Customer purchase orders", { timeout: 5000 });

    // Change schema — description should clear
    await schemaSelect.selectOption("");
    await expect(descInput).toHaveValue("");
  });
});
