// Copyright (c) 2025 Kenneth Stott
// Canary: f4cfb737-ac5c-4865-9f61-6948d3d000bc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("RelationshipsPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships", exact: true })).toBeVisible({ timeout: 10000 });
  });

  test("shows relationships table", async ({ page }) => {
    // Use the first data-table (relationships, not candidates)
    const relTable = page.locator(".data-table").first();
    await expect(relTable.getByRole("cell", { name: "orders" })).toBeVisible();
    await expect(relTable.getByRole("cell", { name: "customers" })).toBeVisible();
    await expect(relTable.getByRole("cell", { name: "many-to-one" })).toBeVisible();
  });

  test("opens and fills add relationship form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    await expect(page.locator(".form-card")).toBeVisible();
    await page.locator("input[placeholder='orders-to-customers']").fill("test-rel");
    // Source and Target table selects are within the form
    const formSelects = page.locator(".form-card select");
    await formSelects.nth(0).selectOption("orders");
    await formSelects.nth(1).selectOption("customers");
    await page.locator("input[placeholder='customer_id']").fill("customer_id");
    await page.locator("input[placeholder='id']").fill("id");
  });

  test("creates a relationship", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    await page.locator("input[placeholder='orders-to-customers']").fill("new-rel");
    const formSelects = page.locator(".form-card select");
    await formSelects.nth(0).selectOption("orders");
    await formSelects.nth(1).selectOption("customers");
    await page.locator("input[placeholder='customer_id']").fill("fk_id");
    await page.locator("input[placeholder='id']").fill("id");
    await page.locator(".form-card").getByRole("button", { name: "Save" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });
  });

  test("cancel hides form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    await expect(page.locator(".form-card")).toBeVisible();
    // The Add Relationship button toggles to Cancel
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible();
  });

  test("deletes a relationship", async ({ page }) => {
    const relTable = page.locator(".data-table").first();
    await relTable.getByRole("button", { name: "Delete" }).first().click();
  });

  test("toggles materialize checkbox on existing relationship", async ({ page }) => {
    const relTable = page.locator(".data-table").first();
    const matCheckbox = relTable.locator("input[type='checkbox']").first();
    await matCheckbox.click();
  });

  test("materialize form shows refresh interval", async ({ page }) => {
    await page.getByRole("button", { name: "Add Relationship" }).click();
    const matLabel = page.locator(".checkbox-label", { hasText: "Materialize" });
    await matLabel.locator("input[type='checkbox']").check();
    await expect(page.locator(".form-card input[type='number']")).toBeVisible();
  });

  test("discover relationships (Suggest with AI)", async ({ page }) => {
    await page.getByRole("button", { name: "Suggest with AI" }).click();
    await expect(page.locator("h3", { hasText: "AI-Suggested" })).toBeVisible({ timeout: 5000 });
  });

  test("shows AI-suggested candidates", async ({ page }) => {
    await expect(page.locator("h3", { hasText: "AI-Suggested" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "95%" })).toBeVisible();
    await expect(page.locator("td.reasoning-cell", { hasText: "FK naming convention" })).toBeVisible();
  });

  test("accepts a candidate", async ({ page }) => {
    const candidateTable = page.locator(".data-table").last();
    await candidateTable.getByRole("button", { name: "Accept" }).first().click();
  });

  test("rejects a candidate", async ({ page }) => {
    const candidateTable = page.locator(".data-table").last();
    await candidateTable.getByRole("button", { name: "Reject" }).first().click();
    await expect(page.getByRole("cell", { name: "95%" })).not.toBeVisible({ timeout: 3000 });
  });
});
