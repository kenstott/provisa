// Copyright (c) 2026 Kenneth Stott
// Canary: e877c813-4fb5-489f-9b31-004888836dc8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("SecurityPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({ timeout: 10000 });
  });

  test("shows roles and RLS rules tables", async ({ page }) => {
    await expect(page.getByRole("cell", { name: "admin" }).first()).toBeVisible();
    await expect(page.getByRole("cell", { name: "analyst" }).first()).toBeVisible();
    await expect(page.getByRole("heading", { name: "RLS Rules" })).toBeVisible();
    await expect(page.locator("td code", { hasText: "region = 'US'" })).toBeVisible();
  });

  test("opens new role form and creates a role", async ({ page }) => {
    await page.getByRole("button", { name: "Add Role" }).click();
    await expect(page.locator(".form-card").first()).toBeVisible();

    await page.locator("input[placeholder='analyst']").fill("viewer");
    await page.locator(".checkbox-label", { hasText: "query_development" }).locator("input[type='checkbox']").check();
    await page.locator(".checkbox-label", { hasText: "sales" }).locator("input[type='checkbox']").check();

    await page.getByRole("button", { name: "Save" }).first().click();
    await expect(page.locator("input[placeholder='analyst']")).not.toBeVisible({ timeout: 5000 });
  });

  test("edits an existing role", async ({ page }) => {
    await page.getByRole("button", { name: "Edit" }).first().click();
    await expect(page.locator(".form-card").first()).toBeVisible();
    await expect(page.locator("input[placeholder='analyst']")).toBeDisabled();
    await page.getByRole("button", { name: "Save" }).first().click();
    await expect(page.locator("input[placeholder='analyst']")).not.toBeVisible({ timeout: 5000 });
  });

  test("deletes a role", async ({ page }) => {
    const roleTable = page.locator(".data-table").first();
    await roleTable.getByRole("button", { name: "Delete" }).first().click();
  });

  test("cancel hides role form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Role" }).click();
    await expect(page.locator(".form-card").first()).toBeVisible();
    // The Add Role button becomes Cancel when form is open
    await page.getByRole("button", { name: "Cancel" }).first().click();
    await expect(page.locator("input[placeholder='analyst']")).not.toBeVisible();
  });

  test("opens new RLS rule form and creates a rule", async ({ page }) => {
    await page.getByRole("button", { name: "Add RLS Rule" }).click();
    const ruleForm = page.locator(".form-card").last();
    await expect(ruleForm).toBeVisible();

    await ruleForm.locator("select").first().selectOption("orders");
    await ruleForm.locator("select").nth(1).selectOption("analyst");
    await ruleForm.locator("input[placeholder*='region']").fill("status = 'active'");

    await ruleForm.getByRole("button", { name: "Save" }).click();
    await expect(ruleForm.locator("input[placeholder*='region']")).not.toBeVisible({ timeout: 5000 });
  });

  test("edits an existing RLS rule", async ({ page }) => {
    const rlsTable = page.locator(".data-table").last();
    await rlsTable.getByRole("button", { name: "Edit" }).first().click();
    const ruleForm = page.locator(".form-card").last();
    await expect(ruleForm).toBeVisible();
    await ruleForm.getByRole("button", { name: "Save" }).click();
  });

  test("deletes an RLS rule", async ({ page }) => {
    const rlsTable = page.locator(".data-table").last();
    await rlsTable.getByRole("button", { name: "Delete" }).first().click();
  });

  test("cancel hides RLS rule form", async ({ page }) => {
    await page.getByRole("button", { name: "Add RLS Rule" }).click();
    const ruleForm = page.locator(".form-card").last();
    await expect(ruleForm).toBeVisible();
    // The Add RLS Rule button becomes Cancel when form is open
    await page.getByRole("button", { name: "Cancel" }).last().click();
    await expect(ruleForm).not.toBeVisible();
  });
});
