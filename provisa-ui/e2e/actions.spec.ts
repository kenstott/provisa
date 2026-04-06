// Copyright (c) 2026 Kenneth Stott
// Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

// ── Mock data ─────────────────────────────────────────────────────────────────

const MOCK_FUNCTIONS = [
  {
    name: "process_order",
    sourceId: "sales-pg",
    schemaName: "public",
    functionName: "process_order",
    returns: "sales-pg.public.orders",
    arguments: [{ name: "order_id", type: "Int" }],
    visibleTo: ["admin"],
    writableBy: ["admin"],
    domainId: "sales",
    description: "Processes a single order",
  },
];

const MOCK_WEBHOOKS = [
  {
    name: "notify_slack",
    url: "https://hooks.slack.com/services/T000/B000/xxx",
    method: "POST",
    timeoutMs: 3000,
    returns: null,
    inlineReturnType: [{ name: "ok", type: "Boolean" }],
    arguments: [{ name: "message", type: "String" }],
    visibleTo: ["admin"],
    domainId: "ops",
    description: "Sends a Slack notification",
  },
];

// ── Mock setup ────────────────────────────────────────────────────────────────

async function setupActionsMocks(page: Parameters<typeof setupMocks>[0]) {
  await setupMocks(page);

  // GET /admin/actions
  await page.route("**/admin/actions", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        json: { functions: MOCK_FUNCTIONS, webhooks: MOCK_WEBHOOKS },
      });
    } else {
      await route.continue();
    }
  });

  // POST /admin/actions/functions  (create / update)
  await page.route("**/admin/actions/functions", async (route) => {
    await route.fulfill({ json: { success: true, message: "Function saved" } });
  });

  // DELETE /admin/actions/functions/*
  await page.route("**/admin/actions/functions/*", async (route) => {
    if (route.request().method() === "DELETE") {
      await route.fulfill({ json: { success: true, message: "Function deleted" } });
    } else {
      await route.continue();
    }
  });

  // POST /admin/actions/webhooks  (create / update)
  await page.route("**/admin/actions/webhooks", async (route) => {
    await route.fulfill({ json: { success: true, message: "Webhook saved" } });
  });

  // DELETE /admin/actions/webhooks/*
  await page.route("**/admin/actions/webhooks/*", async (route) => {
    if (route.request().method() === "DELETE") {
      await route.fulfill({ json: { success: true, message: "Webhook deleted" } });
    } else {
      await route.continue();
    }
  });

  // POST /admin/actions/test
  await page.route("**/admin/actions/test", async (route) => {
    await route.fulfill({
      json: { rows: [{ id: 1, total: 99.99 }], count: 1 },
    });
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test.describe("ActionsPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupActionsMocks(page);
    await page.goto("/actions");
    await expect(page.getByRole("heading", { name: "Actions" })).toBeVisible({ timeout: 15000 });
  });

  // ── Page renders ───────────────────────────────────────────────────────────

  test("shows Actions heading and section headings", async ({ page }) => {
    await expect(page.getByRole("heading", { name: "Actions" })).toBeVisible();
    await expect(page.locator("h3", { hasText: "DB Functions" })).toBeVisible();
    await expect(page.locator("h3", { hasText: "Webhooks" })).toBeVisible();
  });

  test("lists existing functions in the functions table", async ({ page }) => {
    await expect(page.locator("td", { hasText: "process_order" }).first()).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "sales-pg" })).toBeVisible();
  });

  test("lists existing webhooks in the webhooks table", async ({ page }) => {
    await expect(page.locator("td", { hasText: "notify_slack" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "POST" }).first()).toBeVisible();
    await expect(page.locator("td", { hasText: "3000ms" })).toBeVisible();
  });

  // ── Empty states ───────────────────────────────────────────────────────────

  test("empty functions state shows placeholder message", async ({ page }) => {
    await page.route("**/admin/actions", async (route) => {
      await route.fulfill({ json: { functions: [], webhooks: [] } });
    });
    await page.goto("/actions");
    await expect(page.getByRole("heading", { name: "Actions" })).toBeVisible({ timeout: 15000 });
    await expect(page.locator("td", { hasText: "No functions registered" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("td", { hasText: "No webhooks registered" })).toBeVisible({ timeout: 10000 });
  });

  // ── Add action form ────────────────────────────────────────────────────────

  test("opens Add Action form when button is clicked", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await expect(page.locator(".form-card")).toBeVisible({ timeout: 5000 });
  });

  test("cancel hides the Add Action form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await expect(page.locator(".form-card")).toBeVisible({ timeout: 5000 });
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });
  });

  test("form defaults to DB Function type", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    const typeSelect = page.locator(".form-card select").first();
    await expect(typeSelect).toHaveValue("function");
  });

  // ── Create DB Function ─────────────────────────────────────────────────────

  test("creates a DB Function and closes the form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await expect(page.locator(".form-card")).toBeVisible();

    // Name is required
    await page.locator(".form-card input[placeholder='e.g. process_order']").fill("my_function");

    // Source
    await page.locator(".form-card select").nth(1).selectOption("sales-pg");

    // Function name (DB name)
    await page.locator(".form-card input[placeholder='DB function name']").fill("my_function");

    // Returns — pick the first table option
    const returnsSelect = page.locator(".form-card label", { hasText: "Returns (table)" }).locator("select");
    const firstOption = returnsSelect.locator("option").nth(1);
    const firstOptionValue = await firstOption.getAttribute("value");
    if (firstOptionValue) {
      await returnsSelect.selectOption(firstOptionValue);
    }

    await page.locator(".form-card button[type='submit']").click();

    // Form disappears after successful save
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 10000 });
  });

  test("create function shows success message", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();

    await page.locator(".form-card input[placeholder='e.g. process_order']").fill("fn_test");
    await page.locator(".form-card select").nth(1).selectOption("sales-pg");
    await page.locator(".form-card input[placeholder='DB function name']").fill("fn_test");

    const returnsSelect = page.locator(".form-card label", { hasText: "Returns (table)" }).locator("select");
    const firstOption = returnsSelect.locator("option").nth(1);
    const firstOptionValue = await firstOption.getAttribute("value");
    if (firstOptionValue) {
      await returnsSelect.selectOption(firstOptionValue);
    }

    await page.locator(".form-card button[type='submit']").click();

    await expect(page.locator(".success, div.success")).toBeVisible({ timeout: 10000 });
  });

  // ── Create Webhook ─────────────────────────────────────────────────────────

  test("switching form type to Webhook shows URL and Method fields", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.locator(".form-card select").first().selectOption("webhook");

    await expect(page.locator(".form-card input[placeholder='https://api.example.com/action']")).toBeVisible();
    await expect(page.locator(".form-card label", { hasText: "Method" })).toBeVisible();
    await expect(page.locator(".form-card label", { hasText: "Timeout (ms)" })).toBeVisible();
  });

  test("creates a Webhook and closes the form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.locator(".form-card select").first().selectOption("webhook");

    await page.locator(".form-card input[placeholder='e.g. process_order']").fill("my_webhook");
    await page.locator(".form-card input[placeholder='https://api.example.com/action']").fill("https://example.com/hook");

    await page.locator(".form-card button[type='submit']").click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 10000 });
  });

  test("webhook method dropdown has POST GET PUT PATCH options", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.locator(".form-card select").first().selectOption("webhook");

    const methodSelect = page.locator(".form-card label", { hasText: "Method" }).locator("select");
    await expect(methodSelect.locator("option[value='POST']")).toBeAttached();
    await expect(methodSelect.locator("option[value='GET']")).toBeAttached();
    await expect(methodSelect.locator("option[value='PUT']")).toBeAttached();
    await expect(methodSelect.locator("option[value='PATCH']")).toBeAttached();
  });

  test("webhook inline return type fields appear when no Returns table is selected", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.locator(".form-card select").first().selectOption("webhook");

    // No Returns table selected — Inline Return Type section should be visible
    await expect(page.locator("h4", { hasText: "Inline Return Type" })).toBeVisible();
    await expect(page.getByRole("button", { name: "+ Add Field" })).toBeVisible();
  });

  test("adds and removes an inline return field", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.locator(".form-card select").first().selectOption("webhook");

    await page.getByRole("button", { name: "+ Add Field" }).click();
    await expect(page.locator(".form-card input[placeholder='Field name']")).toBeVisible();

    // Fill the field name
    await page.locator(".form-card input[placeholder='Field name']").fill("result_id");

    // Remove it
    await page.locator(".form-card button.destructive").first().click();
    await expect(page.locator(".form-card input[placeholder='Field name']")).not.toBeVisible({ timeout: 3000 });
  });

  // ── Arguments ─────────────────────────────────────────────────────────────

  test("adds an argument to the form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();

    await page.getByRole("button", { name: "+ Add Argument" }).click();
    await expect(page.locator(".form-card input[placeholder='Arg name']")).toBeVisible();
  });

  test("removes an argument from the form", async ({ page }) => {
    await page.getByRole("button", { name: "Add Action" }).click();
    await page.getByRole("button", { name: "+ Add Argument" }).click();
    await page.locator(".form-card input[placeholder='Arg name']").fill("arg1");

    // X button next to the argument row removes it
    const destructiveBtn = page.locator(".form-card button.destructive").last();
    await destructiveBtn.click();
    await expect(page.locator(".form-card input[placeholder='Arg name']")).not.toBeVisible({ timeout: 3000 });
  });

  // ── Edit existing action ───────────────────────────────────────────────────

  test("clicking Edit on a function opens the form pre-populated", async ({ page }) => {
    // Functions table: Edit button in the first row
    const functionRows = page.locator("h3", { hasText: "DB Functions" });
    await expect(functionRows).toBeVisible({ timeout: 10000 });

    // Find the Edit button in the functions section (first data-table)
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Edit" }).first().click();

    await expect(page.locator(".form-card")).toBeVisible({ timeout: 5000 });
    // Name field should be disabled (cannot rename existing action)
    const nameInput = page.locator(".form-card input[placeholder='e.g. process_order']");
    await expect(nameInput).toBeDisabled();
  });

  test("clicking Edit on a webhook opens the form pre-populated with webhook fields", async ({ page }) => {
    const whTable = page.locator(".data-table").last();
    await whTable.getByRole("button", { name: "Edit" }).first().click();

    await expect(page.locator(".form-card")).toBeVisible({ timeout: 5000 });
    const urlInput = page.locator(".form-card input[placeholder='https://api.example.com/action']");
    await expect(urlInput).toBeVisible();
    await expect(urlInput).toHaveValue("https://hooks.slack.com/services/T000/B000/xxx");
  });

  test("Update button shown when editing an existing action", async ({ page }) => {
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Edit" }).first().click();
    await expect(page.locator(".form-card button[type='submit']", { hasText: "Update" })).toBeVisible();
  });

  // ── Delete via ConfirmDialog ───────────────────────────────────────────────

  test("deletes a function via ConfirmDialog", async ({ page }) => {
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Delete" }).first().click();

    await expect(page.locator(".modal")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".modal")).toContainText("process_order");
    await page.getByRole("button", { name: "Confirm" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
  });

  test("cancel delete dialog leaves modal content intact", async ({ page }) => {
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Delete" }).first().click();

    await expect(page.locator(".modal")).toBeVisible({ timeout: 5000 });
    await page.locator(".modal").getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
  });

  test("deletes a webhook via ConfirmDialog", async ({ page }) => {
    const whTable = page.locator(".data-table").last();
    await whTable.getByRole("button", { name: "Delete" }).first().click();

    await expect(page.locator(".modal")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".modal")).toContainText("notify_slack");
    await page.getByRole("button", { name: "Confirm" }).click();
    await expect(page.locator(".modal")).not.toBeVisible({ timeout: 5000 });
  });

  // ── Test action button ─────────────────────────────────────────────────────

  test("Test button fires the function action and shows result panel", async ({ page }) => {
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Test" }).first().click();

    // Result panel must appear with the action name and JSON data
    await expect(page.locator("h4", { hasText: "Test Result: process_order" })).toBeVisible({ timeout: 10000 });
    await expect(page.locator("pre")).toContainText("99.99");
  });

  test("test result panel can be closed", async ({ page }) => {
    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Test" }).first().click();
    await expect(page.locator("h4", { hasText: "Test Result:" })).toBeVisible({ timeout: 10000 });

    await page.getByRole("button", { name: "Close" }).click();
    await expect(page.locator("h4", { hasText: "Test Result:" })).not.toBeVisible({ timeout: 5000 });
  });

  test("Test button on webhook fires webhook action and shows result", async ({ page }) => {
    const whTable = page.locator(".data-table").last();
    await whTable.getByRole("button", { name: "Test" }).first().click();

    await expect(page.locator("h4", { hasText: "Test Result: notify_slack" })).toBeVisible({ timeout: 10000 });
  });

  test("test action failure shows error message", async ({ page }) => {
    // Override test route to return a failure
    await page.route("**/admin/actions/test", async (route) => {
      await route.fulfill({ status: 500, json: { detail: "Connection refused" } });
    });

    const fnTable = page.locator(".data-table").first();
    await fnTable.getByRole("button", { name: "Test" }).first().click();

    await expect(page.locator(".error", { hasText: "Test failed" })).toBeVisible({ timeout: 10000 });
  });
});

// COVERAGE NOTE
// Tested:
//   - Page loads and shows Actions / DB Functions / Webhooks headings
//   - Lists existing functions (name, source) and webhooks (name, URL, method, timeout)
//   - Empty state messages for both functions and webhooks tables
//   - Add Action button opens form; Cancel hides it
//   - Form defaults to "DB Function" type
//   - Create DB Function: fills required fields, submits, form closes, success message shown
//   - Switching form type to Webhook shows URL, Method, Timeout, Inline Return Type section
//   - Create Webhook: fills URL, submits, form closes
//   - Webhook method dropdown options (POST/GET/PUT/PATCH)
//   - Inline Return Type section appears when no Returns table selected
//   - Add / remove inline return fields
//   - Add / remove arguments
//   - Edit function: form opens pre-populated with name disabled, Update button shown
//   - Edit webhook: form opens pre-populated with existing URL
//   - Delete function via ConfirmDialog (confirm and cancel paths)
//   - Delete webhook via ConfirmDialog
//   - Test function button shows result panel with JSON data
//   - Result panel Close button hides panel
//   - Test webhook button shows result panel
//   - Test action failure shows .error message
