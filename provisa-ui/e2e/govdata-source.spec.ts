// Copyright (c) 2026 Kenneth Stott
// Canary: a1b2c3d4-e5f6-7890-a1b2-c3d4e5f6a7b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("GovData source entry", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("button", { hasText: "+ Source" }).click();
    await page.locator(".form-card select").first().selectOption("govdata");
  });

  test("govdata form shows Data Subjects checkboxes", async ({ page }) => {
    await expect(page.locator(".form-card", { hasText: "Data Subjects" })).toBeVisible();
    await expect(page.locator(".form-card label").filter({ hasText: /^Government$/ })).toBeVisible();
    await expect(page.locator(".form-card label").filter({ hasText: /^Commerce$/ })).toBeVisible();
  });

  test("govdata form shows AskAmerica API Key field and Get API Key button", async ({ page }) => {
    await expect(page.locator("label", { hasText: "AskAmerica API Key" }).locator("input")).toBeVisible();
    await expect(page.getByRole("button", { name: "Get API Key →" })).toBeVisible();
  });

  test("AskAmerica API Key field accepts input", async ({ page }) => {
    await page.locator("label", { hasText: "AskAmerica API Key" }).locator("input").fill("aa_test_key_12345");
    await expect(page.locator("label", { hasText: "AskAmerica API Key" }).locator("input")).toHaveValue("aa_test_key_12345");
  });

  test("govdata create submits api key in username field", async ({ page }) => {
    const captured: Record<string, unknown>[] = [];
    await page.route("**/admin/graphql", async (route) => {
      const body = JSON.parse(route.request().postData() || "{}");
      if ((body.query || "").includes("createSource")) {
        captured.push(body.variables?.input ?? {});
        await route.fulfill({ json: { data: { createSource: { success: true, message: "ok" } } } });
      } else {
        await route.continue();
      }
    });

    await page.locator("input[placeholder='e.g. sales-pg']").fill("fec-source");
    await page.locator(".form-card label").filter({ hasText: /^Government$/ }).locator("input[type='checkbox']").check();
    await page.locator("label", { hasText: "AskAmerica API Key" }).locator("input").fill("aa_test_key_12345");

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });

    expect(captured.length).toBeGreaterThan(0);
    expect(captured[0]).toMatchObject({
      id: "fec-source",
      type: "govdata",
      username: "aa_test_key_12345",
    });
  });

  test("govdata edit restores AskAmerica API Key from saved username", async ({ page }) => {
    await setupMocks(page, {
      sources: [
        { id: "fec-source", type: "govdata", host: "", port: 0, database: "fec,ref,geo", username: "aa_saved_key_99999", dialect: "govdata", cacheEnabled: true, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
      ],
    });
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("tr", { hasText: "fec-source" }).first().click();
    await page.locator("button[title='Edit']").first().click();
    await expect(page.locator("label", { hasText: "AskAmerica API Key" }).locator("input")).toHaveValue("aa_saved_key_99999");
  });
});

test.describe("GovData table registration and query (live backend)", () => {
  const apiKey = process.env.ASKAMERICA_API_KEY ?? "";

  test.beforeEach(async () => {
    test.skip(!apiKey, "ASKAMERICA_API_KEY not set in environment");
  });

  test("creates fec-source and registers fec.candidates", async ({ page }) => {
    test.setTimeout(300000);
    // Delete fec.candidates if already registered so the dropdown shows it
    const tablesResp = await page.request.post("http://localhost:8000/admin/graphql", {
      data: { query: "{ tables { id sourceId schemaName tableName } }" },
    });
    const tablesJson = await tablesResp.json();
    const existing = (tablesJson.data?.tables ?? []).find(
      (t: { sourceId: string; schemaName: string; tableName: string }) =>
        t.sourceId === "fec-source" && t.schemaName === "fec" && t.tableName === "candidates"
    );
    if (existing) {
      await page.request.post("http://localhost:8000/admin/graphql", {
        data: { query: `mutation { deleteTable(id: ${existing.id}) { success } }` },
      });
    }
    // Create the govdata source
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("button", { hasText: "+ Source" }).click();
    await page.locator(".form-card select").first().selectOption("govdata");
    await page.locator("input[placeholder='e.g. sales-pg']").fill("fec-source");
    await page.locator(".form-card label").filter({ hasText: /^Government$/ }).locator("input[type='checkbox']").check();
    await page.locator("label", { hasText: "AskAmerica API Key" }).locator("input").fill(apiKey);
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 60000 });

    // Register fec.candidates
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
    await page.getByRole("button", { name: "+ Table" }).click();
    await page.locator(".form-card select").first().selectOption("fec-source");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 30000 });
    await schemaSelect.selectOption("fec");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 90000 });
    await tableSelect.selectOption("candidates");

    await expect(page.locator(".column-editor-row").first()).toBeVisible({ timeout: 30000 });
    await page.locator(".form-card button", { hasText: "+ Table" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 60000 });
  });

  test("SELECT * FROM fec.candidates returns rows", async ({ page }) => {
    test.setTimeout(300000);
    await page.goto("/sql");
    await expect(page.locator(".cm-editor")).toBeVisible({ timeout: 10000 });

    await page.locator(".cm-editor").click();
    await page.keyboard.press("Control+a");
    await page.keyboard.type("SELECT * FROM fec.candidates");

    await page.getByRole("button", { name: /Sample/ }).click();

    await expect(page.locator("table.sql-results-table")).toBeVisible({ timeout: 120000 });
    await expect(page.locator("table.sql-results-table th").first()).toBeVisible();
    await expect(page.locator("table.sql-results-table tbody tr").first()).toBeVisible();
  });

  test("creates econ-source and econ schema returns tables", async ({ page }) => {
    test.setTimeout(300000);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("button", { hasText: "+ Source" }).click();
    await page.locator(".form-card select").first().selectOption("govdata");
    await page.locator("input[placeholder='e.g. sales-pg']").fill("econ-source");
    await page.locator(".form-card label").filter({ hasText: /^Economy$/ }).locator("input[type='checkbox']").check();
    await page.locator("label", { hasText: "AskAmerica API Key" }).locator("input").fill(apiKey);
    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 60000 });

    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
    await page.getByRole("button", { name: "+ Table" }).click();
    await page.locator(".form-card select").first().selectOption("econ-source");

    const schemaSelect = page.locator(".form-card select").nth(2);
    await expect(schemaSelect).not.toBeDisabled({ timeout: 30000 });
    await schemaSelect.selectOption("econ");

    const tableSelect = page.locator(".form-card select").nth(3);
    await expect(tableSelect).not.toBeDisabled({ timeout: 60000 });
    const tableOptions = await tableSelect.locator("option").count();
    expect(tableOptions).toBeGreaterThan(1);
  });
});
