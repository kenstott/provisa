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

  test("govdata form shows AWS credential fields", async ({ page }) => {
    await expect(page.locator("label", { hasText: "AWS Access Key ID" }).locator("input")).toBeVisible();
    await expect(page.locator("label", { hasText: "AWS Secret Access Key" }).locator("input[type='password']")).toBeVisible();
  });

  test("AWS credential fields accept input", async ({ page }) => {
    await page.locator("label", { hasText: "AWS Access Key ID" }).locator("input").fill("AKIAIOSFODNN7EXAMPLE");
    await page.locator("label", { hasText: "AWS Secret Access Key" }).locator("input[type='password']").fill("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    await expect(page.locator("label", { hasText: "AWS Access Key ID" }).locator("input")).toHaveValue("AKIAIOSFODNN7EXAMPLE");
  });

  test("govdata create submits username/password from AWS fields", async ({ page }) => {
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
    await page.locator("label", { hasText: "AWS Access Key ID" }).locator("input").fill("AKIAIOSFODNN7EXAMPLE");
    await page.locator("label", { hasText: "AWS Secret Access Key" }).locator("input[type='password']").fill("secretkey123");

    await page.getByRole("button", { name: "Create" }).click();
    await expect(page.locator(".form-card")).not.toBeVisible({ timeout: 5000 });

    expect(captured.length).toBeGreaterThan(0);
    expect(captured[0]).toMatchObject({
      id: "fec-source",
      type: "govdata",
      username: "AKIAIOSFODNN7EXAMPLE",
      password: "secretkey123",
    });
  });

  test("govdata edit restores AWS Access Key ID from saved username", async ({ page }) => {
    await setupMocks(page, {
      sources: [
        { id: "fec-source", type: "govdata", host: "", port: 0, database: "fec,ref,geo", username: "AKIAIOSFODNN7EXAMPLE", dialect: "govdata", cacheEnabled: true, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
      ],
    });
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("tr", { hasText: "fec-source" }).first().click();
    await page.locator("button[title='Edit']").first().click();
    await expect(page.locator("label", { hasText: "AWS Access Key ID" }).locator("input")).toHaveValue("AKIAIOSFODNN7EXAMPLE");
  });
});

test.describe("GovData table registration and query (live backend)", () => {
  const accessKey = process.env.AWS_ACCESS_KEY_ID ?? "";
  const secretKey = process.env.AWS_SECRET_ACCESS_KEY ?? "";

  test.beforeEach(async () => {
    test.skip(!accessKey || !secretKey, "AWS credentials not set in environment");
  });

  test("creates fec-source and registers fec.candidates", async ({ page }) => {
    test.setTimeout(300000);
    // Create the govdata source
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
    await page.locator("button", { hasText: "+ Source" }).click();
    await page.locator(".form-card select").first().selectOption("govdata");
    await page.locator("input[placeholder='e.g. sales-pg']").fill("fec-source");
    await page.locator(".form-card label").filter({ hasText: /^Government$/ }).locator("input[type='checkbox']").check();
    await page.locator("label", { hasText: "AWS Access Key ID" }).locator("input").fill(accessKey);
    await page.locator("label", { hasText: "AWS Secret Access Key" }).locator("input[type='password']").fill(secretKey);
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
    await expect(tableSelect).not.toBeDisabled({ timeout: 30000 });
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
    await page.locator("label", { hasText: "AWS Access Key ID" }).locator("input").fill(accessKey);
    await page.locator("label", { hasText: "AWS Secret Access Key" }).locator("input[type='password']").fill(secretKey);
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
