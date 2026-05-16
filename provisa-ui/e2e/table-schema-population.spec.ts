// Copyright (c) 2026 Kenneth Stott
// Canary: c7e1a2b3-d4f5-4e6a-9b8c-1d2e3f4a5b6c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks, MOCK_SOURCES, MOCK_SCHEMAS, MOCK_AVAILABLE_TABLES, MOCK_COLUMNS_META } from "./mocks";

test.describe("Table creation — RDBMS schema population", () => {
  test.beforeEach(async ({ page }) => {
    // tables: [] so no pre-registered tables filter out available ones
    await setupMocks(page, { tables: [] });
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
  });

  test("+ Table button opens registration form", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();
    await expect(page.locator("label", { hasText: "Source" })).toBeVisible();
    await expect(page.locator("label", { hasText: "Schema" })).toBeVisible();
    await expect(page.locator("label", { hasText: "Table" })).toBeVisible();
  });

  test("selecting a postgresql source triggers schema population", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    // Source dropdown should list mock sources
    const sourceSelect = page.locator("label", { hasText: "Source" }).locator("select");
    await sourceSelect.selectOption("sales-pg");

    // Schema dropdown should populate with values from mock
    const schemaSelect = page.locator("label", { hasText: "Schema" }).locator("select");
    await expect(schemaSelect.locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
  });

  test("selecting schema populates table dropdown", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });

    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");

    const tableSelect = page.locator("label", { hasText: "Table" }).locator("select");
    await expect(tableSelect.locator("option", { hasText: "orders" })).toBeAttached({ timeout: 5000 });
    await expect(tableSelect.locator("option", { hasText: "customers" })).toBeAttached();
  });

  test("selecting a table auto-fills description from comment", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");

    await expect(page.locator("label", { hasText: "Table" }).locator("select").locator("option", { hasText: "customers" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Table" }).locator("select").selectOption("customers");

    const descInput = page.locator("input[placeholder*='SDL docs'], input[placeholder*='sdl docs']").first();
    await expect(descInput).toHaveValue("Registered customer accounts", { timeout: 3000 });
  });

  test("table with no comment leaves description empty", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");

    await expect(page.locator("label", { hasText: "Table" }).locator("select").locator("option", { hasText: "products" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Table" }).locator("select").selectOption("products");

    const descInput = page.locator("input[placeholder*='SDL docs'], input[placeholder*='sdl docs']").first();
    await expect(descInput).toHaveValue("", { timeout: 3000 });
  });

  test("column rows are populated after table selection", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");

    await expect(page.locator("label", { hasText: "Table" }).locator("select").locator("option", { hasText: "orders" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Table" }).locator("select").selectOption("orders");

    // Column rows appear in .column-editor-row as .col-name spans
    await expect(page.locator(".col-name", { hasText: "id" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".col-name", { hasText: "name" })).toBeVisible();
    await expect(page.locator(".col-name", { hasText: "created_at" })).toBeVisible();
  });

  test("changing source resets schema and table dropdowns", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");
    await expect(page.locator("label", { hasText: "Table" }).locator("select").locator("option", { hasText: "orders" })).toBeAttached({ timeout: 5000 });

    // Change source back to blank
    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("");

    // Schema and table should reset
    const schemaSelect = page.locator("label", { hasText: "Schema" }).locator("select");
    await expect(schemaSelect).toHaveValue("", { timeout: 3000 });
  });

  test("Cancel button hides the registration form", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();
    await expect(page.locator("label", { hasText: "Source" })).toBeVisible();

    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.locator("label", { hasText: "Source" })).not.toBeVisible({ timeout: 3000 });
  });

  test("source → schema → table → columns cascade all populate", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();

    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("sales-pg");
    await expect(page.locator("label", { hasText: "Schema" }).locator("select").locator("option", { hasText: "public" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Schema" }).locator("select").selectOption("public");

    await expect(page.locator("label", { hasText: "Table" }).locator("select").locator("option", { hasText: "orders" })).toBeAttached({ timeout: 5000 });
    await page.locator("label", { hasText: "Table" }).locator("select").selectOption("orders");

    // All three mock columns should appear in the column editor
    await expect(page.locator(".col-name", { hasText: "id" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".col-name", { hasText: "name" })).toBeVisible();
    await expect(page.locator(".col-name", { hasText: "created_at" })).toBeVisible();
  });
});

test.describe("Table creation — fixed-schema sources", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page, {
      sources: [
        { id: "sales-pg", type: "postgresql", host: "localhost", port: 5432, database: "sales", username: "admin", dialect: "postgresql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
        { id: "my-gql", type: "graphql", host: "api.example.com", port: 443, database: "", username: "", dialect: "graphql", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
        { id: "my-kafka", type: "kafka", host: "kafka:9092", port: 9092, database: "", username: "", dialect: "kafka", cacheEnabled: false, cacheTtl: null, allowedDomains: [], namingConvention: null, path: null, description: "" },
      ],
    });
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({ timeout: 10000 });
  });

  test("graphql source auto-selects 'default' schema without API call", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();
    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("my-gql");

    const schemaSelect = page.locator("label", { hasText: "Schema" }).locator("select");
    await expect(schemaSelect).toHaveValue("default", { timeout: 3000 });
    // Schema select should be disabled for fixed-schema sources
    await expect(schemaSelect).toBeDisabled();
  });

  test("kafka source auto-selects 'default' schema without API call", async ({ page }) => {
    await page.getByRole("button", { name: "+ Table" }).click();
    await page.locator("label", { hasText: "Source" }).locator("select").selectOption("my-kafka");

    const schemaSelect = page.locator("label", { hasText: "Schema" }).locator("select");
    await expect(schemaSelect).toHaveValue("default", { timeout: 3000 });
    await expect(schemaSelect).toBeDisabled();
  });
});
