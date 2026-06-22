// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const SOURCE_ID = "e2e-northwind";
const SCHEMA_NAME = "e2e_northwind";
const GQL = "http://localhost:8000/admin/graphql";
const GLOB = "/data/files/northwind/**";

// LINQ4J maps camelCase CSV headers to snake_case column names
const CUSTOMERS_COLUMNS = [
  { name: "customer_id", visibleTo: [], writableBy: [] },
  { name: "company_name", visibleTo: [], writableBy: [] },
  { name: "contact_name", visibleTo: [], writableBy: [] },
  { name: "contact_title", visibleTo: [], writableBy: [] },
  { name: "address", visibleTo: [], writableBy: [] },
  { name: "city", visibleTo: [], writableBy: [] },
  { name: "region", visibleTo: [], writableBy: [] },
  { name: "postal_code", visibleTo: [], writableBy: [] },
  { name: "country", visibleTo: [], writableBy: [] },
  { name: "phone", visibleTo: [], writableBy: [] },
  { name: "fax", visibleTo: [], writableBy: [] },
];

async function gql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json();
}

async function getDomainId(): Promise<string> {
  const res = await gql(`{ domains { id } }`);
  const domains: Array<{ id: string }> = res.data?.domains ?? [];
  const d = domains.find((d) => d.id !== "meta" && d.id !== "ops") ?? domains[0];
  return d?.id ?? "pet-store";
}

async function cleanupSource() {
  await gql(`mutation D($id: String!) { deleteSource(id: $id) { success } }`, {
    id: SOURCE_ID,
  });
}

test.beforeEach(async () => {
  await cleanupSource();
});

test.afterEach(async () => {
  await cleanupSource();
});

test("file connector: add northwind source and query customers", async ({ page }) => {
  test.setTimeout(120000);

  // ── 1. Add Files source via UI ────────────────────────────────────────────
  await page.goto("/sources");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: /\+ Source/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

  await page.locator(".form-card label").filter({ hasText: /^ID/ }).locator("input").fill(SOURCE_ID);
  await page.locator(".form-card label").filter({ hasText: /^Type/ }).locator("select").selectOption("files");
  await page.waitForSelector('label:has-text("Directory Glob")', { timeout: 5000 });

  await page.locator("label").filter({ hasText: /Directory Glob/ }).locator("input").fill(GLOB);

  await page.locator('.form-card button[type="submit"]').click();

  // Wait for source row to appear in list
  await expect(page.locator(".data-table td").filter({ hasText: SOURCE_ID })).toBeVisible({ timeout: 30000 });

  // ── 2. Open table form and verify Northwind tables are enumerated ─────────
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: "+ Table" }).first().click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

  // Select source
  await page.locator(".form-card label").filter({ hasText: /^Source/ }).locator("select").selectOption(SOURCE_ID);

  // Wait for domain options to load
  const domainSelect = page.locator(".form-card label").filter({ hasText: /^Domain/ }).locator("select");
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return selects[1]?.options.length > 1;
    },
    { timeout: 10000 },
  );
  const firstDomain = await domainSelect.locator("option").nth(1).getAttribute("value");
  await domainSelect.selectOption(firstDomain!);

  // Wait for northwind schema to appear
  await page.waitForFunction(
    (schema) => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return Array.from(selects[2]?.options ?? []).some((o) => o.value === schema);
    },
    SCHEMA_NAME,
    { timeout: 60000 },
  );

  await page.locator(".form-card label").filter({ hasText: /^Schema/ }).locator("select").selectOption(SCHEMA_NAME);

  // Wait for customers table to appear
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return Array.from(selects[3]?.options ?? []).some((o) => o.value === "customers");
    },
    { timeout: 30000 },
  );

  const tableOptions = await page
    .locator(".form-card label")
    .filter({ hasText: /^Table/ })
    .locator("select option")
    .allTextContents();

  expect(tableOptions).toContain("customers");
  expect(tableOptions).toContain("orders");
  expect(tableOptions).toContain("products");

  // ── 3. Register 'customers' table via GQL ────────────────────────────────
  const domainId = await getDomainId();
  const registerResult = await gql(
    `mutation R($input: TableInput!) { registerTable(input: $input) { success message } }`,
    {
      input: {
        sourceId: SOURCE_ID,
        domainId,
        schemaName: SCHEMA_NAME,
        tableName: "customers",
        columns: CUSTOMERS_COLUMNS,
      },
    },
  );

  expect(registerResult.errors, `registerTable errors: ${JSON.stringify(registerResult.errors)}`).toBeUndefined();
  expect(registerResult.data?.registerTable?.success).toBe(true);

  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await expect(page.locator(".data-table").getByText("customers")).toBeVisible({ timeout: 15000 });

  // ── 4. Query customers via GQL data endpoint ──────────────────────────────
  // Domain prefix (e.g. "pet-store" → "ps__") is prepended to the field name.
  const introRes = await fetch("http://localhost:8000/data/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ __schema { queryType { fields { name } } } }` }),
  });
  const introData = await introRes.json();
  const allFields: string[] = introData.data?.__schema?.queryType?.fields?.map((f: { name: string }) => f.name) ?? [];
  const customersField = allFields.find((f) => f.toLowerCase().endsWith("customers") && !f.toLowerCase().includes("aggregate"));
  expect(customersField, `customers field not found in schema. Available: ${allFields.join(", ")}`).toBeDefined();

  const dataRes = await fetch("http://localhost:8000/data/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ ${customersField} { customerId companyName } }` }),
  });
  const queryResult = await dataRes.json();
  expect(queryResult.errors, `query errors: ${JSON.stringify(queryResult.errors)}`).toBeUndefined();
  const rows: Array<{ customerId: string; companyName: string }> = queryResult.data?.[customersField!] ?? [];
  expect(rows.length).toBeGreaterThan(0);
  expect(rows[0].customerId).toBeDefined();
});
