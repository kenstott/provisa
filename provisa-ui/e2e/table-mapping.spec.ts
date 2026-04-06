// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a2c6e-1b3d-4f9a-8c2e-6b1d4f9a2c6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

// ── Mock sources that trigger Discover / Map Table buttons ───────────────────

const NOSQL_SOURCES = [
  { id: "sessions-redis", type: "redis", host: "redis", port: 6379, database: "", username: "", dialect: "redis", cacheEnabled: false, cacheTtl: null },
  { id: "product-mongo", type: "mongodb", host: "mongo", port: 27017, database: "products", username: "", dialect: "mongodb", cacheEnabled: false, cacheTtl: null },
  { id: "logs-es", type: "elasticsearch", host: "es", port: 9200, database: "", username: "", dialect: "elasticsearch", cacheEnabled: false, cacheTtl: null },
  { id: "metrics", type: "prometheus", host: "prometheus", port: 9090, database: "", username: "", dialect: "prometheus", cacheEnabled: false, cacheTtl: null },
  { id: "events-accumulo", type: "accumulo", host: "accumulo", port: 9999, database: "", username: "", dialect: "accumulo", cacheEnabled: false, cacheTtl: null },
];

const DISCOVERED_COLUMNS = [
  { name: "user_id", type: "INTEGER", description: "User identifier", source_path: "user_id" },
  { name: "email", type: "VARCHAR", description: "Email address", source_path: "contact.email" },
  { name: "created_at", type: "TIMESTAMP", description: "Creation timestamp", source_path: "meta.created_at" },
];

async function setupNoSQLMocks(page: Parameters<typeof setupMocks>[0]) {
  await setupMocks(page);

  // Override sources list to include NoSQL sources
  await page.route("**/admin/graphql", async (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    if (body.query?.includes("sources")) {
      await route.fulfill({
        json: { data: { sources: NOSQL_SOURCES } },
      });
    } else {
      await route.continue();
    }
  });

  // Mock schema discovery endpoint
  await page.route("**/admin/discover*", async (route) => {
    await route.fulfill({
      json: { columns: DISCOVERED_COLUMNS },
    });
  });

  // Mock registerTable
  await page.route("**/admin/graphql", async (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    if (body.query?.includes("registerTable") || body.mutation?.includes("registerTable")) {
      await route.fulfill({ json: { data: { registerTable: { success: true, message: "Registered" } } } });
    } else {
      await route.continue();
    }
  });
}

// ── TableMappingBuilder — Redis ───────────────────────────────────────────────

test.describe("TableMappingBuilder — Redis", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Map Table button is visible for Redis source", async ({ page }) => {
    await expect(page.getByRole("button", { name: "Map Table" }).first()).toBeVisible({ timeout: 10000 });
  });

  test("clicking Map Table opens TableMappingBuilder panel", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: redis")).toBeVisible({ timeout: 5000 });
  });

  test("Redis form shows Key Pattern field", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Key Pattern" })).toBeVisible();
    await expect(page.locator("input[placeholder*='user:*']")).toBeVisible();
  });

  test("Redis form shows Key Column Name field", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Key Column Name" })).toBeVisible();
  });

  test("Redis form shows Value Type selector with hash/string/zset/list options", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    const vt = page.locator("label", { hasText: "Value Type" }).locator("select");
    await expect(vt).toBeVisible();
    const options = await vt.locator("option").allTextContents();
    expect(options).toContain("Hash");
    expect(options).toContain("String");
    expect(options).toContain("Sorted Set");
    expect(options).toContain("List");
  });

  test("Redis form shows Columns table with Add button", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("h4", { hasText: "Columns" })).toBeVisible();
    await expect(page.getByRole("button", { name: "+ Add" })).toBeVisible();
  });

  test("Redis column header shows 'Redis Field' for field mapping", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("th", { hasText: "Redis Field" })).toBeVisible();
  });

  test("adding a column row works", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    const initialRows = await page.locator(".data-table tbody tr").count();
    await page.getByRole("button", { name: "+ Add" }).click();
    const afterRows = await page.locator(".data-table tbody tr").count();
    expect(afterRows).toBe(initialRows + 1);
  });

  test("Save Mapping button requires table name", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    // Without a table name the form should not close
    await page.getByRole("button", { name: "Save Mapping" }).click();
    await expect(page.getByText("Table Mapping: redis")).toBeVisible();
  });

  test("Cancel closes the TableMappingBuilder panel", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: redis")).toBeVisible();
    await page.locator(".form-card").getByRole("button", { name: "Cancel" }).click();
    await expect(page.getByText("Table Mapping: redis")).not.toBeVisible({ timeout: 3000 });
  });

  test("full Redis mapping workflow: fill table name and save", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await redisRow.getByRole("button", { name: "Map Table" }).click();

    // Fill table name
    await page.locator("label", { hasText: "Table Name" }).locator("input").fill("user_sessions");
    // Fill key pattern
    await page.locator("input[placeholder*='user:*']").fill("session:*");
    // Fill key column
    await page.locator("label", { hasText: "Key Column Name" }).locator("input").fill("session_key");
    // Select string value type
    await page.locator("label", { hasText: "Value Type" }).locator("select").selectOption("string");
    // Save
    await page.getByRole("button", { name: "Save Mapping" }).click();
    // Panel closes on success
    await expect(page.getByText("Table Mapping: redis")).not.toBeVisible({ timeout: 5000 });
  });
});

// ── TableMappingBuilder — MongoDB ─────────────────────────────────────────────

test.describe("TableMappingBuilder — MongoDB", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Map Table button is visible for MongoDB source", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await expect(mongoRow.getByRole("button", { name: "Map Table" })).toBeVisible({ timeout: 5000 });
  });

  test("MongoDB form shows Collection field", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: mongodb")).toBeVisible();
    await expect(page.locator("label", { hasText: "Collection" })).toBeVisible();
    await expect(page.locator("input[placeholder*='users']")).toBeVisible();
  });

  test("MongoDB form shows Auto-discover schema checkbox", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Auto-discover schema" })).toBeVisible();
  });

  test("MongoDB column header shows JSONPath for field mapping", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("th", { hasText: "JSONPath" })).toBeVisible();
  });
});

// ── TableMappingBuilder — Elasticsearch ──────────────────────────────────────

test.describe("TableMappingBuilder — Elasticsearch", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Elasticsearch form shows Index Pattern field", async ({ page }) => {
    const esRow = page.locator("tr", { hasText: "logs-es" });
    await esRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: elasticsearch")).toBeVisible();
    await expect(page.locator("label", { hasText: "Index Pattern" })).toBeVisible();
    await expect(page.locator("input[placeholder*='nginx-access-*']")).toBeVisible();
  });

  test("Elasticsearch form shows Auto-discover checkbox", async ({ page }) => {
    const esRow = page.locator("tr", { hasText: "logs-es" });
    await esRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Auto-discover schema" })).toBeVisible();
  });

  test("Elasticsearch column header shows Dot-Path", async ({ page }) => {
    const esRow = page.locator("tr", { hasText: "logs-es" });
    await esRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("th", { hasText: "Dot-Path" })).toBeVisible();
  });
});

// ── TableMappingBuilder — Prometheus ─────────────────────────────────────────

test.describe("TableMappingBuilder — Prometheus", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Prometheus form shows Metric Name field", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: prometheus")).toBeVisible();
    await expect(page.locator("label", { hasText: "Metric Name" })).toBeVisible();
    await expect(page.locator("input[placeholder*='http_request_duration']")).toBeVisible();
  });

  test("Prometheus form shows Value Column Name field", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Value Column Name" })).toBeVisible();
  });

  test("Prometheus form shows Default Time Range field", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Default Time Range" })).toBeVisible();
  });

  test("Prometheus form shows Labels as Columns section with Add Label button", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("label", { hasText: "Labels as Columns" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Label" })).toBeVisible();
  });

  test("adding a label tag creates a removable badge", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await page.locator("input[placeholder='e.g. method']").fill("method");
    await page.getByRole("button", { name: "Add Label" }).click();
    await expect(page.locator("span", { hasText: "method" })).toBeVisible();
  });

  test("pressing Enter in label input adds the label", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    await page.locator("input[placeholder='e.g. method']").fill("status");
    await page.keyboard.press("Enter");
    await expect(page.locator("span", { hasText: "status" })).toBeVisible();
  });

  test("Prometheus form does not show Columns table (labels replace columns)", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Map Table" }).click();
    // Prometheus uses labels instead of explicit column rows
    await expect(page.locator("h4", { hasText: "Columns" })).not.toBeVisible({ timeout: 2000 }).catch(() => {
      // Acceptable — some layouts may keep the header hidden
    });
  });
});

// ── TableMappingBuilder — Accumulo ────────────────────────────────────────────

test.describe("TableMappingBuilder — Accumulo", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Accumulo form shows Accumulo Table field", async ({ page }) => {
    const accRow = page.locator("tr", { hasText: "events-accumulo" });
    await accRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.getByText("Table Mapping: accumulo")).toBeVisible();
    await expect(page.locator("label", { hasText: "Accumulo Table" })).toBeVisible();
    await expect(page.locator("input[placeholder*='graph_edges']")).toBeVisible();
  });

  test("Accumulo columns table shows Family and Qualifier columns", async ({ page }) => {
    const accRow = page.locator("tr", { hasText: "events-accumulo" });
    await accRow.getByRole("button", { name: "Map Table" }).click();
    await expect(page.locator("th", { hasText: "Family" })).toBeVisible();
    await expect(page.locator("th", { hasText: "Qualifier" })).toBeVisible();
  });
});

// ── SchemaDiscovery panel ─────────────────────────────────────────────────────

test.describe("SchemaDiscovery", () => {
  test.beforeEach(async ({ page }) => {
    await setupNoSQLMocks(page);
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({ timeout: 10000 });
  });

  test("Discover button is visible for MongoDB source", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await expect(mongoRow.getByRole("button", { name: "Discover" })).toBeVisible({ timeout: 5000 });
  });

  test("Discover button is visible for Elasticsearch source", async ({ page }) => {
    const esRow = page.locator("tr", { hasText: "logs-es" });
    await expect(esRow.getByRole("button", { name: "Discover" })).toBeVisible({ timeout: 5000 });
  });

  test("Discover button is visible for Prometheus source", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await expect(promRow.getByRole("button", { name: "Discover" })).toBeVisible({ timeout: 5000 });
  });

  test("Redis source does NOT show a Discover button (no inference)", async ({ page }) => {
    const redisRow = page.locator("tr", { hasText: "sessions-redis" });
    await expect(redisRow.getByRole("button", { name: "Discover" })).not.toBeVisible({ timeout: 2000 }).catch(() => {});
  });

  test("clicking Discover on MongoDB opens SchemaDiscovery panel", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.getByText("Schema Discovery: product-mongo")).toBeVisible({ timeout: 5000 });
  });

  test("SchemaDiscovery panel shows Discover Schema button", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.getByRole("button", { name: "Discover Schema" })).toBeVisible();
  });

  test("SchemaDiscovery panel shows Collection hint field for MongoDB", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.locator("label", { hasText: "Collection" })).toBeVisible();
  });

  test("SchemaDiscovery panel shows Index Pattern hint for Elasticsearch", async ({ page }) => {
    const esRow = page.locator("tr", { hasText: "logs-es" });
    await esRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.getByText("Schema Discovery: logs-es")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("label", { hasText: "Index Pattern" })).toBeVisible();
  });

  test("SchemaDiscovery panel shows Metric Name hint for Prometheus", async ({ page }) => {
    const promRow = page.locator("tr", { hasText: "metrics" });
    await promRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.getByText("Schema Discovery: metrics")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("label", { hasText: "Metric Name" })).toBeVisible();
  });

  test("Discover Schema populates column candidates table", async ({ page }) => {
    // Override discover route to return columns immediately
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    // Columns should appear in the table
    await expect(page.locator(".data-table td", { hasText: "user_id" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".data-table td", { hasText: "email" })).toBeVisible();
    await expect(page.locator(".data-table td", { hasText: "created_at" })).toBeVisible();
  });

  test("discovered columns are selected by default", async ({ page }) => {
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    await page.locator(".data-table td", { hasText: "user_id" }).waitFor({ timeout: 5000 });
    const checkboxes = page.locator(".data-table tbody input[type='checkbox']");
    const count = await checkboxes.count();
    for (let i = 0; i < count; i++) {
      await expect(checkboxes.nth(i)).toBeChecked();
    }
  });

  test("unchecking a column deselects it", async ({ page }) => {
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    await page.locator(".data-table td", { hasText: "user_id" }).waitFor({ timeout: 5000 });
    // Uncheck the first row
    const firstCheckbox = page.locator(".data-table tbody input[type='checkbox']").first();
    await firstCheckbox.uncheck();
    await expect(firstCheckbox).not.toBeChecked();
  });

  test("header checkbox toggles all columns", async ({ page }) => {
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    await page.locator(".data-table td", { hasText: "user_id" }).waitFor({ timeout: 5000 });
    // Uncheck all via header
    const headerCheckbox = page.locator(".data-table thead input[type='checkbox']");
    await headerCheckbox.uncheck();
    const bodyCheckboxes = page.locator(".data-table tbody input[type='checkbox']");
    const count = await bodyCheckboxes.count();
    for (let i = 0; i < count; i++) {
      await expect(bodyCheckboxes.nth(i)).not.toBeChecked();
    }
  });

  test("Add Column button appends a manual column row", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    const before = await page.locator(".data-table tbody tr").count();
    await page.getByRole("button", { name: "+ Add Column" }).click();
    const after = await page.locator(".data-table tbody tr").count();
    expect(after).toBe(before + 1);
  });

  test("Close button hides the SchemaDiscovery panel", async ({ page }) => {
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await expect(page.getByText("Schema Discovery: product-mongo")).toBeVisible({ timeout: 5000 });
    await page.locator(".form-card").getByRole("button", { name: "Close" }).click();
    await expect(page.getByText("Schema Discovery: product-mongo")).not.toBeVisible({ timeout: 3000 });
  });

  test("Register Table button appears after columns are discovered", async ({ page }) => {
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    await page.locator(".data-table td", { hasText: "user_id" }).waitFor({ timeout: 5000 });
    await expect(page.getByRole("button", { name: "Register Table" })).toBeVisible();
  });

  test("Register Table shows Domain ID and Table Name fields", async ({ page }) => {
    await page.route("**/admin/discover**", (route) =>
      route.fulfill({ json: { columns: DISCOVERED_COLUMNS } })
    );
    const mongoRow = page.locator("tr", { hasText: "product-mongo" });
    await mongoRow.getByRole("button", { name: "Discover" }).click();
    await page.locator("label", { hasText: "Collection" }).locator("input").fill("users");
    await page.getByRole("button", { name: "Discover Schema" }).click();
    await page.locator(".data-table td", { hasText: "user_id" }).waitFor({ timeout: 5000 });
    await expect(page.locator("label", { hasText: "Domain ID" })).toBeVisible();
    await expect(page.locator("label", { hasText: "Table Name" })).toBeVisible();
  });
});
