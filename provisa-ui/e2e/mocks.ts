import type { Page } from "@playwright/test";

// ── Mock Data ──

export const MOCK_SOURCES = [
  { id: "sales-pg", type: "postgresql", host: "localhost", port: 5432, database: "sales", username: "admin", dialect: "postgresql", cacheEnabled: true, cacheTtl: 600 },
  { id: "analytics-sf", type: "snowflake", host: "org.snowflakecomputing.com", port: 443, database: "ANALYTICS", username: "svc", dialect: "snowflake", cacheEnabled: true, cacheTtl: null },
];

export const MOCK_DOMAINS = [
  { id: "sales", description: "Sales data" },
  { id: "analytics", description: "Analytics domain" },
];

export const MOCK_TABLES = [
  {
    id: 1, sourceId: "sales-pg", domainId: "sales", schemaName: "public", tableName: "orders",
    governance: "open", alias: null, description: "Customer orders", cacheTtl: 60,
    columns: [
      { id: 1, columnName: "id", visibleTo: [], writableBy: [], unmaskedTo: [], maskType: null, maskPattern: null, maskReplace: null, maskValue: null, maskPrecision: null, alias: null, description: "Primary key" },
      { id: 2, columnName: "customer_id", visibleTo: [], writableBy: [], unmaskedTo: [], maskType: null, maskPattern: null, maskReplace: null, maskValue: null, maskPrecision: null, alias: null, description: "FK to customers" },
      { id: 3, columnName: "total", visibleTo: ["admin"], writableBy: ["admin"], unmaskedTo: ["admin"], maskType: "constant", maskPattern: null, maskReplace: null, maskValue: "0", maskPrecision: null, alias: "order_total", description: "Order total" },
    ],
  },
  {
    id: 2, sourceId: "sales-pg", domainId: "sales", schemaName: "public", tableName: "customers",
    governance: "restricted", alias: "clients", description: null, cacheTtl: null,
    columns: [
      { id: 4, columnName: "id", visibleTo: [], writableBy: [], unmaskedTo: [], maskType: null, maskPattern: null, maskReplace: null, maskValue: null, maskPrecision: null, alias: null, description: null },
      { id: 5, columnName: "name", visibleTo: [], writableBy: [], unmaskedTo: [], maskType: null, maskPattern: null, maskReplace: null, maskValue: null, maskPrecision: null, alias: null, description: null },
    ],
  },
];

export const MOCK_RELATIONSHIPS = [
  {
    id: 1, sourceTableId: 1, targetTableId: 2,
    sourceTableName: "orders", targetTableName: "customers",
    sourceColumn: "customer_id", targetColumn: "id",
    cardinality: "many-to-one", materialize: false, refreshInterval: 300,
  },
];

export const MOCK_ROLES = [
  { id: "admin", capabilities: ["admin"], domainAccess: ["*"] },
  { id: "analyst", capabilities: ["query_development", "full_results"], domainAccess: ["sales"] },
];

export const MOCK_RLS_RULES = [
  { id: 1, tableId: 1, roleId: "analyst", filterExpr: "region = 'US'" },
];

export const MOCK_VIEWS = [
  {
    id: "monthly-revenue", sql: "SELECT date_trunc('month', created_at) AS month, SUM(total) AS revenue FROM orders GROUP BY 1",
    description: "Monthly revenue", domain_id: "sales", governance: "pre-approved",
    materialize: true, refresh_interval: 600, columns: [{ name: "month", visible_to: [] }, { name: "revenue", visible_to: [] }],
  },
];

export const MOCK_SETTINGS = {
  redirect: { enabled: true, threshold: 10000, default_format: "parquet", ttl: 3600 },
  sampling: { default_sample_size: 100 },
  cache: { default_ttl: 300 },
  naming: { domain_prefix: true },
};

export const MOCK_CANDIDATES = [
  {
    id: 1, source_table_id: 1, target_table_id: 2,
    source_column: "customer_id", target_column: "id",
    cardinality: "many-to-one", confidence: 0.95,
    reasoning: "FK naming convention matches",
  },
];

export const MOCK_PENDING_QUERIES = [
  { id: 1, queryText: "query GetOrders { orders { id total } }", developerId: "dev@co.com", status: "pending" },
  { id: 2, queryText: "query GetCustomers { customers { id name } }", developerId: "dev@co.com", status: "pending" },
];

export const MOCK_SCHEMAS = ["public", "analytics"];
export const MOCK_AVAILABLE_TABLES = [
  { name: "orders", comment: "Customer purchase orders" },
  { name: "customers", comment: "Registered customer accounts" },
  { name: "products", comment: null },
];
export const MOCK_COLUMNS_META = [
  { name: "id", dataType: "integer", comment: "Primary key" },
  { name: "name", dataType: "varchar", comment: null },
  { name: "created_at", dataType: "timestamp", comment: "Creation date" },
];

// ── Route Setup ──

export async function setupMocks(page: Page, overrides?: Partial<{
  sources: unknown[];
  domains: unknown[];
  tables: unknown[];
  relationships: unknown[];
  roles: unknown[];
  rlsRules: unknown[];
  views: unknown[];
  settings: unknown;
  candidates: unknown[];
  pendingQueries: unknown[];
  schemas: string[];
  availableTables: unknown[];
  columnsMeta: unknown[];
}>) {
  const sources = overrides?.sources ?? MOCK_SOURCES;
  const domains = overrides?.domains ?? MOCK_DOMAINS;
  const tables = overrides?.tables ?? MOCK_TABLES;
  const relationships = overrides?.relationships ?? MOCK_RELATIONSHIPS;
  const roles = overrides?.roles ?? MOCK_ROLES;
  const rlsRules = overrides?.rlsRules ?? MOCK_RLS_RULES;
  const views = overrides?.views ?? MOCK_VIEWS;
  const settings = overrides?.settings ?? MOCK_SETTINGS;
  const candidates = overrides?.candidates ?? MOCK_CANDIDATES;
  const pendingQueries = overrides?.pendingQueries ?? MOCK_PENDING_QUERIES;
  const schemas = overrides?.schemas ?? MOCK_SCHEMAS;
  const availableTables = overrides?.availableTables ?? MOCK_AVAILABLE_TABLES;
  const columnsMeta = overrides?.columnsMeta ?? MOCK_COLUMNS_META;

  // GraphQL endpoint
  await page.route("**/admin/graphql", async (route) => {
    const req = route.request();
    const body = JSON.parse(req.postData() || "{}");
    const query: string = body.query || "";

    let data: Record<string, unknown> = {};

    // Queries
    if (query.includes("sources")) data.sources = sources;
    if (query.includes("domains")) data.domains = domains;
    if (query.includes("tables") && !query.includes("availableTable")) data.tables = tables;
    if (query.includes("relationships")) data.relationships = relationships;
    if (query.includes("roles")) data.roles = roles;
    if (query.includes("rlsRules")) data.rlsRules = rlsRules;
    if (query.includes("persistedQueries")) data.persistedQueries = pendingQueries;
    if (query.includes("availableSchemas")) data.availableSchemas = schemas;
    if (query.includes("availableTables")) data.availableTables = availableTables;
    if (query.includes("availableColumns")) data.availableColumnsMetadata = columnsMeta;
    if (query.includes("mvList")) data.mvList = [
      { id: "mv-orders-customers", sourceTables: ["orders", "customers"], targetTable: "mv_orders_customers", refreshInterval: 300, enabled: true, status: "fresh", lastRefreshAt: Date.now() / 1000, rowCount: 1500, lastError: null },
    ];
    if (query.includes("cacheStats")) data.cacheStats = { totalKeys: 42, hitCount: 350, missCount: 50, storeType: "redis" };
    if (query.includes("systemHealth")) data.systemHealth = { trinoConnected: true, pgPoolSize: 5, pgPoolFree: 3, cacheConnected: true, flightServerRunning: false, mvRefreshLoopRunning: true };

    // Mutations
    if (query.includes("mutation")) {
      if (query.includes("createSource")) data.createSource = { success: true, message: "Created" };
      if (query.includes("deleteSource")) data.deleteSource = { success: true, message: "Deleted" };
      if (query.includes("registerTable")) data.registerTable = { success: true, message: "Registered" };
      if (query.includes("updateTable")) data.updateTable = { success: true, message: "Updated" };
      if (query.includes("deleteTable")) data.deleteTable = { success: true, message: "Deleted" };
      if (query.includes("createRole")) data.createRole = { success: true, message: "Created" };
      if (query.includes("deleteRole")) data.deleteRole = { success: true, message: "Deleted" };
      if (query.includes("upsertRlsRule")) data.upsertRlsRule = { success: true, message: "Created" };
      if (query.includes("deleteRlsRule")) data.deleteRlsRule = { success: true, message: "Deleted" };
      if (query.includes("upsertRelationship")) data.upsertRelationship = { success: true, message: "Saved" };
      if (query.includes("deleteRelationship")) data.deleteRelationship = { success: true, message: "Deleted" };
      if (query.includes("approveQuery")) data.approveQuery = { success: true };
      if (query.includes("rejectQuery")) data.rejectQuery = { success: true };
      if (query.includes("refreshMv")) data.refreshMv = { success: true, message: "MV refreshed" };
      if (query.includes("toggleMv")) data.toggleMv = { success: true, message: "MV toggled" };
      if (query.includes("purgeCache")) data.purgeCache = { success: true, message: "Purged 42 entries" };
      if (query.includes("purgeCacheByTable")) data.purgeCacheByTable = { success: true, message: "Purged 5 entries" };
    }

    await route.fulfill({ json: { data } });
  });

  // REST endpoints
  await page.route("**/admin/discover/relationships", async (route) => {
    await route.fulfill({ json: { candidates_found: 1, stored_ids: [1] } });
  });

  await page.route("**/admin/discover/candidates", async (route) => {
    await route.fulfill({ json: candidates });
  });

  await page.route("**/admin/discover/candidates/*/accept", async (route) => {
    await route.fulfill({ json: { success: true } });
  });

  await page.route("**/admin/discover/candidates/*/reject", async (route) => {
    await route.fulfill({ json: { success: true } });
  });

  await page.route("**/admin/config", async (route) => {
    if (route.request().method() === "PUT") {
      await route.fulfill({ json: { success: true, message: "Config uploaded" } });
    } else {
      await route.fulfill({ body: "# Provisa Config\nsources:\n  - id: sales-pg\n", contentType: "application/x-yaml" });
    }
  });

  await page.route("**/admin/settings", async (route) => {
    if (route.request().method() === "PUT") {
      await route.fulfill({ json: { success: true, updated: ["redirect", "naming"] } });
    } else {
      await route.fulfill({ json: settings });
    }
  });

  await page.route("**/admin/views", async (route) => {
    if (route.request().method() === "POST") {
      await route.fulfill({ json: { success: true, message: "View saved" } });
    } else {
      await route.fulfill({ json: views });
    }
  });

  await page.route("**/admin/views/*/sample", async (route) => {
    await route.fulfill({
      json: { columns: ["month", "revenue"], rows: [{ month: "2026-01", revenue: 50000 }, { month: "2026-02", revenue: 62000 }], count: 2 },
    });
  });

  await page.route("**/admin/views/*", async (route) => {
    if (route.request().method() === "DELETE") {
      await route.fulfill({ json: { success: true, message: "View deleted" } });
    } else {
      await route.continue();
    }
  });

  // Data endpoints
  await page.route("**/data/compile", async (route) => {
    await route.fulfill({
      json: {
        sql: "SELECT id, total FROM orders WHERE region = 'US'",
        trino_sql: null, direct_sql: "SELECT id, total FROM orders WHERE region = 'US'",
        params: [], route: "direct", route_reason: "single source",
        sources: ["sales-pg"], root_field: "orders",
      },
    });
  });

  await page.route("**/data/submit", async (route) => {
    await route.fulfill({ json: { query_id: 42, operation_name: "GetOrders", message: "Submitted for approval" } });
  });

  await page.route("**/data/sdl", async (route) => {
    await route.fulfill({ body: "type Query { orders: [Order] }\ntype Order { id: Int total: Float }", contentType: "text/plain" });
  });

  await page.route("**/health", async (route) => {
    await route.fulfill({ json: { status: "ok" } });
  });
}
