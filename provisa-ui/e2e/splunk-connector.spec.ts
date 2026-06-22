// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import https from "https";
import { test, expect } from "./coverage";

const SOURCE_ID = "e2e-splunk";
const ADMIN_GQL = "http://localhost:8000/admin/graphql";
// Splunk management port — mapped to host from Docker container
const SPLUNK_URL = "https://localhost:8089";
// Splunk runs inside Docker; Trino connects to it via the service name
const SPLUNK_HOST = "splunk";
const SPLUNK_PORT = 8089;
const SPLUNK_ADMIN_PASSWORD = "Admin1234!";

const INTERNAL_SERVER_COLUMNS = [
  { name: "time", visibleTo: [], writableBy: [] },
  { name: "host", visibleTo: [], writableBy: [] },
  { name: "source", visibleTo: [], writableBy: [] },
  { name: "sourcetype", visibleTo: [], writableBy: [] },
];

async function gql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(ADMIN_GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json();
}

async function getSplunkSessionKey(): Promise<string> {
  return new Promise((resolve, reject) => {
    const body = `username=admin&password=${encodeURIComponent(SPLUNK_ADMIN_PASSWORD)}&output_mode=json`;
    const req = https.request(
      `${SPLUNK_URL}/services/auth/login`,
      { method: "POST", rejectUnauthorized: false, headers: { "Content-Type": "application/x-www-form-urlencoded", "Content-Length": Buffer.byteLength(body) } },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            const parsed = JSON.parse(data) as { sessionKey?: string };
            if (!parsed.sessionKey) reject(new Error(`Splunk login failed: ${data}`));
            else resolve(parsed.sessionKey);
          } catch (e) {
            reject(e);
          }
        });
      },
    );
    req.on("error", reject);
    req.write(body);
    req.end();
  });
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

test("splunk connector: add source and query internal_server", async ({ page }) => {
  test.setTimeout(180000);

  // ── 0. Get a fresh Splunk session key ────────────────────────────────────
  const sessionKey = await getSplunkSessionKey();

  // ── 1. Add Splunk source via UI ───────────────────────────────────────────
  await page.goto("/sources");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: /\+ Source/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

  await page.locator(".form-card label").filter({ hasText: /^ID/ }).locator("input").fill(SOURCE_ID);
  await page.locator(".form-card label").filter({ hasText: /^Type/ }).locator("select").selectOption("splunk");
  await page.waitForSelector('label:has-text("Host")', { timeout: 5000 });

  await page.locator(".form-card label").filter({ hasText: /^Host/ }).locator("input").fill(SPLUNK_HOST);
  await page.locator(".form-card label").filter({ hasText: /^Port/ }).locator("input").fill(String(SPLUNK_PORT));
  await page.locator(".form-card label").filter({ hasText: /Auth Token/ }).locator("input").fill(sessionKey);

  // Check "Disable SSL Validation" — Splunk uses self-signed cert in Docker
  await page.locator(".form-card label").filter({ hasText: /Disable SSL/ }).locator('input[type="checkbox"]').check();

  await page.locator('.form-card button[type="submit"]').click();

  // Wait for source row to appear in list
  await expect(page.locator(".data-table td").filter({ hasText: SOURCE_ID })).toBeVisible({ timeout: 30000 });

  // ── 2. Open table form and verify Splunk schema and tables are enumerated ─
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: "+ Table" }).first().click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

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

  // Wait for 'splunk' schema to appear
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return Array.from(selects[2]?.options ?? []).some((o) => o.value === "splunk");
    },
    { timeout: 60000 },
  );

  await page.locator(".form-card label").filter({ hasText: /^Schema/ }).locator("select").selectOption("splunk");

  // Wait for internal_server table to appear
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return Array.from(selects[3]?.options ?? []).some((o) => o.value === "internal_server");
    },
    { timeout: 30000 },
  );

  const tableOptions = await page
    .locator(".form-card label")
    .filter({ hasText: /^Table/ })
    .locator("select option")
    .allTextContents();

  expect(tableOptions).toContain("internal_server");
  expect(tableOptions).toContain("internal_audit_logs");

  // ── 3. Register 'internal_server' via GQL ────────────────────────────────
  const domainId = await getDomainId();
  const registerResult = await gql(
    `mutation R($input: TableInput!) { registerTable(input: $input) { success message } }`,
    {
      input: {
        sourceId: SOURCE_ID,
        domainId,
        schemaName: "splunk",
        tableName: "internal_server",
        columns: INTERNAL_SERVER_COLUMNS,
      },
    },
  );

  expect(registerResult.errors, `registerTable errors: ${JSON.stringify(registerResult.errors)}`).toBeUndefined();
  expect(registerResult.data?.registerTable?.success).toBe(true);

  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await expect(page.locator(".data-table td").filter({ hasText: SOURCE_ID })).toBeVisible({ timeout: 15000 });

  // ── 4. Query internal_server via GQL data endpoint ───────────────────────
  const introRes = await fetch("http://localhost:8000/data/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ __schema { queryType { fields { name } } } }` }),
  });
  const introData = await introRes.json();
  const allFields: string[] = introData.data?.__schema?.queryType?.fields?.map((f: { name: string }) => f.name) ?? [];
  const tableField = allFields.find(
    (f) => f.toLowerCase().endsWith("internalserver") && !f.toLowerCase().includes("aggregate"),
  );
  expect(tableField, `internal_server field not found. Available: ${allFields.join(", ")}`).toBeDefined();

  const dataRes = await fetch("http://localhost:8000/data/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ ${tableField} { host source } }` }),
  });
  const queryResult = await dataRes.json();
  expect(queryResult.errors, `query errors: ${JSON.stringify(queryResult.errors)}`).toBeUndefined();
  const rows: Array<{ host: string; source: string }> = queryResult.data?.[tableField!] ?? [];
  // Large datasets are redirected to MinIO; row_count confirms data exists.
  const rowCount = rows.length > 0 ? rows.length : (queryResult.redirect?.row_count ?? 0);
  expect(rowCount).toBeGreaterThan(0);
});
