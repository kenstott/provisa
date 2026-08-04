// Copyright (c) 2026 Kenneth Stott
// Canary: e81bdcec-3a6a-4695-a85c-a9a6a0159d4b
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import https from "https";
import { spawnSync } from "child_process";
import { test, expect, UI_URL, TRINO_BACKEND_URL } from "./coverage";

const SOURCE_ID = "e2e-splunk";
// Trino backend: register_source() creates a real Trino catalog so the schema dropdown populates.
// DuckDB backend's register_source() is a no-op — the schema dropdown would stay empty.
const ADMIN_GQL = `${TRINO_BACKEND_URL}/admin/graphql`;

// Splunk management port — mapped to host from Docker container.
// Must match the -p flag in beforeAll's docker run command.
const SPLUNK_MGMT_PORT = Number(process.env.SPLUNK_MGMT_PORT ?? 8089);
const SPLUNK_URL = `https://localhost:${SPLUNK_MGMT_PORT}`;

// Splunk source config seen by Trino:
// The container is joined to provisa_default with alias "splunk" so Trino
// resolves it at splunk:8089 on the project network (container port unchanged).
const SPLUNK_HOST = "splunk";
const SPLUNK_PORT = 8089;

// Must match SPLUNK_PASSWORD set in the docker run command below.
const SPLUNK_ADMIN_PASSWORD = "Provisa_2026!";

const CONTAINER_NAME = "e2e-splunk-connector";

// The core compose project's default network — Trino runs here and must reach
// the splunk container by its alias for federation queries to work.
const DOCKER_NETWORK = process.env.PROVISA_E2E_DOCKER_NETWORK ?? "provisa_default";

// visibleTo must be non-empty — empty list means "visible to no role" in Provisa's governance model
const INTERNAL_SERVER_COLUMNS = [
  { name: "time", visibleTo: ["developer", "org_admin", "analyst"], writableBy: [] },
  { name: "host", visibleTo: ["developer", "org_admin", "analyst"], writableBy: [] },
  { name: "source", visibleTo: ["developer", "org_admin", "analyst"], writableBy: [] },
  { name: "sourcetype", visibleTo: ["developer", "org_admin", "analyst"], writableBy: [] },
];

async function gql(query: string, variables: Record<string, unknown> = {}) {
  // 120s: backend may be processing a Trino catalog init; fail explicitly rather than hanging
  // indefinitely and consuming the entire test timeout with no diagnostic error.
  const res = await fetch(ADMIN_GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(120000),
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
    // Per-request timeout: if Splunk TCP connects but hangs on HTTP, destroy after 5s
    // so waitForSplunk()'s poll loop can advance rather than blocking indefinitely.
    req.setTimeout(5000, () => req.destroy(new Error("getSplunkSessionKey: request timeout")));
    req.write(body);
    req.end();
  });
}

/** Poll Splunk management HTTPS endpoint until it responds (200 or 401 = server up). */
async function waitForSplunk(timeoutMs = 420_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await getSplunkSessionKey();
      return; // login success → Splunk is up and accepting auth
    } catch {
      // not ready — ECONNREFUSED, login not yet available, etc.
    }
    await new Promise((r) => setTimeout(r, 10_000));
  }
  throw new Error(`Splunk container did not become ready within ${timeoutMs}ms`);
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

test.beforeAll(async () => {
  // Splunk's cold start (image pull + first boot) dominates this spec: up to 420 s before the
  // management port accepts a login. Doing it here rather than in the test body keeps it off the
  // test clock, so a slow boot no longer expires the test wall on whichever assertion the timer
  // happened to reach — it fails the hook with waitForSplunk's own explicit message instead.
  test.setTimeout(600_000);
  // Remove any stale container from a prior run, then start fresh.
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
  const result = spawnSync(
    "docker",
    [
      "run", "-d",
      "--name", CONTAINER_NAME,
      // Publish management port to host so this test process can authenticate.
      "-p", `${SPLUNK_MGMT_PORT}:8089`,
      // Join the core project's network with alias "splunk" so Trino can reach
      // splunk:8089 for federation queries (the source's host field).
      "--network", DOCKER_NETWORK,
      "--network-alias", SPLUNK_HOST,
      "-e", "SPLUNK_START_ARGS=--accept-license",
      "-e", "SPLUNK_GENERAL_TERMS=--accept-sgt-current-at-splunk-com",
      "-e", `SPLUNK_PASSWORD=${SPLUNK_ADMIN_PASSWORD}`,
      "splunk/splunk:latest",
    ],
    { stdio: "pipe" },
  );
  if (result.status !== 0) {
    throw new Error(`Failed to start Splunk container: ${result.stderr.toString()}`);
  }
  await waitForSplunk();

  // Trino begins connecting to Splunk immediately after the container is up, causing transient
  // backend errors. Wait for the Trino backend to stabilise before any test navigates — poll
  // /health until stable or 60 s elapsed.
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${TRINO_BACKEND_URL}/health`);
      if (r.ok) break;
    } catch {
      // backend not yet reachable
    }
    await new Promise((res) => setTimeout(res, 3000));
  }
});

test.afterAll(() => {
  spawnSync("docker", ["rm", "-f", CONTAINER_NAME], { stdio: "pipe" });
});

test.beforeEach(async () => {
  await cleanupSource();
});

test.afterEach(async () => {
  await cleanupSource();
});

test("splunk connector: add source and query internal_server", async ({ page }) => {
  // beforeAll has already paid for the Splunk cold start and the backend stabilisation; what is
  // left on this clock is the UI walk plus the Trino catalog init behind the schema dropdown.
  test.setTimeout(420_000);

  // ── 0. Splunk is up (beforeAll waited); take a session key for the seed step ──
  const sessionKey = await getSplunkSessionKey();

  // Redirect the UI's API calls from the default DuckDB backend to the Trino backend so
  // that register_source() creates a real Trino catalog and schema enumeration succeeds.
  // The Apollo client uses relative URLs (/admin/graphql) so the browser sends requests
  // to the UI origin (http://localhost:3901); Vite proxies them server-side. Intercepting
  // at the UI origin redirects them to the Trino backend before Vite's proxy fires.
  for (const prefix of ["/admin", "/data", "/query", "/health"]) {
    await page.route(`${UI_URL}${prefix}*`, (route) => {
      route.continue({ url: route.request().url().replace(UI_URL, TRINO_BACKEND_URL) });
    });
  }

  // ── 1. Add Splunk source via UI ───────────────────────────────────────────
  await page.goto("/sources");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: /\+ Source/i }).click();
  await page.waitForSelector('[data-testid="sources-id-input"]', { timeout: 5000 });

  // Native inputs are inside <label> elements in the source form
  await page.locator('[data-testid="sources-id-input"]').fill(SOURCE_ID);
  await page.locator('[data-testid="sources-type-select"]').selectOption("splunk");

  // Mantine TextInput/NumberInput/PasswordInput/Checkbox: data-testid is forwarded to the
  // underlying <input> element directly — selectors target data-testid with no child combinator.
  await page.waitForSelector('[data-testid="splunk-host-input"]', { timeout: 10000 });
  await page.locator('[data-testid="splunk-host-input"]').fill(SPLUNK_HOST);
  await page.locator('[data-testid="splunk-port-input"]').fill(String(SPLUNK_PORT));
  await page.locator('[data-testid="splunk-auth-token-input"]').fill(sessionKey);

  // Check "Disable SSL Validation" — Splunk uses self-signed cert in Docker
  await page.locator('[data-testid="splunk-disable-ssl-checkbox"]').check();

  await page.locator('[data-testid="sources-submit"]').click();

  // Wait for source row to appear in list
  await expect(page.locator(".data-table td").filter({ hasText: SOURCE_ID })).toBeVisible({ timeout: 30000 });

  // ── 2. Open table form and verify Splunk schema and tables are enumerated ─
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });

  await page.getByRole("button", { name: "+ Table" }).first().click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

  // Wait for source to appear in dropdown — Apollo cache-and-network may serve stale
  // data initially; the network fetch updates the cache within a few seconds.
  await page.waitForFunction(
    (id: string) => {
      const sel = document.querySelector<HTMLSelectElement>('[data-testid="register-table-source-select"]');
      return Array.from(sel?.options ?? []).some((o) => o.value === id);
    },
    SOURCE_ID,
    { timeout: 15000 },
  );
  await page.locator('[data-testid="register-table-source-select"]').selectOption(SOURCE_ID);

  // Wait for Domain dropdown to have options
  await page.waitForFunction(
    () => {
      const sel = document.querySelector<HTMLSelectElement>('[data-testid="register-table-domain-select"]');
      return sel != null && sel.options.length > 1;
    },
    { timeout: 10000 },
  );
  const domainSelect = page.locator('[data-testid="register-table-domain-select"]');
  const firstDomain = await domainSelect.locator("option").nth(1).getAttribute("value");
  await domainSelect.selectOption(firstDomain!);

  // Wait for 'splunk' schema to appear — Trino Splunk catalog loads asynchronously after the source
  // is registered; allow 180s for Trino catalog initialisation + schema query. Splunk's cold start
  // is already paid for in beforeAll, so this budget covers only the catalog handshake.
  await page.waitForFunction(
    () => {
      const sel = document.querySelector<HTMLSelectElement>('[data-testid="register-table-schema-select"]');
      return Array.from(sel?.options ?? []).some((o) => o.value === "splunk");
    },
    { timeout: 180000 },
  );

  await page.locator('[data-testid="register-table-schema-select"]').selectOption("splunk");

  // Wait for internal_server table to appear
  await page.waitForFunction(
    () => {
      const sel = document.querySelector<HTMLSelectElement>('[data-testid="register-table-table-select"]');
      return Array.from(sel?.options ?? []).some((o) => o.value === "internal_server");
    },
    { timeout: 30000 },
  );

  const tableOptions = await page
    .locator('[data-testid="register-table-table-select"] option')
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
  // 60s: schema introspect should be fast; if backend hangs, fail with a clear error.
  const introRes = await fetch(`${TRINO_BACKEND_URL}/data/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ __schema { queryType { fields { name } } } }` }),
    signal: AbortSignal.timeout(60000),
  });
  const introData = await introRes.json();
  const allFields: string[] = introData.data?.__schema?.queryType?.fields?.map((f: { name: string }) => f.name) ?? [];
  const tableField = allFields.find(
    (f) => f.toLowerCase().endsWith("internalserver") && !f.toLowerCase().includes("aggregate"),
  );
  expect(tableField, `internal_server field not found. Available: ${allFields.join(", ")}`).toBeDefined();

  // 180s: Trino→Splunk JDBC query; Splunk data is indexed so allow extra time for the
  // connector to execute the search and stream results back through Trino.
  const dataRes = await fetch(`${TRINO_BACKEND_URL}/data/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ ${tableField} { host source } }` }),
    signal: AbortSignal.timeout(180000),
  });
  const queryResult = await dataRes.json();
  expect(queryResult.errors, `query errors: ${JSON.stringify(queryResult.errors)}`).toBeUndefined();
  const rows: Array<{ host: string; source: string }> = queryResult.data?.[tableField!] ?? [];
  // Large datasets are redirected to MinIO; row_count confirms data exists.
  const rowCount = rows.length > 0 ? rows.length : (queryResult.redirect?.row_count ?? 0);
  expect(rowCount).toBeGreaterThan(0);
});
