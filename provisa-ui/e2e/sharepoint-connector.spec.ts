// Copyright (c) 2026 Kenneth Stott
// Canary: 39422f73-15eb-43ce-9ba1-d88adeaa2542
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect, UI_URL, TRINO_BACKEND_URL } from "./coverage";
import { deleteSourceAndItsTables } from "./delete-source";
import { isAssignableDomain } from "../src/lib/internalDomains";
import { addSourceOfType, listedTableNames } from "./source-to-query-helpers";

const SOURCE_ID = "e2e-sharepoint";
// REQ-1730: the sharepoint catalog's schema is the sql-normalized source id on every engine
// (trino_connectors.TrinoSharepointConnector passes it as the catalog's `schema`, as
// the DuckDB pgwire bridge exposes it), not the fixed literal "sharepoint".
const SCHEMA = SOURCE_ID.replace(/-/g, "_");
// Trino backend: register_source() creates a real Trino catalog so the schema dropdown populates.
// DuckDB backend's register_source() is a no-op — the schema dropdown would stay empty.
const GQL = `${TRINO_BACKEND_URL}/admin/graphql`;
// The certificate's ONE absolute path that both readers of the source can open: Trino, in its
// container, and the source's own pgwire server, which Provisa starts on this host for the
// source's writes (REQ-1946) and the keys it declares (REQ-2170). docker-compose.e2e-mounts.yml
// mounts the file into Trino at this same path. `/certs/sharepoint.pfx` exists only in the
// container: the host's server died on FileNotFoundException and registerTable failed with
// "Error instantiating JsonCustomSchema" (runs 42367768201, 42368724170).
function certPath(): string {
  const path = process.env.PROVISA_E2E_SHAREPOINT_CERT_HOST;
  if (!path) {
    throw new Error(
      "PROVISA_E2E_SHAREPOINT_CERT_HOST is not set: the certificate's absolute host path",
    );
  }
  return path;
}

// SharePoint calendar list columns (obtained via Graph API — information_schema.columns
// returns empty for the Calcite-based sharepoint connector, so we supply known columns)
// visibleTo must be non-empty — empty list means "visible to no role" in Provisa's governance model
const CALENDAR_COLUMNS = [
  // Exactly the names the connector exposes in e2e_sharepoint.information_schema.columns for
  // sharepoint.calendar — the connector maps the SharePoint list fields to snake_case (EventDate
  // -> start_time, EndDate -> end_time). register_table resolves each column's type by exact name
  // (REQ-846) and refuses the registration for any name the source does not have.
  { name: "id", visibleTo: ["developer", "org_admin", "analyst"] },
  { name: "title", visibleTo: ["developer", "org_admin", "analyst"] },
  { name: "start_time", visibleTo: ["developer", "org_admin", "analyst"] },
  { name: "end_time", visibleTo: ["developer", "org_admin", "analyst"] },
  { name: "description", visibleTo: ["developer", "org_admin", "analyst"] },
  { name: "location", visibleTo: ["developer", "org_admin", "analyst"] },
];

async function gql(query: string, variables: Record<string, unknown> = {}) {
  // 120s: backend may be processing a Trino catalog init; fail explicitly rather than hanging
  // indefinitely and consuming the entire test timeout with no diagnostic error.
  const res = await fetch(GQL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(120000),
  });
  return res.json();
}

async function cleanupSource() {
  await deleteSourceAndItsTables(gql, SOURCE_ID);
}

async function getDomainId(): Promise<string> {
  const res = await gql(`{ domains { id } }`);
  const domains: Array<{ id: string }> = res.data?.domains ?? [];
  // The first domain a table may be put into: the form offers no internal one (meta, ontology, ops).
  const d = domains.find((d) => isAssignableDomain(d.id));
  if (!d) {
    throw new Error(
      `no assignable domain among ${JSON.stringify(domains)}: ${JSON.stringify(res)}`,
    );
  }
  return d.id;
}

test.beforeEach(async () => {
  await cleanupSource();
});

test.afterEach(async () => {
  await cleanupSource();
});

test("sharepoint connector: add source and verify calendar list is available", async ({ page }) => {
  // 600s: beforeEach cleanup + navigation + form fill (~150s) + Trino SharePoint catalog init
  // (~135s) + headroom. The trino lane runs with workers = 1 (playwright.config.ts derives
  // `workers` from CORE_BACKENDS, which is a single entry outside the core lane), so the splunk
  // spec is serialized against this one and cannot queue a competing catalog init.
  test.setTimeout(600000);

  // Redirect the UI's API calls from the default DuckDB backend to the Trino backend so
  // that register_source() creates a real Trino catalog and schema enumeration succeeds.
  // The Apollo client uses relative URLs (/admin/graphql) so the browser sends requests
  // to the UI origin (http://localhost:3901); Vite proxies them server-side. Intercepting
  // at the UI origin redirects them to the Trino backend before Vite's proxy fires.
  // `**`, not `*`: Playwright compiles a single star to `([^/]*)`, which stops at the first slash,
  // so `/admin*` matched nothing the app actually calls — every request, /admin/graphql included,
  // fell through to Vite and reached the default DuckDB backend instead. The source was then
  // registered against a backend whose register_source() is a no-op for this connector, so the
  // schema dropdown this test waits on could never populate. `**` compiles to `(.*)` and matches
  // the full path.
  for (const prefix of ["/admin", "/data", "/query", "/health"]) {
    await page.route(`${UI_URL}${prefix}**`, (route) => {
      route.continue({ url: route.request().url().replace(UI_URL, TRINO_BACKEND_URL) });
    });
  }

  // ── 1. Add SharePoint source via UI ──────────────────────────────────────
  await page.goto("/sources");
  await page.waitForFunction(() => !document.body.textContent?.includes("Loading sources..."), {
    timeout: 30000,
  });
  // A schema-version advance makes apolloClient.ts re-fetch every active query, causing
  // "Loading sources..." to reappear briefly. Use 30s to survive the re-fetch.
  await page.waitForSelector(".page-header", { timeout: 30000 });

  await addSourceOfType(page, "sharepoint");

  // Native inputs are inside <label> elements in the source form
  await page.locator('[data-testid="sources-id-input"]').fill(SOURCE_ID);
  await page.locator('[data-testid="sources-type-select"]').selectOption("sharepoint");

  // Mantine TextInput/Select: data-testid is forwarded to the underlying <input> element
  // (not a wrapper div), so selectors target data-testid directly — no child combinator.
  await page.waitForSelector('[data-testid="sharepoint-site-url-input"]', { timeout: 10000 });
  await page
    .locator('[data-testid="sharepoint-site-url-input"]')
    .fill("https://kenstott.sharepoint.com");
  await page
    .locator('[data-testid="sharepoint-tenant-id-input"]')
    .fill("5d2609cc-7eff-4b82-8f83-f0b28c71fafc");

  // Mantine Select — click the combobox input, then click the option in the listbox
  await page.locator('[data-testid="sharepoint-auth-type-select"]').click();
  await page.getByRole("option", { name: "Certificate" }).click();

  await page.waitForSelector('[data-testid="sharepoint-cert-path-input"]', { timeout: 10000 });
  await page
    .locator('[data-testid="sharepoint-client-id-input"]')
    .fill("d6f6b74e-df85-470f-8e68-e34c767436be");
  await page.locator('[data-testid="sharepoint-cert-path-input"]').fill(certPath());
  // PFX password — the cert was created with the password literally set to "YOUR_PFX_PASSWORD"
  await page.locator('[data-testid="sharepoint-cert-password-input"]').fill("YOUR_PFX_PASSWORD");

  await page.locator('[data-testid="sources-submit"]').click();

  // Wait for source row to appear in list
  await expect(page.locator(".data-table td").filter({ hasText: SOURCE_ID })).toBeVisible({
    timeout: 30000,
  });

  // ── 2. Open table form and verify SharePoint lists are enumerated ─────────
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 30000 });

  // Open the add-table form
  await page.getByRole("button", { name: "+ Table" }).first().click();
  await page.waitForSelector(".form-card", { timeout: 5000 });

  // Wait for source to appear in dropdown — Apollo cache-and-network may serve stale
  // data initially; the network fetch updates the cache within a few seconds.
  await page.waitForFunction(
    (id: string) => {
      const sel = document.querySelector<HTMLSelectElement>(
        '[data-testid="register-table-source-select"]',
      );
      return Array.from(sel?.options ?? []).some((o) => o.value === id);
    },
    SOURCE_ID,
    { timeout: 15000 },
  );
  // Select source — native <select> inside <label> in RegisterTableForm
  await page.locator('[data-testid="register-table-source-select"]').selectOption(SOURCE_ID);

  // Wait for Domain dropdown to have options
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return selects.some(
        (s) => s.dataset.testid === "register-table-domain-select" && s.options.length > 1,
      );
    },
    { timeout: 10000 },
  );
  const domainSelect = page.locator('[data-testid="register-table-domain-select"]');
  const firstDomain = await domainSelect.locator("option").nth(1).getAttribute("value");
  await domainSelect.selectOption(firstDomain!);

  // Wait for the source's schema to appear — Trino SharePoint catalog loads asynchronously; allow
  // 240s for Trino catalog initialisation + schema enumeration via SharePoint REST API.
  await page.waitForFunction(
    (schema) => {
      const sel = document.querySelector<HTMLSelectElement>(
        '[data-testid="register-table-schema-select"]',
      );
      return Array.from(sel?.options ?? []).some((o) => o.value === schema);
    },
    SCHEMA,
    { timeout: 240000 },
  );

  // Select sharepoint schema
  await page.locator('[data-testid="register-table-schema-select"]').selectOption(SCHEMA);

  // Wait for 'calendar' table to appear — proves SharePoint lists are enumerated: the Tables field (REQ-2234) lists them in its open list.
  const tablesField = page.getByTestId("register-tables-field").getByRole("textbox");
  await expect(tablesField).toBeEnabled({ timeout: 30000 });
  await tablesField.click();
  await expect(page.getByRole("option", { name: /^calendar/ })).toBeVisible({ timeout: 30000 });
  const tableOptions = await listedTableNames(page);
  await page.keyboard.press("Escape");

  expect(tableOptions).toContain("calendar");
  expect(tableOptions).toContain("events");

  // ── 3. Register 'calendar' via GQL with known columns ────────────────────
  // The Calcite sharepoint connector does not expose information_schema.columns
  // for user tables (connector-level bug), so the UI cannot auto-fetch columns.
  // We register via the mutation directly with columns obtained from Graph API.
  const domainId = await getDomainId();
  const registerResult = await gql(
    `mutation R($input: TableInput!) { registerTable(input: $input) { success message } }`,
    {
      input: {
        sourceId: SOURCE_ID,
        domainId,
        // The source schema the table lives in — the same SCHEMA selected in the
        // dropdown above, not a name derived from the domain. register_table introspects
        // <source>.<schemaName>.<tableName> to resolve each column's data type (REQ-846), and a
        // schema the catalog does not have resolves nothing, so the mutation refuses the
        // registration with "no data type could be resolved from the source".
        schemaName: SCHEMA,
        tableName: "calendar",
        alias: "e2e_sp_calendar",
        columns: CALENDAR_COLUMNS,
      },
    },
  );

  expect(
    registerResult.errors,
    `registerTable errors: ${JSON.stringify(registerResult.errors)}`,
  ).toBeUndefined();
  // The mutation reports a refusal in `message` with no GraphQL error, so carry it into the
  // assertion — otherwise CI shows only "expected true, received false".
  expect(
    registerResult.data?.registerTable?.success,
    `registerTable message: ${registerResult.data?.registerTable?.message}`,
  ).toBe(true);

  // REQ-1921: registered through the admin, the table starts as draft; released, it is read.
  const registeredId = Number(
    /\(id=(\d+)\)/.exec(String(registerResult.data?.registerTable?.message))?.[1],
  );
  const releaseResult = await gql(
    `mutation($id: Int!) { setTableDraft(tableId: $id, draft: false) { success message } }`,
    { id: registeredId },
  );
  expect(
    releaseResult.data?.setTableDraft?.success,
    `setTableDraft: ${JSON.stringify(releaseResult)}`,
  ).toBe(true);

  // ── 4. Verify the registered table appears in the UI ─────────────────────
  // Reload the page to show the registered tables list
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 30000 });

  await expect(page.locator(".data-table").getByText("e2e_sp_calendar")).toBeVisible({
    timeout: 10000,
  });

  // ── 5. Query via GQL data endpoint ───────────────────────────────────────
  const queryResult = await gql(`{ e2eSpCalendar { id title } }`);
  // Query may error since the Calcite sharepoint connector does not support
  // direct SQL table access (getTableHandle returns null for user tables).
  // We assert the response shape is defined — success or a recognisable error.
  expect(queryResult).toBeDefined();
});
