// Copyright (c) 2026 Kenneth Stott
// Canary: 6889fd4a-055f-46a3-8737-883e85f84bf4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The three screens of the source-to-query e2e (REQ-1671), as steps both lanes' specs share:
// the Sources form, the Register Table form, and the SQL page's results grid.

import { BACKEND_URL, expect, type Page } from "./coverage";
import { deleteSourceAndItsTables } from "./delete-source";

export const DOMAIN = "pet-store"; // shipped by both lanes' configs; its SQL schema is pet_store

export async function openSourcesForm(page: Page) {
  await page.goto("/sources");
  // /sources sits behind a CapabilityGate — the header renders once the identity bootstrap resolves.
  await page.waitForSelector(".page-header", { timeout: 60000 });
  // the form's own default type: a spec sets its kind on the form's Type field (selectOption)
  await addSourceOfType(page, "postgresql");
}

/** "+ Source" opens the type picker first; choosing `type` there opens the create form with it. */
export async function addSourceOfType(page: Page, type: string) {
  await page.getByRole("button", { name: /\+ Source/i }).click();
  // a type chosen before is listed under Recent too: either row picks it
  await page.getByTestId(`source-type-option-${type}`).first().click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
}

// The cache TTL a spec asked for while filling the short create step, by page: the step asks
// only what a source needs in order to exist, so the TTL is set in the edit form the new source
// opens in (submitSourceAndExpectListed), unless the step itself shows the field.
const askedCacheTtl = new WeakMap<Page, number>();

/** Give a new source the cache TTL its tables need (REQ-1907): a source the engine serves from a
 *  replica must have a landing clock before a table is registered. A kind that cannot exist
 *  without one shows the field in the create step, and it is filled there; for any other kind
 *  it is set, right after the source is created, in the edit form it opens in. */
export async function setSourceCacheTtl(page: Page, seconds: number) {
  // The panel is open with no toggle wherever it is shown.
  if ((await page.getByTestId("source-load-management-panel").count()) === 0) {
    askedCacheTtl.set(page, seconds);
    return;
  }
  await page.getByTestId("cache-ttl-input").fill(String(seconds));
}

/** Remove every source the tests registered on this worker's backend, with its tables.
 *
 * For a spec's teardown, BEFORE it takes its stack down. A source left registered against a
 * server that is gone is not inert: the backend goes on building its tables' replicas for as
 * long as the registration exists -- the hiveserver2 case's table was retried 29 times over the
 * remaining 83 minutes of the core lane, each with a refused connection and a traceback (run
 * 37918605161) -- and every read of the whole model meets it.
 *
 * The tests' sources are told from the model's own by their ids: `e2e_<kind>_<13-digit stamp>`,
 * which no source of the demo model has. */
export async function removeTestSources(baseUrl: string = BACKEND_URL): Promise<string[]> {
  const gql = async (query: string, variables: Record<string, unknown> = {}) => {
    const res = await fetch(`${baseUrl}/admin/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-provisa-role": "org_admin" },
      body: JSON.stringify({ query, variables }),
    });
    if (!res.ok) throw new Error(`admin GraphQL answered ${res.status}: ${await res.text()}`);
    return res.json();
  };
  const listed = await gql("{ sources { id } }");
  if (listed.errors)
    throw new Error(`sources could not be listed: ${JSON.stringify(listed.errors)}`);
  const mine = ((listed.data?.sources ?? []) as Array<{ id: string }>)
    .map((s) => s.id)
    .filter((id) => /^e2e_[a-z0-9_]+_\d{13}$/.test(id));
  for (const id of mine) await deleteSourceAndItsTables(gql, id);
  return mine;
}

// A source form field, by its label. The sources list behind the form has sortable column headers
// whose buttons are labelled "Host, not sorted", "Port, ...", "Database, ...", so a label alone
// names two elements once a source is listed (UI e2e Trino lane on v0.1.0-alpha.477: "strict mode
// violation: getByLabel(/^Host/) resolved to 2 elements"). Narrow a label locator with
// `.and(page.locator(FORM_FIELD))`.
export const FORM_FIELD = "input, select, textarea";

export async function submitSourceAndExpectListed(page: Page, sourceId: string) {
  await page.getByTestId("sources-submit").click();
  await expect(page.locator(".data-table td").filter({ hasText: sourceId })).toBeVisible({
    timeout: 60000,
  });
  // The row is listed as soon as the create returns, before the settings saved with it (cache,
  // Replicate, region...) -- each of which rebuilds the model. The create is done when the new
  // source opens in edit; a step that navigates before then waits behind those rebuilds (run
  // 40227062159: the next page's tables query outlasted its 15 s).
  const save = page.getByTestId("sources-edit-submit");
  await expect(save).toBeVisible({ timeout: 60000 });
  const seconds = askedCacheTtl.get(page);
  if (seconds === undefined) return;
  askedCacheTtl.delete(page);
  // The new source is selected and its detail is open in edit, everything else at defaults.
  // The edit form is in sections; the cache TTL is on the Load and timeliness one.
  await page.getByTestId("source-section-load").click();
  await page.getByTestId("cache-ttl-input").fill(String(seconds));
  await save.click();
  // Saved: the panel shows the source's detail again.
  await expect(page.getByTestId("source-detail-edit")).toBeVisible({ timeout: 60000 });
}

/**
 * Open the schema discovery of a source whose detail panel is open. Its Discover button is in the
 * panel's head, which shows no actions in edit, and a just-created source opens in edit: the edit
 * is left first.
 */
export async function openSourceDiscovery(page: Page, sourceId: string) {
  const discover = page.getByTestId(`sources-discover-${sourceId}`);
  const cancelEdit = page.getByTestId("sources-edit-cancel");
  // A just-created source's edit opens once the list has it again; a source left in its read
  // view shows Discover. The head never shows both.
  await expect(cancelEdit.or(discover)).toBeVisible({ timeout: 60000 });
  if (await cancelEdit.isVisible()) await cancelEdit.click();
  await discover.click();
  await expect(page.getByTestId("schema-discovery")).toBeVisible({ timeout: 10000 });
}

export async function openRegisterForm(page: Page, sourceId: string, domain: string = DOMAIN) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.getByRole("button", { name: /\+ Table/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
  // Apollo may serve the sources list from cache first; the new source arrives with the refetch.
  await expect(
    page.getByTestId("register-table-source-select").locator(`option[value='${sourceId}']`),
  ).toHaveCount(1, { timeout: 30000 });
  await page.getByTestId("register-table-source-select").selectOption(sourceId);
  await expect(
    page.getByTestId("register-table-domain-select").locator(`option[value='${domain}']`),
  ).toHaveCount(1, { timeout: 30000 });
  await page.getByTestId("register-table-domain-select").selectOption(domain);
}

/** Create a domain of the test's own (REQ-1933: a table's SQL address is unique in its domain, so
 * a case that registers a table the shipped model already has registers it somewhere else). */
export async function createDomain(page: Page, id: string): Promise<void> {
  const res = await page.request.post("/admin/graphql", {
    data: {
      query:
        "mutation($id: String!) { createDomain(input: { id: $id, description: $id }) { success message } }",
      variables: { id },
    },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const result = (await res.json()).data.createDomain;
  expect(result.success, result.message).toBeTruthy();
}

/** The general bound on a source's own server answering after its source is saved. */
export const SOURCE_SERVING_BUDGET_MS = 15 * 60 * 1000;

/** AskAmerica's bound: its server answers within a minute of the save; later is a defect. */
export const ASKAMERICA_SERVING_BUDGET_MS = 60 * 1000;

/**
 * Wait until a source's own server lists the tables of `schema`. While it starts, the listing
 * answers `STARTING:` (the port is not open, or the catalog is being prepared) and is asked
 * again, as the Register Table form does. Any other error ends the wait at once with that error:
 * a server that exited or could not prepare its catalog is never waited out. The schema list is
 * no use here: a source that declares its schemas answers it without its server.
 */
export async function waitForSourceServing(
  page: Page,
  sourceId: string,
  schema: string,
  budgetMs = SOURCE_SERVING_BUDGET_MS,
): Promise<string[]> {
  const deadline = Date.now() + budgetMs;
  let last = "";
  for (;;) {
    const res = await page.request.post("/admin/graphql", {
      data: {
        query:
          "query($sourceId: String!, $schema: String!) { availableTables(sourceId: $sourceId, schemaName: $schema) { name } }",
        variables: { sourceId, schema },
      },
    });
    expect(res.ok(), await res.text()).toBeTruthy();
    const body = await res.json();
    if (!body.errors?.length) {
      return (body.data.availableTables as { name: string }[]).map((t) => t.name);
    }
    const message = body.errors.map((e: { message: string }) => e.message).join("; ");
    if (!message.includes("STARTING:")) {
      throw new Error(`source ${sourceId} is not serving: ${message}`);
    }
    if (message !== last) {
      console.log(`[${new Date().toISOString()}] ${message}`);
      last = message;
    }
    if (Date.now() >= deadline) {
      throw new Error(`source ${sourceId} was still starting after ${budgetMs} ms: ${message}`);
    }
    await page.waitForTimeout(5000);
  }
}

/** Pick a schema and a table in the pickers, waiting for each to be introspected from the source. */
export async function pickSchemaAndTable(page: Page, schema: string, table: string) {
  const schemaSelect = page.getByTestId("register-table-schema-select");
  await expect(schemaSelect.locator(`option[value='${schema}']`)).toHaveCount(1, {
    timeout: 120000,
  });
  if ((await schemaSelect.inputValue()) !== schema) await schemaSelect.selectOption(schema);
  const tableSelect = page.getByTestId("register-table-table-select");
  await expect(tableSelect.locator(`option[value='${table}']`)).toHaveCount(1, { timeout: 120000 });
  await tableSelect.selectOption(table);
  // Column metadata loads asynchronously after the table selection (RegisterTableForm's own
  // useEffect) and the submit button is never disabled while it is in flight — an openapi table
  // additionally needs a live HTTP round-trip (dynamic column inference off the response shape)
  // that can lose the race with the caller's next action under load, leaving submit clicked
  // before any column has populated and "At least one column must be selected" silently blocking
  // it forever. Wait for the discovered columns before returning control to the caller.
  await expect(discoveredColumns(page).first()).toBeVisible({ timeout: 30000 });
}

/**
 * Where the register form shows the columns discovery found: the short step names them in one
 * summary line; the longer path (a query API's preview) lists each in its column table.
 */
function discoveredColumns(page: Page) {
  return page
    .getByTestId("register-table-columns-summary")
    .or(page.locator('[data-testid^="register-table-col-selected-"]'));
}

/** Waits until discovery has found each named column, on either form of the register step. */
export async function expectDiscoveredColumns(page: Page, names: string[], timeout = 60000) {
  const summary = page.getByTestId("register-table-columns-summary");
  for (const name of names) {
    // In the summary a name stands between "): " or ", " and ", " or ". ".
    const named = new RegExp(`(: |, )${name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(,|\\.)`);
    await expect(
      summary
        .filter({ hasText: named })
        .or(page.getByTestId(`register-table-col-selected-${name}`)),
    ).toBeVisible({ timeout });
  }
}

/** How many columns discovery found, on either form of the register step. */
export async function discoveredColumnCount(page: Page): Promise<number> {
  await expect(discoveredColumns(page).first()).toBeVisible({ timeout: 30000 });
  const summary = page.getByTestId("register-table-columns-summary");
  if (await summary.isVisible()) {
    const counted = /\((\d+)\)/.exec((await summary.textContent()) ?? "");
    expect(counted, "the register step's column summary gives no count").not.toBeNull();
    return Number(counted![1]);
  }
  return page.locator('[data-testid^="register-table-col-selected-"]').count();
}

/**
 * What a test sets on a table's columns once it is registered. The short register step asks none
 * of it: it is set in the editor the step opens on the registered draft.
 */
export interface RegisteredColumnEdits {
  /** Columns declared the primary key (Columns > Type and keys). */
  primaryKey?: string[];
  /** An ingest source's per-column JSON paths (Columns > Naming). */
  paths?: Record<string, string>;
}

/**
 * REQ-1921: a table or view registered through the admin starts as draft — out of service until
 * its domain's owners release it. Release every draft table matching ``match`` (a source's, or one
 * table by name), as a domain owner does after registering it and before anything reads it.
 * Returns how many were released.
 */
export async function releaseDraftTables(
  page: Page,
  match: { sourceId?: string; tableName?: string },
  baseUrl = "",
): Promise<number> {
  const res = await page.request.post(`${baseUrl}/admin/graphql`, {
    data: { query: "{ tables { id sourceId tableName draft } }" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const tables = (await res.json()).data.tables as {
    id: number;
    sourceId: string;
    tableName: string;
    draft: boolean;
  }[];
  const drafts = tables.filter(
    (t) =>
      t.draft &&
      (match.sourceId === undefined || t.sourceId === match.sourceId) &&
      (match.tableName === undefined || t.tableName === match.tableName),
  );
  for (const t of drafts) {
    const released = await page.request.post(`${baseUrl}/admin/graphql`, {
      data: {
        query: `mutation($id: Int!) { setTableDraft(tableId: $id, draft: false) { success message } }`,
        variables: { id: t.id },
      },
    });
    expect(released.ok(), await released.text()).toBeTruthy();
    const body = await released.json();
    expect(body.errors, JSON.stringify(body.errors)).toBeUndefined();
    expect(body.data.setTableDraft.success, body.data.setTableDraft.message).toBeTruthy();
  }
  return drafts.length;
}

/**
 * Submit the registration form. A table that will be a replica, of a source that states no Cache
 * TTL under a ttl change signal, states one itself when it is registered (#208, REQ-1907): the form
 * asks for it then (register-table-cache-ttl), and nothing is registered until it has one.
 */
export async function submitRegister(page: Page, cacheTtlSeconds = 300): Promise<void> {
  await continueRegister(page, cacheTtlSeconds);
  await page.getByTestId("table-edit-save").click();
}

/** The register step's Continue: the chosen table's settings follow in the editor's sections,
 * where Save registers it (REQ-2109). */
async function continueRegister(page: Page, cacheTtlSeconds: number): Promise<void> {
  const cacheTtl = page.getByTestId("register-table-cache-ttl");
  if ((await cacheTtl.count()) > 0) await cacheTtl.fill(String(cacheTtlSeconds));
  await page.getByTestId("register-table-submit").click();
  await expect(page.getByTestId("table-edit-save")).toBeVisible({ timeout: 30000 });
}

/**
 * Submit the registration, wait for the table's row in the tables list, and return the name a
 * SELECT addresses it by. The list shows the alias (the GraphQL name, camelCase under the default
 * convention); the SQL-plane name is its snake form, which the server reports as the last segment
 * of the table's dataset name (`provisa/<domain>/<sql name>`) — read from the server rather than
 * re-derived here, so the test asserts against the name that is actually served.
 */
export async function submitRegisterAndExpectListed(
  page: Page,
  sourceId: string,
  timeoutMs = 120000,
  // REQ-1730: `page.request` is a separate APIRequestContext that `page.route()` never intercepts
  // (only browser-initiated requests are routed — see engine-swap-helpers.ts's own comment on
  // reprovisionSourceOnEngine), so a caller that has routed `page`'s browser traffic at a NON-
  // default backend (the reboot harness) still needs this one call redirected explicitly. Empty
  // string (default) preserves the original relative-URL/Vite-proxied behavior for every other
  // caller unchanged.
  baseUrl = "",
  columns: RegisteredColumnEdits = {},
): Promise<string> {
  await continueRegister(page, 300);
  // What the test sets on its columns is set in the create form's Columns section, before the one
  // Save registers the table (REQ-2109).
  const paths = Object.entries(columns.paths ?? {});
  const primaryKey = columns.primaryKey ?? [];
  if (paths.length > 0 || primaryKey.length > 0) {
    await page.getByTestId("table-section-columns").click();
    for (const [column, path] of paths) {
      await page.getByTestId(`table-edit-col-path-${column}`).fill(path);
    }
    if (primaryKey.length > 0) {
      await page.getByTestId("column-tab-type").click();
      for (const column of primaryKey) {
        await page
          .getByTestId(`column-row-${column}`)
          .getByRole("checkbox", { name: "Primary Key" })
          .check();
      }
    }
  }
  const save = page.getByTestId("table-edit-save");
  await save.click();
  // Registration rebuilds the schemas; the row lands in the tables list when it is done.
  const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
  await expect(row).toBeVisible({ timeout: timeoutMs });
  await expect(save).toBeHidden({ timeout: 15000 });
  await releaseDraftTables(page, { sourceId }, baseUrl);
  const res = await page.request.post(`${baseUrl}/admin/graphql`, {
    data: { query: "{ tables { sourceId dqDataset } }" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const tables = (await res.json()).data.tables as { sourceId: string; dqDataset: string | null }[];
  const mine = tables.find((t) => t.sourceId === sourceId);
  expect(mine?.dqDataset, `no dataset name reported for ${sourceId}`).toBeTruthy();
  return mine!.dqDataset!.split("/").pop()!;
}

/** The `path` (endpoint URL) a baked-in source already connects with — reused so a new
 * registration reaches the same live mock without hardcoding a port the e2e harness may reassign. */
export async function existingSourcePath(page: Page, sourceId: string): Promise<string> {
  const res = await page.request.post("/admin/graphql", {
    data: { query: "{ sources { id path } }" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const sources = (await res.json()).data.sources as { id: string; path: string | null }[];
  const found = sources.find((s) => s.id === sourceId);
  expect(found?.path, `source ${sourceId} has no path`).toBeTruthy();
  return found!.path!;
}

/** SQL-plane names of every table a source has registered, regardless of whether they were added
 * through the Register Table form or auto-registered by the source itself (graphql_remote). */
export async function registeredTableNames(page: Page, sourceId: string): Promise<string[]> {
  const res = await page.request.post("/admin/graphql", {
    data: { query: "{ tables { sourceId tableName } }" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const tables = (await res.json()).data.tables as { sourceId: string; tableName: string }[];
  return tables.filter((t) => t.sourceId === sourceId).map((t) => t.tableName);
}

/**
 * Register one of the tables a remote source offers (REQ-308, REQ-322): adding such a source
 * registers nothing, so the table is found in what the source offers and registered with the
 * columns named, each visible to every role. Returns the table's registered name.
 */
export async function registerOfferedTable(
  page: Page,
  sourceId: string,
  schemaName: string,
  nameIncludes: string,
  columns: string[],
): Promise<string> {
  expect(await registeredTableNames(page, sourceId), `${sourceId} registered tables`).toEqual([]);
  const offered = await page.request.post("/admin/graphql", {
    data: {
      query: `query($sourceId: String!, $schemaName: String!) {
        availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
      }`,
      variables: { sourceId, schemaName },
    },
  });
  expect(offered.ok(), await offered.text()).toBeTruthy();
  const offeredJson = await offered.json();
  expect(offeredJson.errors, JSON.stringify(offeredJson.errors)).toBeUndefined();
  const names = (offeredJson.data.availableTables as { name: string }[]).map((t) => t.name);
  const tableName = names.find((n) => n.includes(nameIncludes));
  expect(tableName, `${sourceId} offers no table matching ${nameIncludes}: ${names}`).toBeTruthy();
  const res = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { registerTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: "",
          schemaName,
          tableName,
          columns: columns.map((name) => ({ name, visibleTo: ["*"] })),
        },
      },
    },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const json = await res.json();
  expect(json.errors, JSON.stringify(json.errors)).toBeUndefined();
  expect(json.data.registerTable.success, json.data.registerTable.message).toBeTruthy();
  expect(await registeredTableNames(page, sourceId)).toEqual([tableName]);
  await releaseDraftTables(page, { sourceId, tableName: tableName as string });
  return tableName as string;
}

export async function typeSql(page: Page, sql: string) {
  const editor = page.locator(".cm-content").first();
  await editor.click();
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.press("Backspace");
  await page.keyboard.type(sql);
  // Close CodeMirror's completion popup so the run sees exactly this statement.
  await page.keyboard.press("Escape");
  await expect(editor).toHaveText(sql.replace(/\s+/g, " ").trim(), { useInnerText: true });
}

/** Run the SQL on the SQL page and return the rendered result rows as arrays of cell text. */
export async function runSqlOnPage(
  page: Page,
  sql: string,
  role = "org_admin",
): Promise<string[][]> {
  await page.goto("/sql");
  await page.waitForSelector(".cm-content", { timeout: 30000 });
  const picker = page.getByTestId("sql-role");
  if ((await picker.inputValue()) !== role) {
    await picker.click();
    const option = page.getByRole("option", { name: role, exact: true });
    // A lane whose config declares no roles offers none to pick; the run then acts as the caller.
    if (await option.count()) await option.click();
    else await page.keyboard.press("Escape");
  }
  await typeSql(page, sql);
  const runResp = page.waitForResponse(
    (r) => r.url().includes("/data/sql") && r.request().method() === "POST",
  );
  await page.getByTestId("sql-run").click();
  const resp = await runResp;
  expect(resp.ok(), await resp.text()).toBeTruthy();
  // The first query on a materialize-only source lands it first; allow for that. A statement that
  // matched nothing is answered too, with no grid: a caller polling for rows asks again.
  const grid = page.getByTestId("download-csv-btn");
  await expect(grid.or(page.getByTestId("sql-results-empty"))).toBeVisible({ timeout: 120000 });
  if (!(await grid.isVisible())) return [];
  const rows = page.locator(".sql-results-table tbody tr");
  return rows.evaluateAll((trs) =>
    trs.map((tr) =>
      Array.from(tr.querySelectorAll("td")).map((td) => td.textContent?.trim() ?? ""),
    ),
  );
}
