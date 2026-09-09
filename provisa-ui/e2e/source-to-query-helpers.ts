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

import { expect, type Page } from "./coverage";

export const DOMAIN = "pet-store"; // shipped by both lanes' configs; its SQL schema is pet_store

export async function openSourcesForm(page: Page) {
  await page.goto("/sources");
  // /sources sits behind a CapabilityGate — the header renders once the identity bootstrap resolves.
  await page.waitForSelector(".page-header", { timeout: 60000 });
  await page.getByRole("button", { name: /\+ Source/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
}

export async function submitSourceAndExpectListed(page: Page, sourceId: string) {
  await page.getByTestId("sources-submit").click();
  await expect(page.locator(".data-table td").filter({ hasText: sourceId })).toBeVisible({
    timeout: 60000,
  });
}

export async function openRegisterForm(page: Page, sourceId: string) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.getByRole("button", { name: /\+ Table/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
  // Apollo may serve the sources list from cache first; the new source arrives with the refetch.
  await expect(
    page.getByTestId("register-table-source-select").locator(`option[value='${sourceId}']`),
  ).toHaveCount(1, { timeout: 30000 });
  await page.getByTestId("register-table-source-select").selectOption(sourceId);
  await page.getByTestId("register-table-domain-select").selectOption(DOMAIN);
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
}

/**
 * Submit the registration, wait for the table's row in the tables list, and return the name a
 * SELECT addresses it by. The list shows the alias (the GraphQL name, camelCase under the default
 * convention); the SQL-plane name is its snake form, which the server reports as the last segment
 * of the table's dataset name (`provisa/<domain>/<sql name>`) — read from the server rather than
 * re-derived here, so the test asserts against the name that is actually served.
 */
export async function submitRegisterAndExpectListed(page: Page, sourceId: string): Promise<string> {
  await page.getByTestId("register-table-submit").click();
  // Registration rebuilds the schemas; the row lands in the tables list when it is done.
  const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
  await expect(row).toBeVisible({ timeout: 120000 });
  const res = await page.request.post("/admin/graphql", {
    data: { query: "{ tables { sourceId dqDataset } }" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const tables = (await res.json()).data.tables as { sourceId: string; dqDataset: string | null }[];
  const mine = tables.find((t) => t.sourceId === sourceId);
  expect(mine?.dqDataset, `no dataset name reported for ${sourceId}`).toBeTruthy();
  return mine!.dqDataset!.split("/").pop()!;
}

async function typeSql(page: Page, sql: string) {
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
  // The first query on a materialize-only source lands it first; allow for that.
  await expect(page.getByTestId("download-csv-btn")).toBeVisible({ timeout: 120000 });
  const rows = page.locator(".sql-results-table tbody tr");
  return rows.evaluateAll((trs) =>
    trs.map((tr) =>
      Array.from(tr.querySelectorAll("td")).map((td) => td.textContent?.trim() ?? ""),
    ),
  );
}
