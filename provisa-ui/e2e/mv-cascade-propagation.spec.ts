// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

// REQ-966/158/133: a genuine end-to-end DAG cascade — a source-table state change propagates through
// a two-level chain of materialized views, verified via the running app's own SQL/table-admin surfaces
// rather than the internal event-loop harness (tests/mvmvp/*_integration.py cover that boundary; this
// spec covers the user-observable outcome: pet_store.pets -> views.e2e_mv_a -> views.e2e_mv_b).
//
// The SQL Explorer's Run button wraps every query as a read-only SELECT subquery (SqlPage.tsx), so it
// cannot execute the UPDATE that mutates the source, and there is no UI control that forces an
// immediate MV refresh (only a 300s+ TTL poll). Both gaps are bridged with direct calls to the same
// backend endpoints the UI itself uses: POST /data/sql for the raw UPDATE/SELECT, POST /admin/graphql
// for the refreshMv mutation the admin schema already exposes.

const A = "e2e_mv_a";
const B = "e2e_mv_b";

async function runSql(page: import("@playwright/test").Page, sql: string) {
  const resp = await page.request.post("/data/sql", { data: { sql, role: "admin" } });
  expect(resp.ok(), await resp.text()).toBeTruthy();
  const json = await resp.json();
  return json.data.sql as Record<string, unknown>[];
}

async function refreshMv(page: import("@playwright/test").Page, tableName: string) {
  const resp = await page.request.post("/admin/graphql", {
    data: {
      query: "mutation($mvId: String!) { refreshMv(mvId: $mvId) { success message } }",
      variables: { mvId: `view-${tableName}` },
    },
  });
  const json = await resp.json();
  expect(json.data?.refreshMv?.success, JSON.stringify(json)).toBeTruthy();
}

async function typeSql(page: import("@playwright/test").Page, sql: string) {
  const editor = page.locator(".cm-content").first();
  await editor.click();
  await page.keyboard.press("Meta+a");
  await page.keyboard.press("Backspace");
  await page.keyboard.type(sql);
}

async function createView(page: import("@playwright/test").Page, sql: string, alias: string) {
  await typeSql(page, sql);
  const runResp = page.waitForResponse((r) => r.url().includes("/data/sql") && r.request().method() === "POST");
  await page.getByTestId("sql-run").click();
  await runResp;
  await expect(page.getByTestId("download-csv-btn")).toBeVisible({ timeout: 10000 });

  await page.getByTestId("sql-open-view-modal").click();
  // data-testid="view-modal" sits on Mantine's Modal.Root, which is a zero-height wrapper (its
  // content is fixed-positioned/portaled) — assert on the actual field instead of the wrapper.
  await expect(page.getByTestId("view-alias-input")).toBeVisible({ timeout: 10000 });
  await page.getByTestId("view-alias-input").fill(alias);
  await page.getByTestId("view-domain-select").click();
  await page.getByRole("option", { name: /pet-store/i }).first().click();
  await page.getByTestId("save-view-button").click();

  await expect(page.getByTestId("view-saved-close-button")).toBeVisible({ timeout: 10000 });
  await page.getByTestId("view-saved-close-button").click();
}

async function materialize(page: import("@playwright/test").Page, tableName: string) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  const row = page.locator("tr").filter({ hasText: tableName }).first();
  await row.waitFor({ timeout: 10000 });
  await row.click();
  const editBtn = page.getByTestId("table-read-view-edit");
  await editBtn.waitFor({ timeout: 5000 });
  await editBtn.click();
  await page.waitForSelector("input[type='checkbox']", { timeout: 5000 });

  // Mantine's Checkbox renders <input> and <label> as siblings (label uses a `for` attribute rather
  // than wrapping the input), so getByLabel — which resolves that association — is required here;
  // a `label`-then-descendant-`input` locator never matches.
  const matCheckbox = page.getByLabel(/Materialized View/i);
  await matCheckbox.waitFor({ timeout: 5000 });
  if (!(await matCheckbox.isChecked())) await matCheckbox.check();

  await page.getByRole("button", { name: /save/i }).first().click();
  await page.waitForTimeout(500);
}

async function deleteTable(page: import("@playwright/test").Page, tableName: string) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  const row = page.locator("tr").filter({ hasText: tableName }).first();
  if ((await row.count()) === 0) return;
  await row.click();
  page.once("dialog", (d) => d.accept());
  const delBtn = page.getByTestId("table-read-view-delete");
  if ((await delBtn.count()) > 0) {
    await delBtn.click();
    await page.waitForTimeout(500);
  }
}

test("source state change propagates through a two-level materialized-view DAG", async ({ page }) => {
  test.setTimeout(90000);

  // In demo mode, a fresh browser context auto-starts the guided tour on first navigation, which
  // hijacks the route to its own first stop (/sources) regardless of where the test navigated.
  // Seeding the "seen" flag before the first goto suppresses that so the requested route sticks.
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));

  try {
    // Build source -> views.e2e_mv_a -> views.e2e_mv_b via the real SQL Explorer + Create View flow.
    await page.goto("/sql");
    await page.waitForSelector(".cm-content", { timeout: 15000 });
    await createView(page, "SELECT id, name, price FROM pet_store.pets WHERE id = 1", A);

    await page.waitForSelector(".cm-content", { timeout: 15000 });
    await createView(page, `SELECT id, name, price FROM views.${A}`, B);

    // Turn both views into materialized views via the Tables admin surface (real UI toggle).
    await materialize(page, A);
    await materialize(page, B);

    // views.e2e_mv_a's activation synchronously populated it from pets.id=1 (price 380.00); force
    // e2e_mv_b to (re)materialize from it now so both levels start from a known baseline.
    await refreshMv(page, B);
    let rows = await runSql(page, `SELECT price FROM views.${B} WHERE id = 1`);
    expect(Number(rows[0].price)).toBeCloseTo(380.0, 2);

    // Mutate the physical source table directly (the SQL Explorer's Run wraps all SQL as a SELECT
    // subquery and cannot execute this UPDATE) and confirm it landed.
    const updateResult = await runSql(page, "UPDATE pet_store.pets SET price = 999.99 WHERE id = 1");
    expect(Number(updateResult[0].Count)).toBeGreaterThan(0);

    // Force the cascade rather than waiting on the default 300s TTL poll: refresh level 1, then level
    // 2 — mirroring the order the real change-signal loop would apply the ripple in.
    await refreshMv(page, A);
    await refreshMv(page, B);

    rows = await runSql(page, `SELECT price FROM views.${B} WHERE id = 1`);
    expect(Number(rows[0].price)).toBeCloseTo(999.99, 2); // the change reached level 2
  } finally {
    // Best-effort cleanup so repeated runs don't accumulate views or leave the shared demo row mutated.
    await runSql(page, "UPDATE pet_store.pets SET price = 380.00 WHERE id = 1").catch(() => {});
    await deleteTable(page, B).catch(() => {});
    await deleteTable(page, A).catch(() => {});
  }
});
