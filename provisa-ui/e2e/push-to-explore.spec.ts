// Copyright (c) 2026 Kenneth Stott
// Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the trademark holders.

import { test, expect } from "./coverage";

/**
 * Tests the push-to-explore flows:
 *   - SQL panel "Open in SQL" (→) button navigates to /sql and auto-runs the query
 *   - Cypher panel "Open in Graph" (→) button navigates to /graph and executes the Cypher
 *
 * Both flows use ps__pets with the chained many-to-one query so the compiled SQL
 * and Cypher are non-trivial (cover the join rewrite path).
 *
 * Requires the backend on port 8000 with ps__pets / shelter schema loaded.
 */

const PETS_QUERY = `{ ps__pets { assignment { breedName employee { lastName } } name breedName } }`;

// ── Helpers ───────────────────────────────────────────────────────────────────

async function dismissOverlay(page: import("@playwright/test").Page) {
  const overlay = page.locator(".graphiql-dialog-overlay");
  if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
    await page.keyboard.press("Escape");
    await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
  }
}

async function openProviasPanel(page: import("@playwright/test").Page) {
  const sidebar = page.locator(".graphiql-sidebar");
  const buttons = sidebar.locator("button");
  const count = await buttons.count();

  for (let i = 0; i < count; i++) {
    const label = await buttons.nth(i).getAttribute("aria-label");
    if (label?.toLowerCase().includes("provisa")) {
      await buttons.nth(i).click();
      break;
    }
  }

  const panel = page.locator(".provisa-tools");
  if (!(await panel.isVisible({ timeout: 3000 }).catch(() => false))) {
    for (let i = 0; i < count; i++) {
      await buttons.nth(i).click();
      if (await panel.isVisible({ timeout: 500 }).catch(() => false)) break;
    }
  }
  await expect(panel).toBeVisible({ timeout: 5000 });
}

async function setGraphiQLQuery(page: import("@playwright/test").Page, query: string) {
  // GraphiQL 5 uses Monaco Editor (not CM6). Paste via clipboard to avoid
  // Monaco's close-brackets extension firing on individual keystrokes.
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  // Click the editor container to focus Monaco, then paste
  const editorArea = page.locator(".graphiql-query-editor").first();
  await editorArea.waitFor({ state: "visible", timeout: 15000 });
  await editorArea.click();
  await page.evaluate((text: string) => navigator.clipboard.writeText(text), query);
  await page.keyboard.press("ControlOrMeta+a");
  await page.keyboard.press("ControlOrMeta+v");
  await page.waitForTimeout(300);
}

async function runGraphiQLQuery(page: import("@playwright/test").Page) {
  await page
    .locator(
      '[aria-label="Run query (Ctrl-Enter)"], [aria-label="Execute query (Ctrl-Enter)"], button.graphiql-execute-button',
    )
    .first()
    .click();
}

// Wait for the Provisa Tools SQL panel to show compiled SQL text
async function waitForCompiledSql(page: import("@playwright/test").Page) {
  // The SQL panel header contains "SQL" or "Semantic SQL"
  await expect(page.locator(".provisa-tools-sql").first()).toBeVisible({ timeout: 30000 });
}

// Wait for the Provisa Tools Cypher panel to show compiled Cypher
async function waitForCompiledCypher(page: import("@playwright/test").Page) {
  await expect(page.locator(".provisa-tools-cypher").first()).toBeVisible({ timeout: 30000 });
}

// ── SQL push flow ─────────────────────────────────────────────────────────────

test.describe("push to SQL explorer — 'Open in SQL' button", () => {
  test.describe.configure({ timeout: 90_000 });

  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });
    await dismissOverlay(page);
    // Allow schema introspection to complete
    await page.waitForTimeout(2000);
  });

  // Shared setup: set query, open Provisa panel, run query, wait for compiled SQL
  async function setupSql(page: import("@playwright/test").Page) {
    await setGraphiQLQuery(page, PETS_QUERY);
    await openProviasPanel(page);
    await runGraphiQLQuery(page);
    await waitForCompiledSql(page);
  }

  test("navigates to /sql after clicking Open in SQL", async ({ page }) => {
    await setupSql(page);

    const openSqlBtn = page.locator('button[title="Open in SQL"]').first();
    await expect(openSqlBtn).toBeVisible({ timeout: 10000 });
    await openSqlBtn.click();

    await expect(page).toHaveURL(/\/sql/, { timeout: 10000 });
  });

  test("SQL page auto-runs and shows a results table after push", async ({ page }) => {
    await setupSql(page);

    const openSqlBtn = page.locator('button[title="Open in SQL"]').first();
    await expect(openSqlBtn).toBeVisible({ timeout: 10000 });
    await openSqlBtn.click();

    await expect(page).toHaveURL(/\/sql/, { timeout: 10000 });
    // Auto-run fires; wait for the results table to appear
    const resultsTable = page.locator(".sql-results-table").first();
    await expect(resultsTable).toBeVisible({ timeout: 60000 });
  });

  test("SQL page results table contains at least one data row after push", async ({ page }) => {
    await setupSql(page);

    const openSqlBtn = page.locator('button[title="Open in SQL"]').first();
    await expect(openSqlBtn).toBeVisible({ timeout: 10000 });
    await openSqlBtn.click();

    await expect(page).toHaveURL(/\/sql/, { timeout: 10000 });
    const resultsTable = page.locator(".sql-results-table").first();
    await expect(resultsTable).toBeVisible({ timeout: 60000 });
    const rows = resultsTable.locator("tbody tr");
    await expect(rows.first()).toBeVisible({ timeout: 10000 });
  });

  test("SQL editor is populated with the compiled query after push", async ({ page }) => {
    await setupSql(page);

    const openSqlBtn = page.locator('button[title="Open in SQL"]').first();
    await expect(openSqlBtn).toBeVisible({ timeout: 10000 });
    await openSqlBtn.click();

    await expect(page).toHaveURL(/\/sql/, { timeout: 10000 });
    const editor = page.locator(".cm-content").first();
    await expect(editor).toBeVisible({ timeout: 10000 });
    const content = await editor.textContent();
    // The compiled query targets the pets table
    expect(content?.toLowerCase()).toMatch(/pets|select/);
  });
});

// ── Cypher push flow ──────────────────────────────────────────────────────────

test.describe("push to graph explorer — 'Open in Graph' button", () => {
  test.describe.configure({ timeout: 90_000 });

  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });
    await dismissOverlay(page);
    await page.waitForTimeout(2000);
  });

  // Shared setup: set query, open Provisa panel, run query, wait for compiled Cypher
  async function setupCypher(page: import("@playwright/test").Page) {
    await setGraphiQLQuery(page, PETS_QUERY);
    await openProviasPanel(page);
    await runGraphiQLQuery(page);
    await waitForCompiledCypher(page);
  }

  test("navigates to /graph after clicking Open in Graph", async ({ page }) => {
    await setupCypher(page);

    const openGraphBtn = page.locator('button[title="Open in Graph"]').first();
    await expect(openGraphBtn).toBeVisible({ timeout: 10000 });
    await openGraphBtn.click();

    await expect(page).toHaveURL(/\/graph/, { timeout: 10000 });
  });

  test("graph page auto-runs and renders a canvas after push", async ({ page }) => {
    await setupCypher(page);

    const openGraphBtn = page.locator('button[title="Open in Graph"]').first();
    await expect(openGraphBtn).toBeVisible({ timeout: 10000 });
    await openGraphBtn.click();

    await expect(page).toHaveURL(/\/graph/, { timeout: 10000 });
    // Graph sidebar must load before the canvas renders
    await page.waitForSelector(".graph-sidebar", { timeout: 20000 });
    const canvas = page.locator(".gf-canvas, .gf-canvas-wrap").first();
    await expect(canvas).toBeVisible({ timeout: 60000 });
  });

  test("graph query bar is populated with the compiled Cypher after push", async ({ page }) => {
    await setupCypher(page);

    const openGraphBtn = page.locator('button[title="Open in Graph"]').first();
    await expect(openGraphBtn).toBeVisible({ timeout: 10000 });
    await openGraphBtn.click();

    await expect(page).toHaveURL(/\/graph/, { timeout: 10000 });
    await page.waitForSelector(".graph-query-input", { timeout: 15000 });
    const queryBar = page.locator(".graph-query-input");
    const content = await queryBar.textContent();
    // The compiled Cypher uses MATCH and references Pets
    expect(content).toMatch(/MATCH/i);
    expect(content).toMatch(/Pets/i);
  });
});
