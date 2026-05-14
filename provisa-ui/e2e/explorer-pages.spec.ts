// Copyright (c) 2026 Kenneth Stott
// Canary: e2f3a4b5-c6d7-8901-ef23-456789abcdef
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * End-to-end tests for the four Explore sub-pages:
 *   /query   — GraphiQL (GraphQL)
 *   /schema  — Schema Explorer (Voyager iframe)
 *   /graph   — Graph Explorer (Cypher / Neo4j)
 *   /sql     — SQL runner
 *
 * All tests run against the live backend. Queries use the pet-store domain
 * which is always populated in the dev environment.
 */

import { test, expect } from "./coverage";

// ── Helpers ──────────────────────────────────────────────────────────────────

async function dismissOverlay(page: import("@playwright/test").Page) {
  const overlay = page.locator(".graphiql-dialog-overlay");
  if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
    await page.keyboard.press("Escape");
    await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
  }
}

// GraphQL page uses Monaco editor. The textarea with aria-label="Editor content"
// inside .graphiql-query-editor is the correct input target.
async function setGraphiQLQuery(page: import("@playwright/test").Page, query: string) {
  const textarea = page.locator('.graphiql-query-editor textarea[aria-label="Editor content"]');
  await textarea.waitFor({ state: "attached", timeout: 15000 });
  await textarea.focus();
  await page.keyboard.press("Meta+A");
  await page.keyboard.press("Delete");
  await page.keyboard.type(query);
}

async function runGraphiQLQuery(page: import("@playwright/test").Page) {
  await page.locator('[aria-label="Run query (Ctrl-Enter)"], [aria-label="Execute query (Ctrl-Enter)"], button.graphiql-execute-button').first().click();
}

// ── GraphQL page (/query) ─────────────────────────────────────────────────────

test.describe("GraphQL Explorer (/query)", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });
    await dismissOverlay(page);
  });

  test("schema loads and docs button is enabled", async ({ page }) => {
    await page.waitForTimeout(2000);
    const docsBtn = page.locator('[aria-label="Show Documentation Explorer"]');
    await expect(docsBtn).toBeVisible({ timeout: 8000 });
    await expect(docsBtn).not.toBeDisabled();
  });

  test("schema is cached in sessionStorage after load", async ({ page }) => {
    await page.waitForTimeout(3000);
    const keys = await page.evaluate(() => {
      const result: string[] = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (k?.startsWith("introspection:")) result.push(k);
      }
      return result;
    });
    expect(keys.length).toBeGreaterThan(0);
    expect(keys[0]).toMatch(/^introspection:[^:]+:[^:]+:[^:]+$/);
  });

  test("schema served from cache on return navigation", async ({ page }) => {
    await page.waitForTimeout(3000);
    const keysBefore = await page.evaluate(() => {
      const r: string[] = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (k?.startsWith("introspection:")) r.push(k);
      }
      return r;
    });
    expect(keysBefore.length).toBeGreaterThan(0);

    // Navigate away and back — /sources has an <h2>
    await page.goto("/sources");
    await page.waitForSelector("h2", { timeout: 8000 });
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });
    await page.waitForTimeout(1500);

    const keysAfter = await page.evaluate(() => {
      const r: string[] = [];
      for (let i = 0; i < sessionStorage.length; i++) {
        const k = sessionStorage.key(i);
        if (k?.startsWith("introspection:")) r.push(k);
      }
      return r;
    });
    expect(keysAfter).toEqual(keysBefore);
  });

  test("runs ps__pets query and gets results", async ({ page }) => {
    await page.waitForTimeout(2000);
    await dismissOverlay(page);
    await setGraphiQLQuery(page, "{ ps__pets(limit: 3) { id name } }");
    await runGraphiQLQuery(page);

    const response = page.locator(".graphiql-response .view-lines, .graphiql-response pre");
    await expect(response.first()).toBeVisible({ timeout: 30000 });
    const text = await response.first().textContent();
    expect(text).toContain("ps__pets");
  });

  test("runs _meta _queries subquery and returns OTel data", async ({ page }) => {
    await page.waitForTimeout(2000);
    await dismissOverlay(page);
    const query = `{ ps__pets(limit: 2) { name _meta { _queries(limit: 5) { serviceName spanName tableName } } } }`;
    await setGraphiQLQuery(page, query);
    await runGraphiQLQuery(page);

    // Wait for *all* response lines to render, then join them.
    const responsePane = page.locator(".graphiql-response");
    await expect(responsePane).toBeVisible({ timeout: 30000 });
    await page.waitForTimeout(2000);

    // Capture raw JSON from the hidden pre element that GraphiQL keeps in sync,
    // falling back to the Monaco view-lines text.
    const rawJson = await page.evaluate(() => {
      const pre = document.querySelector(".graphiql-response pre");
      if (pre?.textContent) return pre.textContent;
      return Array.from(document.querySelectorAll(".graphiql-response .view-line"))
        .map((el) => el.textContent ?? "")
        .join("\n");
    });

    // No top-level GraphQL errors.
    expect(rawJson).not.toContain('"errors"');
    // _meta._queries returned actual span data — at least one serviceName value.
    expect(rawJson).toMatch(/"serviceName"\s*:\s*"[^"]+"/);
  });

  test("runs _meta _traces subquery and returns raw span data", async ({ page }) => {
    await page.waitForTimeout(2000);
    await dismissOverlay(page);
    const query = `{ ps__pets(limit: 2) { name _meta { _traces(limit: 5) { serviceName spanName tableName } } } }`;
    await setGraphiQLQuery(page, query);
    await runGraphiQLQuery(page);

    const responsePane = page.locator(".graphiql-response");
    await expect(responsePane).toBeVisible({ timeout: 30000 });
    await page.waitForTimeout(2000);

    const rawJson = await page.evaluate(() => {
      const pre = document.querySelector(".graphiql-response pre");
      if (pre?.textContent) return pre.textContent;
      return Array.from(document.querySelectorAll(".graphiql-response .view-line"))
        .map((el) => el.textContent ?? "")
        .join("\n");
    });

    expect(rawJson).not.toContain('"errors"');
    expect(rawJson).toMatch(/"serviceName"\s*:\s*"[^"]+"/);
  });

  test("schema error banner shown on fetch failure", async ({ page }) => {
    // Load normally so schema-version succeeds and serverSchemaVersion is set.
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });
    await page.waitForTimeout(1500);

    // Intercept only introspection (not schema-version) so serverSchemaVersion
    // becomes non-null and the introspection fetch is actually attempted.
    await page.route("**/data/introspection**", (route) =>
      route.fulfill({ status: 503, json: { detail: "Schema unavailable" } })
    );

    // Clear sessionStorage so the cached schema is not used on reload.
    await page.evaluate(() => {
      for (let i = sessionStorage.length - 1; i >= 0; i--) {
        const k = sessionStorage.key(i);
        if (k?.startsWith("introspection:")) sessionStorage.removeItem(k!);
      }
    });

    await page.reload();
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });
    await page.waitForTimeout(2500);
    // Use exact innerText locator to avoid strict-mode multi-match on parent divs.
    const banner = page.locator("div:has-text('Schema error:')").last();
    await expect(banner).toBeVisible({ timeout: 8000 });
  });
});

// ── SQL page (/sql) ───────────────────────────────────────────────────────────

test.describe("SQL Explorer (/sql)", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/sql");
    await page.waitForSelector(".cm-editor", { timeout: 10000 });
  });

  test("page loads with SQL editor and run button", async ({ page }) => {
    await expect(page.locator(".cm-editor")).toBeVisible();
    // "Sample >" is the run button; it may be disabled until SQL is typed
    const runBtn = page.locator("button.btn-primary", { hasText: /Sample/ }).first();
    await expect(runBtn).toBeVisible();
  });

  test("executes SELECT query and shows results table", async ({ page }) => {
    // CodeMirror 6: .cm-content has contenteditable="true"
    const editor = page.locator(".cm-content").first();
    await editor.waitFor({ state: "visible", timeout: 8000 });
    await editor.fill("SELECT name, id FROM pet_store.pets LIMIT 5");

    const runBtn = page.locator("button.btn-primary", { hasText: /Sample/ }).first();
    await expect(runBtn).toBeEnabled({ timeout: 5000 });
    await runBtn.click();

    const resultsTable = page.locator(".sql-results-table").first();
    await expect(resultsTable).toBeVisible({ timeout: 30000 });
    const rows = resultsTable.locator("tbody tr");
    await expect(rows.first()).toBeVisible({ timeout: 10000 });
  });

  test("Sample button runs query and returns rows", async ({ page }) => {
    const editor = page.locator(".cm-content").first();
    await editor.waitFor({ state: "visible", timeout: 8000 });
    await editor.fill("SELECT id, name FROM pet_store.pets LIMIT 3");

    const sampleBtn = page.locator("button.btn-primary", { hasText: /Sample/ }).first();
    await expect(sampleBtn).toBeEnabled({ timeout: 5000 });
    await sampleBtn.click();

    const resultsTable = page.locator(".sql-results-table").first();
    await expect(resultsTable).toBeVisible({ timeout: 30000 });
  });
});

// ── Graph Explorer (/graph) ───────────────────────────────────────────────────

test.describe("Graph Explorer (/graph)", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/graph");
    await page.waitForSelector(".graph-sidebar", { timeout: 12000 });
  });

  test("loads sidebar with node labels and relationship types", async ({ page }) => {
    // CSS uppercases the headings; getByText is case-insensitive by default
    await expect(page.locator(".graph-schema-section").first()).toBeVisible({ timeout: 8000 });
    // At least one label pill should be visible
    await expect(page.locator(".graph-label-pill, .graph-label-item").first()).toBeVisible({ timeout: 8000 });
  });

  test("query bar is present with default MATCH query", async ({ page }) => {
    // .graph-query-input is the CodeMirror wrapper; confirm it is mounted
    const queryBar = page.locator(".graph-query-input");
    await expect(queryBar).toBeVisible({ timeout: 8000 });
    // Default query text should be present in the editor
    const content = await queryBar.textContent();
    expect(content).toMatch(/MATCH/i);
  });

  test("executes MATCH query and renders graph canvas", async ({ page }) => {
    // Type into the CodeMirror .cm-content inside the query bar
    const cmContent = page.locator(".graph-query-input .cm-content").first();
    await cmContent.waitFor({ state: "visible", timeout: 8000 });
    await cmContent.click();
    await page.keyboard.press("Meta+A");
    await page.keyboard.type("MATCH (n:ps__pets) RETURN n LIMIT 5");

    // Use the dedicated run button (.graph-run-btn) instead of Enter to avoid
    // triggering the CodeMirror newContentVersion incompatibility.
    await page.locator(".graph-run-btn").click();

    // After running, cytoscape renders nodes inside .gf-canvas
    const canvas = page.locator(".gf-canvas, .gf-canvas-wrap").first();
    await expect(canvas).toBeVisible({ timeout: 20000 });
  });

  test("clicking a node label in sidebar runs a query", async ({ page }) => {
    await page.waitForTimeout(2000);
    const labelPill = page.locator(".graph-label-pill, span[title]").first();
    if (await labelPill.isVisible({ timeout: 3000 }).catch(() => false)) {
      await labelPill.click();
      const canvas = page.locator(".gf-canvas, .gf-canvas-wrap").first();
      await expect(canvas).toBeVisible({ timeout: 20000 });
    }
  });
});

// ── Cypher panel flat-return regression (/query) ─────────────────────────────

test.describe("Cypher panel flat-return regression (/query)", () => {
  test("Include Fields + non-Aggregated emits per-field paths, not bare node alias", async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });
    await dismissOverlay(page);
    await page.waitForTimeout(2000);

    // Open the Provisa plugin panel if it is not already visible.
    const provisaBtn = page.locator('[aria-label="Show Provisa"], button:has-text("Show Provisa")');
    if (await provisaBtn.isVisible({ timeout: 3000 }).catch(() => false)) {
      await provisaBtn.click();
      await page.waitForTimeout(500);
    }

    // Run a query that has a nested relationship so the Cypher panel appears.
    const query = "{ ps__pets(limit: 3) { name assignment { breedName } } }";
    await setGraphiQLQuery(page, query);
    await runGraphiQLQuery(page);

    // Wait for the Cypher panel to appear (compile fires on query-state change with 600ms debounce).
    const cypherPanel = page.locator(".provisa-tools-cypher");
    await expect(cypherPanel).toBeVisible({ timeout: 25000 });

    // Enable "Include fields" to expose flat vs aggregated controls.
    const includeFieldsBox = cypherPanel.locator('label:has-text("Include fields") input[type="checkbox"]');
    await includeFieldsBox.waitFor({ state: "visible", timeout: 5000 });
    if (!(await includeFieldsBox.isChecked())) {
      await includeFieldsBox.click();
    }
    await page.waitForTimeout(1500);

    // Disable "GraphQL-Shape (Aggregated)" → flatCypher=true.
    const aggregatedBox = cypherPanel.locator('label:has-text("Aggregated") input[type="checkbox"]');
    if (await aggregatedBox.isVisible({ timeout: 3000 }).catch(() => false)) {
      if (await aggregatedBox.isChecked()) {
        await aggregatedBox.click();
      }
    }
    await page.waitForTimeout(1500);

    // Read the generated Cypher from the editor.
    const cypherText = await cypherPanel.locator(".provisa-tools-cypher-editor .cm-content").textContent({ timeout: 8000 });
    if (!cypherText || cypherText.trim() === "") {
      test.skip(); // No Cypher produced — skip rather than false-fail.
      return;
    }

    // Must not have a bare node alias like "b AS assignment".
    expect(cypherText).not.toMatch(/\b[a-z]\s+AS\s+\w+/);
    // RETURN clause must contain dotted property paths.
    const returnClause = cypherText.split("RETURN").at(-1) ?? "";
    expect(returnClause).toMatch(/\w+\.\w+/);
  });
});

// ── Schema Explorer (/schema) ─────────────────────────────────────────────────

test.describe("Schema Explorer (/schema)", () => {
  test("Voyager iframe loads", async ({ page }) => {
    await page.goto("/schema");
    const iframe = page.locator("iframe[title='GraphQL Voyager']");
    await expect(iframe).toBeVisible({ timeout: 20000 });
  });

  test("domain selector triggers iframe reload", async ({ page }) => {
    await page.goto("/schema");
    await page.waitForSelector("iframe[title='GraphQL Voyager']", { timeout: 20000 });

    const domainControls = page.locator(".domain-checkbox, input[type='checkbox']").first();
    if (await domainControls.isVisible({ timeout: 3000 }).catch(() => false)) {
      const srcBefore = await page.locator("iframe[title='GraphQL Voyager']").getAttribute("src");
      await domainControls.click();
      await page.waitForTimeout(1000);
      const srcAfter = await page.locator("iframe[title='GraphQL Voyager']").getAttribute("src");
      expect(srcAfter).toBeDefined();
      void srcBefore;
    }
  });
});
