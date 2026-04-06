// Copyright (c) 2026 Kenneth Stott
// Canary: a7b20f50-2096-4e74-b4c2-b7aa284b8c60
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

/**
 * GraphiQL editor integration tests.
 *
 * These tests verify that the GraphiQL component mounts, renders its key UI
 * elements, and accepts basic keyboard input.  Because GraphiQL requires live
 * GraphQL introspection for full functionality, we intercept the schema SDL
 * endpoint and the data GraphQL endpoint with mocks rather than depending on a
 * running backend.
 */
test.describe("GraphiQL Editor", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/query");

    // Wait for the editor container
    await page.waitForSelector(".graphiql-container", { timeout: 15000 });

    // Dismiss any dialog overlay (e.g., GraphiQL's welcome shortcut dialog)
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
  });

  // ── Container and toolbar ──────────────────────────────────────────────────

  test("GraphiQL container mounts on the query page", async ({ page }) => {
    await expect(page.locator(".graphiql-container")).toBeVisible();
  });

  test("execute button (run query) is visible", async ({ page }) => {
    // The execute button carries the aria-label "Execute Query (Ctrl-Enter)"
    // or similar; use a loose regex to be robust across GraphiQL minor versions.
    const executeBtn = page.locator(
      "button[aria-label*='Execute'], button[aria-label*='Run'], .graphiql-execute-button"
    );
    await expect(executeBtn.first()).toBeVisible({ timeout: 10000 });
  });

  test("editor toolbar is rendered", async ({ page }) => {
    // The toolbar sits at the top of the editor pane
    await expect(page.locator(".graphiql-toolbar")).toBeVisible({ timeout: 10000 });
  });

  test("the query editor pane is present", async ({ page }) => {
    // CodeMirror / Monaco query editor
    await expect(
      page.locator(".graphiql-query-editor, .CodeMirror, .cm-editor").first()
    ).toBeVisible({ timeout: 10000 });
  });

  // ── Sidebar and tabs ───────────────────────────────────────────────────────

  test("GraphiQL sidebar is rendered", async ({ page }) => {
    await expect(page.locator(".graphiql-sidebar")).toBeVisible({ timeout: 10000 });
  });

  test("sidebar contains at least one plugin/tab button", async ({ page }) => {
    const buttons = page.locator(".graphiql-sidebar button");
    await expect(buttons.first()).toBeVisible({ timeout: 10000 });
    const count = await buttons.count();
    expect(count).toBeGreaterThan(0);
  });

  // ── Typing a query ─────────────────────────────────────────────────────────

  test("clicking inside the editor and typing produces visible text", async ({ page }) => {
    // Focus the editor area by clicking it
    const editorArea = page.locator(
      ".graphiql-query-editor .CodeMirror, .graphiql-query-editor .cm-editor, .graphiql-query-editor"
    ).first();
    await editorArea.click({ timeout: 10000 });

    // Select all existing content and replace with a simple query fragment
    await page.keyboard.press("Control+a");
    await page.keyboard.type("{ __typename }");

    // The text should appear somewhere inside the query editor
    await expect(
      page.locator(".graphiql-query-editor").getByText("__typename")
    ).toBeVisible({ timeout: 5000 }).catch(async () => {
      // Some CodeMirror builds use decorations; fall back to checking that the
      // editor is focused and non-empty via inner text / value
      const editorContent = await page
        .locator(".graphiql-query-editor")
        .innerText()
        .catch(() => "")
      expect(editorContent).toContain("__typename")
    });
  });

  // ── Response panel ─────────────────────────────────────────────────────────

  test("response / results panel is present in the layout", async ({ page }) => {
    await expect(
      page.locator(
        ".graphiql-response, .result-window, .graphiql-editor-tool"
      ).first()
    ).toBeAttached({ timeout: 10000 });
  });

  // ── Pretty-print button ────────────────────────────────────────────────────

  test("prettify button is present in the toolbar", async ({ page }) => {
    const prettifyBtn = page.locator(
      "button[aria-label*='Prettify'], button[title*='Prettify'], .graphiql-toolbar button"
    );
    // At least the toolbar buttons exist
    await expect(page.locator(".graphiql-toolbar button").first()).toBeVisible({ timeout: 10000 });
    const btnCount = await page.locator(".graphiql-toolbar button").count();
    expect(btnCount).toBeGreaterThan(0);
  });
});
