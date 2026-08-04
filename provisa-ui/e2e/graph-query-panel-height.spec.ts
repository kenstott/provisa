// Copyright (c) 2026 Kenneth Stott
// Canary: 9225e698-b5da-4cee-90f9-dda5624fba37
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect, BACKEND_URL } from "./coverage";

// Three-line Cypher against PetStore:Inquiries only — the test exercises LAYOUT (line count /
// clipping), not data.  Inquiries is warmed in global-setup; avoiding the Pets OPTIONAL MATCH
// prevents a Pets cold-start from blocking DuckDB and starving concurrent workers.
const MULTI_LINE_QUERY =
  "MATCH (n:PetStore:Inquiries)\nWHERE n.id IS NOT NULL\nRETURN n LIMIT 5";

test("query panel expands to show all lines without clipping", async ({ page }) => {
  // 360s: concurrent sharepoint/splunk tests can block the backend for ~170s during Trino
  // catalog init, stalling the pre-warm POST; 360s gives ample headroom above that.
  test.setTimeout(360000);
  // Pre-warm DuckDB for PetStore:Inquiries in case the server restarted mid-suite
  // (uvicorn --reload restarts cold DuckDB; this ensures the cache is hot before the graph query).
  await page.request.post(`${BACKEND_URL}/data/cypher`, {
    data: { query: "MATCH (n:PetStore:Inquiries) RETURN n LIMIT 5", params: {} },
  });
  await page.goto("/graph");

  // Inject a multi-line pending query so the frame appears on load
  await page.evaluate((q) => {
    localStorage.setItem("provisa.graph.pending_query", q);
  }, MULTI_LINE_QUERY);
  await page.reload();

  // The frame header starts in collapsed mode; click the collapsed display to open the editor.
  // Use .gf-query-editor-wrap to target GraphFrame's collapsed div — not QueryBar's
  // (QueryBar uses .graph-query-editor-wrap and .graph-query-input, different classes).
  const collapsedQuery = page.locator(".gf-query-editor-wrap .gf-header-query-collapsed").first();
  // 90s budget: concurrent DuckDB-heavy tests or a server restart (uvicorn --reload) can delay
  // auth GraphQL calls by up to ~60s; 90s leaves headroom for the assertions below within the
  // 120s test total.
  await expect(collapsedQuery).toBeVisible({ timeout: 90000 });
  await collapsedQuery.click();

  const editorEl = page.locator(".gf-header-query-input .cm-editor").first();
  await expect(editorEl).toBeVisible({ timeout: 5000 });

  // The scroller must not be clipping content: scrollHeight === clientHeight
  const clipped = await editorEl.evaluate((el) => {
    const scroller = el.querySelector(".cm-scroller") as HTMLElement | null;
    if (!scroller) return { scrollHeight: -1, clientHeight: -1, clipped: true };
    return {
      scrollHeight: scroller.scrollHeight,
      clientHeight: scroller.clientHeight,
      clipped: scroller.scrollHeight > scroller.clientHeight,
    };
  });

  console.log("scroller dimensions:", clipped);
  expect(clipped.clipped, `cm-scroller clipped: scrollHeight=${clipped.scrollHeight} clientHeight=${clipped.clientHeight}`).toBe(false);

  // Also verify all 3 query lines are rendered as CodeMirror lines
  const lineCount = await editorEl.evaluate((el) => el.querySelectorAll(".cm-line").length);
  console.log("cm-line count:", lineCount);
  expect(lineCount).toBeGreaterThanOrEqual(3);

  // Header must visually contain the editor — editor bottom <= header bottom
  const headerBox = await page.locator(".gf-header").first().boundingBox();
  const editorBox = await editorEl.boundingBox();
  console.log("header bottom:", headerBox && (headerBox.y + headerBox.height), "editor bottom:", editorBox && (editorBox.y + editorBox.height));
  expect(headerBox).not.toBeNull();
  expect(editorBox).not.toBeNull();
  expect(editorBox!.y + editorBox!.height).toBeLessThanOrEqual(headerBox!.y + headerBox!.height + 2);
});

test("query panel in expanded modal shows all lines without clipping", async ({ page }) => {
  // 360s: same reason as test 1 — sharepoint/splunk backend blocking can consume ~170s.
  test.setTimeout(360000);
  // Pre-warm DuckDB for PetStore:Inquiries in case the server restarted mid-suite
  // (uvicorn --reload restarts cold DuckDB; this ensures the cache is hot before the graph query).
  await page.request.post(`${BACKEND_URL}/data/cypher`, {
    data: { query: "MATCH (n:PetStore:Inquiries) RETURN n LIMIT 5", params: {} },
  });
  await page.goto("/graph");

  await page.evaluate((q) => {
    localStorage.setItem("provisa.graph.pending_query", q);
  }, MULTI_LINE_QUERY);
  await page.reload();

  // The frame header starts in collapsed mode; click to open the editor before expanding.
  // Use .gf-query-editor-wrap to target GraphFrame's collapsed div — not QueryBar's
  // (QueryBar uses .graph-query-editor-wrap and .graph-query-input, different classes).
  const collapsedQuery = page.locator(".gf-query-editor-wrap .gf-header-query-collapsed").first();
  // 90s budget: same reason as test 1 — DuckDB load / server restart can delay auth up to ~60s.
  await expect(collapsedQuery).toBeVisible({ timeout: 90000 });
  await collapsedQuery.click();
  const inlineEditor = page.locator(".gf-header-query-input .cm-editor").first();
  await expect(inlineEditor).toBeVisible({ timeout: 5000 });

  // Now open the modal; queryFocused=true carries into the modal header so the editor is visible
  const expandBtn = page.locator(".gf-icon-btn[title='Expand']").first();
  await expect(expandBtn).toBeVisible({ timeout: 5000 });
  await expandBtn.click();

  // Modal overlay should be visible
  const modalFrame = page.locator(".gf-modal-frame");
  await expect(modalFrame).toBeVisible({ timeout: 5000 });

  const editorEl = modalFrame.locator(".gf-header-query-input .cm-editor");
  await expect(editorEl).toBeVisible();

  const clipped = await editorEl.evaluate((el) => {
    const scroller = el.querySelector(".cm-scroller") as HTMLElement | null;
    if (!scroller) return { scrollHeight: -1, clientHeight: -1, clipped: true };
    return {
      scrollHeight: scroller.scrollHeight,
      clientHeight: scroller.clientHeight,
      clipped: scroller.scrollHeight > scroller.clientHeight,
    };
  });

  console.log("modal scroller dimensions:", clipped);
  expect(clipped.clipped, `modal cm-scroller clipped: scrollHeight=${clipped.scrollHeight} clientHeight=${clipped.clientHeight}`).toBe(false);

  // Modal frame (header) bottom must not overlap the expanded body frame top
  const modalFrameBox = await page.locator(".gf-modal-frame").boundingBox();
  const expandedFrameBox = await page.locator(".gf-frame.gf-expanded").boundingBox();
  console.log("modal header bottom:", modalFrameBox && (modalFrameBox.y + modalFrameBox.height), "body top:", expandedFrameBox?.y);
  expect(modalFrameBox).not.toBeNull();
  expect(expandedFrameBox).not.toBeNull();
  expect(expandedFrameBox!.y).toBeGreaterThanOrEqual(modalFrameBox!.y + modalFrameBox!.height - 2);
});
