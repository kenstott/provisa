// Copyright (c) 2026 Kenneth Stott
// Canary: 9225e698-b5da-4cee-90f9-dda5624fba37
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const MULTI_LINE_QUERY =
  "MATCH (a:Inquiries)\nOPTIONAL MATCH (a:Inquiries)-[:HAS_PETS]->(b:Pets)\nRETURN a, b LIMIT 10";

test("query panel expands to show all lines without clipping", async ({ page }) => {
  await page.goto("/graph");

  // Inject a multi-line pending query so the frame appears on load
  await page.evaluate((q) => {
    localStorage.setItem("provisa.graph.pending_query", q);
  }, MULTI_LINE_QUERY);
  await page.reload();

  // Wait for the frame header to appear
  const editorEl = page.locator(".gf-header-query-input .cm-editor").first();
  await expect(editorEl).toBeVisible({ timeout: 30000 });

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
  await page.goto("/graph");

  await page.evaluate((q) => {
    localStorage.setItem("provisa.graph.pending_query", q);
  }, MULTI_LINE_QUERY);
  await page.reload();

  // Wait for the frame to appear then open the modal
  const expandBtn = page.locator(".gf-icon-btn[title='Expand']").first();
  await expect(expandBtn).toBeVisible({ timeout: 30000 });
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
