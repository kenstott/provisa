// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

test("code view cm-scroller is scrollable when content exceeds frame height", async ({ page }) => {
  await page.goto("/graph");

  // Seed a successful frame with rich queryStats so code view JSON is tall enough to scroll
  const longSql = Array.from({ length: 30 }, (_, i) =>
    `SELECT n${i}.id, n${i}.name, n${i}.label, n${i}.created_at, n${i}.updated_at FROM schema_${i}.nodes n${i} WHERE n${i}.active = true AND n${i}.domain_id IN (1,2,3) ORDER BY n${i}.id LIMIT 50`
  ).join("\nUNION ALL\n");
  const queryStats = {
    total_elapsed_ms: 963,
    sources: Array.from({ length: 20 }, (_, i) => ({
      field: "cypher",
      source: `trino_${i}`,
      strategy: "federated",
      elapsed_ms: 40 + i,
      rows: 70,
      physical_sql: longSql,
    })),
  };
  const frameState = {
    frames: [{
      id: "scroll-test-frame",
      query: "MATCH (n) RETURN n LIMIT 50",
      status: "success",
      nodes: [],
      edges: [],
      rows: [{ n: { id: 1 } }],
      columns: ["n"],
      elapsed: 963,
      queryStats,
      pinned: false,
    }],
    history: ["MATCH (n) RETURN n LIMIT 50"],
    currentQuery: "MATCH (n) RETURN n LIMIT 50",
  };
  await page.evaluate((state) => {
    localStorage.setItem("provisa.graph.state", JSON.stringify(state));
  }, frameState);
  await page.reload();

  // Wait for Code view button to appear
  const codeBtn = page.locator(".gf-view-bar-btn[title='Code']").first();
  await expect(codeBtn).toBeVisible({ timeout: 15000 });
  await codeBtn.click();

  await page.waitForTimeout(150);

  const scroller = page.locator(".gf-json-view .cm-scroller").first();
  await expect(scroller).toBeVisible({ timeout: 5000 });

  const state = await scroller.evaluate((el) => ({
    scrollTop: el.scrollTop,
    scrollHeight: el.scrollHeight,
    clientHeight: el.clientHeight,
    canScroll: el.scrollHeight > el.clientHeight,
  }));

  console.log("scroller state:", state);
  expect(state.canScroll, `cm-scroller must overflow: scrollHeight=${state.scrollHeight} clientHeight=${state.clientHeight}`).toBe(true);

  // Wheel-scroll down and verify position changes
  await scroller.hover();
  await page.mouse.wheel(0, 300);
  await page.waitForTimeout(150);

  const scrollTop = await scroller.evaluate((el) => el.scrollTop);
  console.log("scrollTop after wheel:", scrollTop);
  expect(scrollTop, "scrollTop must increase after wheel scroll").toBeGreaterThan(0);
});
