// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

/**
 * Verifies that auto-impute adds edges between nodes returned from a node-only query.
 * Regression for: imputed edge startNode/endNode ids using raw PK instead of compound
 * "{label}|{pk}" format, causing cy.$id(srcKey) lookups to fail and no edges to render.
 */
test("auto-impute fires and returns compound-id edges for meta node query", async ({ page }) => {
  await page.goto("http://localhost:3000/graph");

  // Intercept impute responses before anything fires
  const imputeResponsePromise = page.waitForResponse(
    (resp) =>
      resp.url().includes("/data/impute-relationships") && resp.status() === 200,
    { timeout: 30000 }
  );

  // Enable auto-impute via the QueryBar toggle (always visible, no tab needed)
  await page.locator('button[title="Auto-impute relationships between visible nodes"]').click();

  // Click into the CodeMirror editor, select all, replace with test query
  const editor = page.locator(".cm-content").first();
  await editor.click();
  await page.keyboard.press("Meta+a");
  await page.keyboard.type("MATCH (n:Meta) RETURN n LIMIT 50");

  // Run the query (⌘↵)
  await page.keyboard.press("Meta+Enter");

  // Wait for the frame to finish loading and show nodes
  await expect(page.locator(".gf-meta-text")).toContainText(/\d+ nodes/, {
    timeout: 15000,
  });

  // Wait for the impute API to fire and respond
  const imputeResp = await imputeResponsePromise;
  const imputeBody = await imputeResp.json();

  const edges = ((imputeBody.rows ?? []) as Array<{ node: Record<string, unknown> }>)
    .map((r) => r.node)
    .filter((n) => "identity" in n && "startNode" in n);

  // Edges must be returned (meta schema has relationships between its tables)
  expect(edges.length, "impute returned 0 edges for Meta nodes").toBeGreaterThan(0);

  // startNode/endNode ids must be compound "{label}|{pk}" so canvas lookup works
  for (const e of edges) {
    const sn = e.startNode as { id: string; label: string };
    const en = e.endNode as { id: string; label: string };
    expect(sn.id).toMatch(new RegExp(`^${sn.label}\\|`));
    expect(en.id).toMatch(new RegExp(`^${en.label}\\|`));
  }
});

/**
 * Verifies that the /data/impute-relationships endpoint returns edges with
 * compound startNode/endNode ids matching the canvas node id format.
 */
test("impute-relationships API returns compound startNode/endNode ids", async ({ request }) => {
  // Get a few Meta nodes from a real query first
  const queryResp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: "MATCH (n:Meta) RETURN n LIMIT 10" },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(queryResp.status()).toBe(200);
  const queryBody = await queryResp.json();
  const nodes = (queryBody.rows as Array<{ n: { id: string; label: string } }>).map((r) => ({
    id: r.n.id,
    label: r.n.label,
  }));

  // All node ids should be in compound "{label}|{pk}" format
  for (const n of nodes) {
    expect(n.id).toMatch(/^.+\|.+$/);
  }

  // Now impute with all 10 nodes (using compound ids as the frontend would send)
  const imputeResp = await request.post("http://localhost:8000/data/impute-relationships", {
    data: { nodes },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(imputeResp.status()).toBe(200);
  const imputeBody = await imputeResp.json();
  const rows: Array<{ node: Record<string, unknown> }> = imputeBody.rows ?? [];
  const edges = rows
    .map((r) => r.node)
    .filter((n) => "identity" in n && "startNode" in n);

  // If any edges were returned, verify startNode/endNode ids are compound
  for (const e of edges) {
    const sn = e.startNode as { id: string; label: string };
    const en = e.endNode as { id: string; label: string };
    expect(sn.id, `startNode.id should be compound for edge ${e.identity}`).toMatch(/^.+\|.+$/);
    expect(en.id, `endNode.id should be compound for edge ${e.identity}`).toMatch(/^.+\|.+$/);
    expect(sn.id).toMatch(new RegExp(`^${sn.label}\\|`));
    expect(en.id).toMatch(new RegExp(`^${en.label}\\|`));
  }
});
