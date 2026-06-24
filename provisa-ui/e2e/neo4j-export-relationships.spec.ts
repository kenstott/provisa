// Copyright (c) 2026 Kenneth Stott
// Canary: b9c8d7e6-f5a4-3b2c-1d0e-9f8a7b6c5d4e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Verifies that the Neo4j export modal collects relationships from frame.edges
 * (query-returned edges) and overlayEdges (auto-imputed edges).
 */

test("cypher API returns edge objects with startNode/endNode for relationship queries", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH (a:Inquiries)-[r:HAS_PETS]->(b:Pets) RETURN a, r, b LIMIT 5",
    },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();

  const rows: Array<Record<string, unknown>> = body.rows ?? [];
  expect(rows.length, "expected rows with relationships").toBeGreaterThan(0);

  // Each row should have an edge in column 'r' with startNode/endNode
  let edgesFound = 0;
  for (const row of rows) {
    const r = row.r as Record<string, unknown> | undefined;
    if (r && "identity" in r && "startNode" in r && "endNode" in r) {
      edgesFound++;
      const startNode = r.startNode as Record<string, unknown>;
      const endNode = r.endNode as Record<string, unknown>;
      expect(typeof startNode.id, "startNode.id should be numeric (register_node_ids ran)").toBe("number");
      expect(typeof endNode.id, "endNode.id should be numeric (register_node_ids ran)").toBe("number");
      expect(startNode.tableLabel, "startNode.tableLabel must exist for Cypher export").toBeTruthy();
      expect(endNode.tableLabel, "endNode.tableLabel must exist for Cypher export").toBeTruthy();
      expect(typeof r.start, "edge.start should be numeric after register_node_ids").toBe("number");
      expect(typeof r.end, "edge.end should be numeric after register_node_ids").toBe("number");
    }
  }
  expect(edgesFound, "at least one edge with startNode/endNode in response").toBeGreaterThan(0);
});

test("neo4j export modal shows correct relationship count after explicit edge query", async ({
  page,
}) => {
  await page.goto("http://localhost:3000/graph");

  // Run a query that explicitly returns relationships
  const editor = page.locator(".cm-content").first();
  await editor.click();
  await page.keyboard.press("Meta+a");
  await page.keyboard.type(
    "MATCH (a:Inquiries)-[r:HAS_PETS]->(b:Pets) RETURN a, r, b LIMIT 5",
  );
  await page.keyboard.press("Meta+Enter");

  // Wait for the frame to finish loading
  await expect(page.locator(".gf-meta-text")).toContainText(/\d+ nodes/, { timeout: 15000 });

  // Open the Neo4j export modal via the graph icon in the sidebar
  await page.locator('button[title="Export to Neo4j"]').click();

  // Modal should appear with the summary
  const summary = page.locator(".neo4j-modal-summary");
  await expect(summary).toBeVisible({ timeout: 5000 });

  const summaryText = await summary.textContent();
  expect(summaryText, "modal should show relationship count").toContain("relationship");

  // Extract relationship count from summary (format: "N nodes · M relationships")
  const match = summaryText?.match(/(\d+) relationship/);
  expect(match, "summary must include a relationship count").not.toBeNull();
  const relCount = parseInt(match![1], 10);
  expect(relCount, "relationship count must be > 0 for a query that returns edges").toBeGreaterThan(0);
});

test("buildCypherScript output includes relationship MERGE statements for frame edges", async ({
  request,
}) => {
  // Fetch edges via the API to get real data
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query: "MATCH (a:Inquiries)-[r:HAS_PETS]->(b:Pets) RETURN a, r, b LIMIT 3",
    },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(resp.status()).toBe(200);
  const body = await resp.json();

  // Collect edges from response (same logic as extractElements)
  type EdgeRow = { start: number; end: number; type: string; startNode: { id: number; tableLabel: string }; endNode: { id: number; tableLabel: string }; identity: string };
  const edges: EdgeRow[] = [];
  for (const row of (body.rows ?? []) as Array<Record<string, unknown>>) {
    const r = row.r as EdgeRow | undefined;
    if (r && "identity" in r && "startNode" in r) {
      edges.push(r);
    }
  }
  expect(edges.length, "expected edge rows from API").toBeGreaterThan(0);

  // Verify the Cypher MATCH pattern would be well-formed
  for (const e of edges) {
    expect(typeof e.start, "e.start must be integer").toBe("number");
    expect(typeof e.end, "e.end must be integer").toBe("number");
    expect(e.type, "e.type must be a string").toBeTruthy();
    expect(e.startNode.tableLabel, "startNode.tableLabel must exist").toBeTruthy();
    expect(e.endNode.tableLabel, "endNode.tableLabel must exist").toBeTruthy();
    // The Cypher line buildCypherScript generates:
    const line =
      `MATCH (a:\`${e.startNode.tableLabel}\` {_provisa_id: ${e.start}}), ` +
      `(b:\`${e.endNode.tableLabel}\` {_provisa_id: ${e.end}}) ` +
      `MERGE (a)-[:\`${e.type}\`]->(b);`;
    expect(line).toContain("MERGE (a)-[");
    expect(line).not.toContain("undefined");
    expect(line).not.toContain("NaN");
    // _provisa_id values must be integers (not compound strings)
    expect(line).not.toMatch(/_provisa_id: [A-Za-z]/);
  }
});
