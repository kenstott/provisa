// Copyright (c) 2026 Kenneth Stott
// Canary: 64873040-5682-4ea9-a51f-e7d3bc61e361
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect, BACKEND_URL } from "./coverage";

/**
 * Verifies that auto-impute adds edges between nodes returned from a node-only query.
 * Regression for: imputed edge startNode/endNode ids using raw PK instead of stable
 * integer ids (registered via node_ids table), causing canvas lookup failures.
 */
test("auto-impute fires and returns integer-id edges for meta node query", async ({ page }) => {
  // Graph UI + role change + editor interaction + Meta query + impute API.
  // Under full-suite load the Meta query alone can take 15-20s; 120s total budget.
  test.setTimeout(120000);
  await page.goto("/graph");

  // The Meta domain is admin-only (config/provisa.yaml grants it to org_admin, not the
  // default "analyst" role) — select org_admin so the query below has access.
  await page.locator('[data-testid="role-selector-trigger"]').click();
  await page.getByRole("menuitem", { name: "org_admin", exact: true }).click();

  // Intercept impute responses before anything fires
  const imputeResponsePromise = page.waitForResponse(
    (resp) =>
      resp.url().includes("/data/impute-relationships") && resp.status() === 200,
    { timeout: 90000 }
  );

  // Enable auto-impute if not already active (button title changes based on state)
  const imputeBtn = page.locator('button[title*="Auto-impute"]').first();
  await imputeBtn.waitFor({ timeout: 10000 });
  const isActive = await imputeBtn.evaluate((el) => el.classList.contains("gf-icon-btn--on"));
  if (!isActive) {
    await imputeBtn.click();
  }

  // Expand the collapsed query bar first, then interact with CodeMirror
  const collapsed = page.locator(".gf-header-query-collapsed").first();
  if (await collapsed.isVisible()) {
    await collapsed.click();
  }
  const editor = page.locator(".cm-content").first();
  await editor.waitFor({ timeout: 5000 });

  // Type, then confirm the text is actually in the editor before running — and retype if it is
  // not. The role switch above refetches the graph schema, and GraphFrame resets editQuery from
  // the frame when that lands; keystrokes delivered in the window before the reset are wiped,
  // leaving an empty editor and a query that never runs (the failure this guards: the frame
  // stayed on "Run a Cypher query to explore the graph" and .gf-meta-text never rendered).
  // ControlOrMeta is the select-all modifier CodeMirror's Mod- bindings resolve to per platform —
  // Meta alone is a no-op on the Linux CI runners.
  const QUERY = "MATCH (n:Meta) RETURN n LIMIT 50";
  await expect(async () => {
    await editor.click();
    await page.keyboard.press("ControlOrMeta+a");
    await page.keyboard.type(QUERY);
    expect((await editor.innerText()).replace(/\s+/g, " ").trim()).toBe(QUERY);
  }).toPass({ timeout: 30000 });

  // Run the query. GraphFrame binds bare Enter at Prec.highest (GraphFrame.tsx:493-503).
  await page.keyboard.press("Enter");

  // Wait for the frame to finish loading and show nodes. Meta query can be slow
  // under full-suite load; 60s matches the DuckDB warm-up budget.
  await expect(page.locator(".gf-meta-text")).toContainText(/\d+ nodes/, {
    timeout: 60000,
  });

  // Wait for the impute API to fire and respond
  const imputeResp = await imputeResponsePromise;
  const imputeBody = await imputeResp.json();

  const edges = ((imputeBody.rows ?? []) as Array<{ node: Record<string, unknown> }>)
    .map((r) => r.node)
    .filter((n) => "identity" in n && "startNode" in n);

  // Edges must be returned (meta schema has relationships between its tables)
  expect(edges.length, "impute returned 0 edges for Meta nodes").toBeGreaterThan(0);

  // startNode/endNode ids must be stable integers (registered via node_ids table)
  for (const e of edges) {
    const sn = e.startNode as { id: unknown; label: string };
    const en = e.endNode as { id: unknown; label: string };
    expect(typeof sn.id, `startNode.id should be number, got ${JSON.stringify(sn.id)}`).toBe("number");
    expect(typeof en.id, `endNode.id should be number, got ${JSON.stringify(en.id)}`).toBe("number");
  }
});

/**
 * Verifies that the /data/impute-relationships endpoint returns edges with
 * stable integer startNode/endNode ids (registered via node_ids table).
 */
test("impute-relationships API returns integer startNode/endNode ids", async ({ request }) => {
  // Get a few Meta nodes from a real query first
  const queryResp = await request.post(`${BACKEND_URL}/data/cypher`, {
    data: { query: "MATCH (n:Meta) RETURN n LIMIT 10" },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(queryResp.status()).toBe(200);
  const queryBody = await queryResp.json();
  const nodes = (queryBody.rows as Array<{ n: { id: number; label: string } }>).map((r) => ({
    id: r.n.id,
    label: r.n.label,
  }));

  // All node ids should be stable integers (registered via node_ids table)
  for (const n of nodes) {
    expect(typeof n.id, `node.id should be number, got ${JSON.stringify(n.id)}`).toBe("number");
  }

  // Now impute with all 10 nodes (using stable integer ids as the frontend sends)
  const imputeResp = await request.post(`${BACKEND_URL}/data/impute-relationships`, {
    data: { nodes },
    headers: { "Content-Type": "application/json", "X-Role": "DEV" },
  });
  expect(imputeResp.status()).toBe(200);
  const imputeBody = await imputeResp.json();
  const rows: Array<{ node: Record<string, unknown> }> = imputeBody.rows ?? [];
  const edges = rows
    .map((r) => r.node)
    .filter((n) => "identity" in n && "startNode" in n);

  // If any edges were returned, verify startNode/endNode ids are stable integers
  for (const e of edges) {
    const sn = e.startNode as { id: unknown; label: string };
    const en = e.endNode as { id: unknown; label: string };
    expect(typeof sn.id, `startNode.id should be number for edge ${e.identity}, got ${JSON.stringify(sn.id)}`).toBe("number");
    expect(typeof en.id, `endNode.id should be number for edge ${e.identity}, got ${JSON.stringify(en.id)}`).toBe("number");
  }
});
