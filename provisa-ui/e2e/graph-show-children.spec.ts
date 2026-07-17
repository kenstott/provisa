// Copyright (c) 2026 Kenneth Stott
// Canary: 63e5d533-be7c-48f0-b5bc-0a6ecc1bb20b
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

type CyNode = { id: () => string; length: number };
type CyInstance = {
  nodes: () => CyNode[];
  $id: (id: string) => { renderedPosition: () => { x: number; y: number }; length: number };
};

async function getOverlayKeys(page: import("@playwright/test").Page): Promise<string[]> {
  return page.evaluate(() => {
    const od = (window as Record<string, unknown>).__overlayData as Map<string, unknown> | undefined;
    return od ? Array.from(od.keys()) : [];
  });
}

test("show children for two nodes keeps both visible", async ({ page }) => {
  // Set pending query before navigation so the page auto-runs it on mount
  await page.goto("/graph");
  await page.evaluate(() => {
    localStorage.setItem(
      "provisa.graph.pending_query",
      "MATCH (n:PetStore:Inquiries) RETURN n LIMIT 5",
    );
  });
  await page.reload();

  // Wait for cytoscape to initialize and have at least 2 nodes.
  // Use a long timeout: cold backend + Vite compilation can take >15s on first load.
  await page.waitForFunction(
    () => {
      const cy = (window as Record<string, unknown>).__cy as CyInstance | undefined;
      return cy != null && cy.nodes().length >= 2;
    },
    { timeout: 45000 },
  );

  // Collect node IDs (only root result nodes, not overlay nodes)
  const nodeIds = await page.evaluate(() => {
    const cy = (window as Record<string, unknown>).__cy as CyInstance;
    return cy.nodes().map((n) => n.id());
  });

  console.log("Node IDs:", nodeIds);
  expect(nodeIds.length).toBeGreaterThanOrEqual(2);

  const canvasBox = await page.locator(".gf-canvas").boundingBox();
  expect(canvasBox).not.toBeNull();

  async function openContextMenuForNode(nodeId: string) {
    const pos = await page.evaluate((id) => {
      const cy = (window as Record<string, unknown>).__cy as CyInstance;
      const node = cy.$id(id);
      return node.length > 0 ? node.renderedPosition() : null;
    }, nodeId);

    expect(pos, `Node ${nodeId} not found in cytoscape`).not.toBeNull();
    const x = canvasBox!.x + pos!.x;
    const y = canvasBox!.y + pos!.y;
    await page.mouse.click(x, y, { button: "right" });
    await page.waitForSelector(".gf-node-ctx-menu", { state: "visible", timeout: 5000 });
  }

  // Show children for first node
  await openContextMenuForNode(nodeIds[0]);
  const btn0 = page.locator(".gf-node-ctx-item", { hasText: /^Show children$/ }).first();
  const btn0Enabled = await btn0.isEnabled();
  if (!btn0Enabled) {
    // Node type has no child relationships — skip with a meaningful message
    test.skip(true, "First inquiry node has no child relationships configured");
    return;
  }
  await btn0.click();

  // Wait for first node's children to appear in overlayData
  await page.waitForFunction(
    () => {
      const od = (window as Record<string, unknown>).__overlayData as Map<string, unknown> | undefined;
      return od != null && Array.from(od.keys()).some((k) => k.endsWith(":children"));
    },
    { timeout: 10000 },
  );

  const keysAfterFirst = await getOverlayKeys(page);
  console.log("Overlay keys after first node:", keysAfterFirst);
  expect(keysAfterFirst.some((k) => k.endsWith(":children"))).toBe(true);

  // Show children for second node
  await openContextMenuForNode(nodeIds[1]);
  const btn1 = page.locator(".gf-node-ctx-item", { hasText: /^Show children$/ }).first();
  await btn1.click();

  // Wait for second node's children to be added (total :children entries should be 2)
  await page.waitForFunction(
    () => {
      const od = (window as Record<string, unknown>).__overlayData as Map<string, unknown> | undefined;
      if (!od) return false;
      return Array.from(od.keys()).filter((k) => k.endsWith(":children")).length >= 2;
    },
    { timeout: 10000 },
  );

  const keysAfterBoth = await getOverlayKeys(page);
  console.log("Overlay keys after both nodes:", keysAfterBoth);

  const childrenKeys = keysAfterBoth.filter((k) => k.endsWith(":children"));
  expect(childrenKeys.length).toBeGreaterThanOrEqual(2);

  // Verify the first node's children key is still present (the bug: it was removed)
  expect(childrenKeys.some((k) => k.startsWith(nodeIds[0]))).toBe(true);
  expect(childrenKeys.some((k) => k.startsWith(nodeIds[1]))).toBe(true);

  // Verify that children of BOTH nodes are actually rendered in cytoscape (not just in state).
  // The original bug: overlayEdges changes trigger a full cytoscape rebuild which wipes overlay
  // nodes from the canvas, and stale prevOverlayNodesRef prevents them from being re-added.
  const cyNodeCount = await page.evaluate(() => {
    const cy = (window as Record<string, unknown>).__cy as CyInstance;
    return cy.nodes().length;
  });
  // With both sets of children visible, there must be more nodes than the 5 initial query results
  expect(cyNodeCount).toBeGreaterThan(5);

  // Also verify the context menu for the first node still says "Hide children"
  await openContextMenuForNode(nodeIds[0]);
  const hideBtn = page.locator(".gf-node-ctx-item", { hasText: /^Hide children$/ }).first();
  await expect(hideBtn).toBeVisible();
  await page.keyboard.press("Escape");
});
