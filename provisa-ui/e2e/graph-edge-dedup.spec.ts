// Copyright (c) 2026 Kenneth Stott
// Canary: f7a8b9c0-d1e2-4f3a-b4c5-d6e7f8a9b0c1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Verifies that edge identity is canonical (src→tgt) regardless of traversal direction.
 *
 * When a Cypher query traverses a relationship backward (c)<-[r]-(o) and returns `r`,
 * the identity must equal the forward-traversal identity so imputed edges de-duplicate
 * correctly in the graph canvas.
 *
 * Requires the backend on port 8000 with the demo install.
 */

const BACKEND = "http://localhost:8000";
const HEADERS = { "Content-Type": "application/json" };

test.describe("Cypher edge identity — canonical direction for dedup", () => {
  test.describe.configure({ timeout: 30_000 });

  test("backward-traversal RETURN r produces same identity as forward traversal", async ({ request }) => {
    // Use a known relationship from the demo dataset.
    // IS_ASSIGNMENT: Pets → Assignments (forward canonical direction)
    const fwdResp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: {
        query: "MATCH (p:Pets)-[r:IS_ASSIGNMENT]->(a:Assignments) RETURN p, r, a LIMIT 1",
        role: "admin",
      },
    });
    // Backend might be unavailable — skip gracefully
    if (!fwdResp.ok()) {
      test.skip();
      return;
    }
    const fwdBody = await fwdResp.json();
    const fwdRows: unknown[] = fwdBody.rows ?? [];
    if (fwdRows.length === 0) {
      test.skip();
      return;
    }

    // Extract forward edge identity
    const fwdRow = fwdRows[0] as Record<string, unknown>;
    const fwdEdge = fwdRow["r"] as { identity?: string } | undefined;
    const fwdIdentity = fwdEdge?.identity;
    expect(fwdIdentity, "forward traversal edge must have identity").toBeTruthy();

    // Backward traversal: same relationship, reversed pattern
    const bwdResp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: {
        query: "MATCH (a:Assignments)<-[r:IS_ASSIGNMENT]-(p:Pets) RETURN p, r, a LIMIT 1",
        role: "admin",
      },
    });
    expect(bwdResp.ok(), `backward traversal request failed: ${bwdResp.status()}`).toBeTruthy();
    const bwdBody = await bwdResp.json();
    const bwdRows: unknown[] = bwdBody.rows ?? [];
    if (bwdRows.length === 0) {
      test.skip();
      return;
    }

    const bwdRow = bwdRows[0] as Record<string, unknown>;
    const bwdEdge = bwdRow["r"] as { identity?: string } | undefined;
    const bwdIdentity = bwdEdge?.identity;
    expect(bwdIdentity, "backward traversal edge must have identity").toBeTruthy();

    // Both must start with the same rel_type prefix
    expect(bwdIdentity!.startsWith("IS_ASSIGNMENT:"), `identity must start with IS_ASSIGNMENT: — got: ${bwdIdentity}`).toBe(true);
    expect(fwdIdentity!.startsWith("IS_ASSIGNMENT:"), `identity must start with IS_ASSIGNMENT: — got: ${fwdIdentity}`).toBe(true);

    // The src-tgt portion must be the same in both directions
    const fwdSuffix = fwdIdentity!.replace(/^IS_ASSIGNMENT:/, "");
    const bwdSuffix = bwdIdentity!.replace(/^IS_ASSIGNMENT:/, "");
    expect(bwdSuffix).toBe(fwdSuffix);
  });

  test("RETURN r identity uses canonical src-tgt regardless of direction", async ({ request }) => {
    // Same as above but using bare RETURN r to exercise _build_edge_object specifically
    const fwdResp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: {
        query: "MATCH (p:Pets)-[r:IS_ASSIGNMENT]->(a:Assignments) RETURN r LIMIT 1",
        role: "admin",
      },
    });
    if (!fwdResp.ok()) { test.skip(); return; }
    const fwdBody = await fwdResp.json();
    const fwdRows: unknown[] = fwdBody.rows ?? [];
    if (fwdRows.length === 0) { test.skip(); return; }

    const fwdEdge = (fwdRows[0] as Record<string, unknown>)["r"] as { identity?: string } | undefined;
    const fwdIdentity = fwdEdge?.identity;
    expect(fwdIdentity).toBeTruthy();

    const bwdResp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: {
        query: "MATCH (a:Assignments)<-[r:IS_ASSIGNMENT]-(p:Pets) RETURN r LIMIT 1",
        role: "admin",
      },
    });
    if (!bwdResp.ok()) { test.skip(); return; }
    const bwdBody = await bwdResp.json();
    const bwdRows: unknown[] = bwdBody.rows ?? [];
    if (bwdRows.length === 0) { test.skip(); return; }

    const bwdEdge = (bwdRows[0] as Record<string, unknown>)["r"] as { identity?: string } | undefined;
    const bwdIdentity = bwdEdge?.identity;
    expect(bwdIdentity).toBeTruthy();

    // Canonical identity must match between forward and backward traversal
    expect(bwdIdentity).toBe(fwdIdentity);
  });
});
