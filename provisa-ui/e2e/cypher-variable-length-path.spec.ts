// Copyright (c) 2026 Kenneth Stott
// Canary: ed8a4842-eed7-427b-9089-26eb96cd5ee5
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const QUERY = `
MATCH r = (n:PetStore:Inquiries)-[*..5]-(mEmployees:Shelter:Employees)
RETURN r
LIMIT 2500
`;

test("variable-length cross-source path executes without FederationError", async ({ request }) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body).slice(0, 500);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toBeDefined();

  const rows: Array<{ r?: { nodes?: Array<{ id: string; label: string }> } }> = body.rows ?? [];
  const nodeMap = new Map<string, unknown>();
  for (const row of rows) {
    for (const node of row.r?.nodes ?? []) {
      nodeMap.set(`${node.label}:${node.id}`, node);
    }
  }
  expect(nodeMap.size, `expected 51 unique nodes, got ${nodeMap.size}`).toBe(51);
});

test("variable-length path RETURN a, b returns node objects not flat columns", async ({ request }) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: {
      query:
        "MATCH r = ((a:PetStore:Inquiries)-[*..5]-(b:Shelter:Employees)) RETURN a, b LIMIT 5",
    },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body).slice(0, 500);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toContain("a");
  expect(body.columns, detail).toContain("b");

  const rows: Array<{ a?: unknown; b?: unknown }> = body.rows ?? [];
  expect(rows.length, detail).toBeGreaterThan(0);

  const first = rows[0];
  const aNode = first.a as { label?: string; properties?: object } | undefined;
  const bNode = first.b as { label?: string; properties?: object } | undefined;
  expect(aNode?.label, "a should be a node with label").toBeDefined();
  expect(aNode?.properties, "a should have properties").toBeDefined();
  expect(bNode?.label, "b should be a node with label").toBeDefined();
  expect(bNode?.properties, "b should have properties").toBeDefined();
});
