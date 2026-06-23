// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const QUERY = `
MATCH r = (n:PetStore:Inquiries)-[*..5]-(mEmployees:Shelter:Employees)
RETURN startNode(r) as c
`;

test("startNode(r) on varlen path returns node objects without FederationError", async ({
  request,
}) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body).slice(0, 500);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toContain("c");

  const rows: Array<{ c?: { id?: string; label?: string; properties?: object } }> =
    body.rows ?? [];
  expect(rows.length, "expected rows").toBeGreaterThan(0);

  const first = rows[0].c;
  expect(first?.label, "c should be a node with label").toBeDefined();
  expect(first?.properties, "c should have properties").toBeDefined();
});
