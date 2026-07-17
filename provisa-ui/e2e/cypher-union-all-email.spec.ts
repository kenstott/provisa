// Copyright (c) 2026 Kenneth Stott
// Canary: 674f02d6-e1d6-47d2-9c39-00ca94cbfaee
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const QUERY = `MATCH (n)
WHERE n.email IS NOT NULL
RETURN DISTINCT n.email AS email
LIMIT 25
UNION ALL
MATCH ()-[r]-()
WHERE r.email IS NOT NULL
RETURN DISTINCT "relationship" AS entity, r.email AS email
LIMIT 25`;

test("UNION ALL query with email filter executes without SYNTAX_ERROR", async ({ request }) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body).slice(0, 500);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toBeDefined();
});
