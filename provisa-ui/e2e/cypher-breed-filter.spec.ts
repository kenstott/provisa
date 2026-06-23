// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const QUERY = `
MATCH (a:Inquiries)
OPTIONAL MATCH (a:Inquiries)-[r1:HAS_PETS]->(b:Pets)
OPTIONAL MATCH (b:Pets)-[r2:IS_ASSIGNMENT]->(c:Assignments)
OPTIONAL MATCH (c:Assignments)-[r3:IS_EMPLOYEE]->(d:Employees)
OPTIONAL MATCH (c:Assignments)-[r4:ASSIGNMENT_BREED]->(e:AnimalBreeds)
OPTIONAL MATCH (a)-[rUsers:SUBMITTED_BY]-(mUsers:PetStore:Users)
WHERE NOT b.id IN [2]
AND NOT c.breedName IN ['Maine Coon']
RETURN a, b, c, d, e, rUsers, mUsers, r1, r2, r3, r4
`;

test("cypher breed filter resolves breedName without FederationError", async ({ request }) => {
  const resp = await request.post("http://localhost:8000/data/cypher", {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toBeDefined();
});
