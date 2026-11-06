// Copyright (c) 2026 Kenneth Stott
// Canary: a31f1819-e369-4798-b82b-0f0b163a2133
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect, BACKEND_URL } from "./coverage";

// No relationship joins pets to assignments on breed_name (it repeats on both, so it cannot be a
// replica key); assignments are matched on their own and reach animal_breeds via ASSIGNMENT_BREED.
const QUERY = `
MATCH (a:Inquiries)
OPTIONAL MATCH (a:Inquiries)-[r1:HAS_PETS]->(b:Pets)
OPTIONAL MATCH (c:Assignments)
OPTIONAL MATCH (c:Assignments)-[r3:ASSIGNMENT_EMPLOYEE]->(d:Employees)
OPTIONAL MATCH (c:Assignments)-[r4:ASSIGNMENT_BREED]->(e:AnimalBreeds)
OPTIONAL MATCH (a)-[rUsers:SUBMITTED_BY]-(mUsers:PetStore:Users)
WHERE NOT b.id IN [2]
AND NOT c.breedName IN ['Maine Coon']
RETURN a, b, c, d, e, rUsers, mUsers, r1, r3, r4
`;

test("cypher breed filter resolves breedName without FederationError", async ({ request }) => {
  // This query hits Inquiries, Pets, Assignments, Employees, AnimalBreeds, Users across
  // multiple sources. DuckDB cold-start + concurrent suite load can exceed 30s.
  // 120s budget: ~30-90s for DuckDB + GQL materialisation, ~30s headroom.
  test.setTimeout(120000);
  const resp = await request.post(`${BACKEND_URL}/data/cypher`, {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  const detail = JSON.stringify(body);
  expect(resp.status(), detail).toBe(200);
  expect(body.error, detail).toBeUndefined();
  expect(body.columns, detail).toBeDefined();
});
