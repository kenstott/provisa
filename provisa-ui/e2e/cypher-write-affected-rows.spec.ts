// Copyright (c) 2026 Kenneth Stott
// Canary: 0292cfb5-db72-47d6-8697-106badeb1bb4
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-670: Cypher write endpoints (CREATE/SET/DELETE) return the number of rows
// affected in an `affected_rows` field of the JSON response body.

import { test, expect } from "./coverage";
import type { APIRequestContext } from "playwright/test";

const BASE = "http://localhost:8000/data/cypher";
const HEADERS = { "Content-Type": "application/json", "x-provisa-role": "admin" };
const ID = 99981; // sentinel id, cleaned up by the DELETE step

async function cypher(request: APIRequestContext, query: string) {
  return request.post(BASE, { data: { query }, headers: HEADERS });
}

test("REQ-670: Cypher CREATE returns affected_rows count of inserted rows", async ({
  request,
}) => {
  // Pre-clean any leftover sentinel row from a prior aborted run.
  await cypher(request, `MATCH (n:Customers) WHERE n.id = ${ID} DELETE n`);

  const resp = await cypher(
    request,
    `CREATE (n:Customers {id: ${ID}, name: 'E2E Cypher', email: 'e2e-cypher@example.com', region: 'e2e-cypher'})`,
  );

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body.type).toBe("cypher");
  expect(body.affected_rows).toBe(1);
});

test("REQ-670: Cypher SET returns affected_rows count of updated rows", async ({
  request,
}) => {
  const resp = await cypher(
    request,
    `MATCH (n:Customers) WHERE n.id = ${ID} SET n.name = 'E2E Cypher Updated'`,
  );

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body.type).toBe("cypher");
  expect(body.affected_rows).toBe(1);
});

test("REQ-670: Cypher DELETE returns affected_rows count of deleted rows", async ({
  request,
}) => {
  const resp = await cypher(
    request,
    `MATCH (n:Customers) WHERE n.id = ${ID} DELETE n`,
  );

  expect(resp.status()).toBe(200);
  const body = await resp.json();
  expect(body.error).toBeUndefined();
  expect(body.type).toBe("cypher");
  expect(body.affected_rows).toBe(1);
});
