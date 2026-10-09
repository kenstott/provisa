// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9f14-3c7a-4d58-a0e1-9f4c2b7d8e36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1924: a source's write operations are registered as commands one at a time and passed
// through as is. Against the live petstore mock the e2e harness serves (source petstore-api):
// the operations are offered and none is registered; one registered is called over GraphQL and
// SQL with a JSON object, reaches the remote unchanged, and the remote's refusal comes back as
// the remote stated it. A write cannot be composed into a larger statement.

import { test, expect, BACKEND_URL } from "./coverage";
import type { APIRequestContext } from "playwright/test";

const ADMIN = { "Content-Type": "application/json", "x-provisa-role": "org_admin" };
const STAMP = Date.now();
const PLACE = `e2e_place_order_${STAMP}`;
const CANCEL = `e2e_delete_order_${STAMP}`;
const ORDER_ID = 900000 + (STAMP % 99999);

async function adminGql(request: APIRequestContext, query: string, variables = {}) {
  const res = await request.post(`${BACKEND_URL}/admin/graphql`, {
    data: { query, variables },
    headers: ADMIN,
  });
  expect(res.ok(), await res.text()).toBeTruthy();
  const json = await res.json();
  expect(json.errors, JSON.stringify(json.errors)).toBeUndefined();
  return json.data;
}

async function register(request: APIRequestContext, name: string, operation: string) {
  const res = await request.post(`${BACKEND_URL}/admin/actions/functions`, {
    headers: ADMIN,
    // Only what the steward chooses: the operation, its domain and who may call it. What the
    // command takes and answers follows from the operation.
    data: {
      name,
      sourceId: "petstore-api",
      functionName: operation,
      domainId: "pet-store",
      writableBy: ["org_admin"],
    },
  });
  expect(res.ok(), await res.text()).toBeTruthy();
}

async function petstoreBase(request: APIRequestContext): Promise<string> {
  const data = await adminGql(request, "{ sources { id path } }");
  const path = (data.sources as { id: string; path: string }[]).find(
    (s) => s.id === "petstore-api",
  )?.path;
  expect(path, "petstore-api has no path").toBeTruthy();
  return (path as string).replace(/\/openapi\.json$/, "");
}

test.afterAll(async ({ request }) => {
  for (const name of [PLACE, CANCEL]) {
    await request.delete(`${BACKEND_URL}/admin/actions/functions/${name}`, { headers: ADMIN });
  }
});

test("REQ-1924: a source's write operations are offered, and adding the source registers none", async ({
  request,
}) => {
  const data = await adminGql(
    request,
    `query { availableFunctions(sourceId: "petstore-api", schemaName: "openapi") { name } }`,
  );
  const offered = (data.availableFunctions as { name: string }[]).map((f) => f.name);
  expect(offered).toEqual(expect.arrayContaining(["placeOrder", "deleteOrder", "addPet"]));
  expect(offered).not.toContain("getInventory"); // a GET is a table, not a command

  const actions = await request.get(`${BACKEND_URL}/admin/actions`, { headers: ADMIN });
  const fns = (await actions.json()).functions as { sourceId: string }[];
  expect(fns.filter((f) => f.sourceId === "petstore-api")).toEqual([]);
});

test("REQ-1924: a registered operation is called with a JSON object and reaches the remote unchanged", async ({
  request,
}) => {
  test.setTimeout(120000);
  await register(request, PLACE, "placeOrder");
  await register(request, CANCEL, "deleteOrder");

  const actions = await request.get(`${BACKEND_URL}/admin/actions`, { headers: ADMIN });
  const place = ((await actions.json()).functions as Record<string, unknown>[]).find(
    (f) => f.name === PLACE,
  );
  expect(place?.implKind).toBe("source_operation");
  expect(place?.arguments).toEqual([expect.objectContaining({ name: "body", type: "json" })]);

  // GraphQL: the command is a mutation field taking the object as written.
  const schema = await request.post(`${BACKEND_URL}/data/graphql`, {
    headers: ADMIN,
    data: { query: "{ __schema { mutationType { fields { name } } } }" },
  });
  const fields = ((await schema.json()).data.__schema.mutationType.fields as { name: string }[])
    .map((f) => f.name)
    // The field is the domain prefix and the naming convention's casing of the command's name,
    // and the convention keeps the underscore before a run of digits:
    // pet_store__e2ePlaceOrder_<stamp>. Match the name's letters and digits, not its separators.
    .filter((n) => n.toLowerCase().replace(/_/g, "").endsWith(`placeorder${STAMP}`));
  expect(fields, "the command's mutation field").toHaveLength(1);
  // A field the petstore spec does not declare is passed through all the same.
  const order = { id: ORDER_ID, petId: 7, quantity: 2, status: "placed", note: "e2e" };
  const called = await request.post(`${BACKEND_URL}/data/graphql`, {
    headers: ADMIN,
    data: {
      query: `mutation($o: JSON) { ${fields[0]}(body: $o) }`,
      variables: { o: order },
    },
  });
  expect(called.ok(), await called.text()).toBeTruthy();
  const answer = await called.json();
  expect(answer.errors, JSON.stringify(answer.errors)).toBeUndefined();
  expect(answer.data[fields[0]]).toEqual([order]);

  // The write reached the remote: the petstore now holds the order, note included.
  const base = await petstoreBase(request);
  const held = await request.get(`${base}/store/order/${ORDER_ID}`);
  expect(await held.json()).toEqual(order);

  // SQL: each argument is a JSON literal; the path parameter fills the path.
  const sql = await request.post(`${BACKEND_URL}/data/sql`, {
    headers: ADMIN,
    data: { sql: `SELECT * FROM ${CANCEL}('${ORDER_ID}')` },
  });
  expect(sql.ok(), await sql.text()).toBeTruthy();
  const gone = await request.get(`${base}/store/order/${ORDER_ID}`);
  expect(gone.status()).toBe(404);
});

test("REQ-1924: the remote's refusal comes back as the remote stated it", async ({ request }) => {
  await register(request, PLACE, "placeOrder");
  const sql = await request.post(`${BACKEND_URL}/data/sql`, {
    headers: ADMIN,
    // No order at all: the petstore refuses with 400, "No Order provided".
    data: { sql: `SELECT * FROM ${PLACE}('null')` },
  });
  expect(sql.status()).toBe(422);
  const body = await sql.json();
  expect(body.code).toBe("functions.remote_refused");
  expect(body.params.remote_status).toBe(400);
  expect(body.params.answer).toContain("No Order provided");
});

test("REQ-1924: a write cannot be composed into a larger statement", async ({ request }) => {
  await register(request, CANCEL, "deleteOrder");
  const sql = await request.post(`${BACKEND_URL}/data/sql`, {
    headers: ADMIN,
    data: {
      sql: `SELECT o.id FROM (SELECT 1 AS id) o JOIN ${CANCEL}('${ORDER_ID}') c ON true`,
    },
  });
  expect(sql.ok()).toBeFalsy();
  expect(await sql.text()).toContain(
    "cannot be composed in a query, a view or a materialized view",
  );
});
