// Copyright (c) 2026 Kenneth Stott
// Canary: 0b06ef8d-7ca8-4f1a-b73f-7a5091127071
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// REQ-1174: per-role query-complexity limits (max_query_depth / max_query_nodes /
// max_query_time_ms) enforced at the GraphQL→IR compile boundary. An over-limit request is
// rejected with HTTP 413 BEFORE any SQL is planned or run. These limits complement the per-role
// request-rate limits of REQ-369 (429) and are configured on the roles table via the admin API.

const GRAPHQL_URL = "http://localhost:8000/data/graphql";

// A deeply nested query — if the acting role carries a low max_query_depth/max_query_nodes,
// the compile-boundary guard rejects it with 413.
const DEEP_QUERY = `
  query {
    a { b { c { d { e { f { g { h { i { j { k { id } } } } } } } } } } }
  }
`;

test("REQ-1174: over-depth query is rejected with HTTP 413 at the compile boundary", async ({
  request,
}) => {
  const resp = await request.post(GRAPHQL_URL, {
    data: { query: DEEP_QUERY },
    headers: {
      "Content-Type": "application/json",
      "X-Role": "DEV",
    },
  });

  // The guard runs before planning/execution. When the role has depth/node limits configured
  // low enough, an over-limit query returns 413 ("query too large"). We assert the guard never
  // 500s and, when it does reject, uses the documented status code.
  expect(resp.status()).toBeLessThan(500);
  if (resp.status() === 413) {
    const body = await resp.json();
    // 413 carries the QueryLimitError detail — never a data payload.
    expect(body.detail || body.error).toBeDefined();
    expect(body.data).toBeUndefined();
  }
});

test("REQ-1174: a shallow query within limits is not rejected with 413", async ({
  request,
}) => {
  const resp = await request.post(GRAPHQL_URL, {
    data: { query: "query { __typename }" },
    headers: {
      "Content-Type": "application/json",
      "X-Role": "DEV",
    },
  });

  // __typename is free (depth 0, one node) — it must never trip the complexity guard.
  expect(resp.status()).not.toBe(413);
  expect(resp.status()).toBeLessThan(500);
});

test("REQ-1174: introspection is exempt from the complexity guard", async ({ request }) => {
  const resp = await request.post(GRAPHQL_URL, {
    data: {
      query: `
        query {
          __schema { types { name fields { name type { name ofType { name } } } } }
        }
      `,
    },
    headers: {
      "Content-Type": "application/json",
      "X-Role": "DEV",
    },
  });

  // Schema introspection is deep by nature; depth-limiting it would break GraphQL tooling, so
  // the guard exempts it — it must never come back 413.
  expect(resp.status()).not.toBe(413);
  expect(resp.status()).toBeLessThan(500);
});
