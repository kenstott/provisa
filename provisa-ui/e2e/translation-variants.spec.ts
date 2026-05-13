// Copyright (c) 2026 Kenneth Stott
// Canary: d7e3f1a2-b4c5-4d6e-9f0a-1b2c3d4e5f6a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Validates SQL and Cypher translation outputs for all query shapes and variants.
 *
 * Tests call /admin/graphql compileQuery directly — no UI interaction — so they
 * are unaffected by GraphiQL render state.  Each test group covers a distinct
 * query shape; each test within a group covers a translation variant
 * (flatSql, flatCypher, nodeOnlyCypher, aggregated).
 *
 * Requires the backend on port 8000 with ps__pets / shelter schema loaded.
 */

const BACKEND = "http://localhost:8000";
const HEADERS = { "Content-Type": "application/json", "X-Role": "admin" };

const COMPILE_MUTATION = `
  mutation CompileQuery($input: CompileQueryInput!) {
    compileQuery(input: $input) {
      sql semanticSql trinoSql compiledCypher cypherError
      rootField canonicalField route routeReason
    }
  }
`;

async function compile(
  request: Parameters<typeof test>[1] extends { request: infer R } ? R : never,
  query: string,
  opts: { flatSql?: boolean; flatCypher?: boolean; nodeOnlyCypher?: boolean } = {},
) {
  const resp = await (request as any).post(`${BACKEND}/admin/graphql`, {
    headers: HEADERS,
    data: {
      query: COMPILE_MUTATION,
      variables: {
        input: {
          query,
          role: "admin",
          variables: null,
          flatSql: opts.flatSql ?? false,
          flatCypher: opts.flatCypher ?? false,
          nodeOnlyCypher: opts.nodeOnlyCypher ?? false,
        },
      },
    },
  });
  return resp;
}

// ── Query shapes ──────────────────────────────────────────────────────────────

const Q_FLAT = `{ ps__pets { name breedName } }`;

const Q_MANY_TO_ONE = `{ ps__pets { assignment { breedName } name breedName } }`;

const Q_ONE_TO_MANY_NESTED = `{
  ps__pets {
    assignment { breedName employee { lastName } }
    name
    breedName
  }
}`;

// ── Helpers ───────────────────────────────────────────────────────────────────

function firstResult(body: any) {
  const results: any[] = body?.data?.compileQuery ?? [];
  expect(results.length).toBeGreaterThan(0);
  return results[0];
}

// ── Flat query (no joins) ─────────────────────────────────────────────────────

test.describe("flat query — ps__pets { name breedName }", () => {
  test.describe.configure({ timeout: 30_000 });

  test("compiles to SQL with pets table", async ({ request }) => {
    const resp = await compile(request, Q_FLAT);
    expect(resp.ok()).toBeTruthy();
    const body = await resp.json();
    expect(body.errors).toBeUndefined();
    const r = firstResult(body);
    const sql: string = r.semanticSql ?? r.sql ?? "";
    expect(sql.toLowerCase()).toContain("pets");
  });

  test("Cypher contains MATCH (a:Pets)", async ({ request }) => {
    const resp = await compile(request, Q_FLAT);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(r.cypherError ?? null).toBeNull();
    expect(cypher).toContain("MATCH");
    expect(cypher).toMatch(/Pets/);
  });

  test("flatSql=true produces equivalent SQL without ARRAY_AGG", async ({ request }) => {
    const resp = await compile(request, Q_FLAT, { flatSql: true });
    const body = await resp.json();
    const r = firstResult(body);
    const sql: string = (r.semanticSql ?? r.sql ?? "").toLowerCase();
    expect(sql).not.toContain("array_agg");
  });

  test("nodeOnlyCypher=true produces RETURN with single alias", async ({ request }) => {
    const resp = await compile(request, Q_FLAT, { nodeOnlyCypher: true });
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    const returnLine = cypher.split("\n").find((l) => l.startsWith("RETURN"));
    expect(returnLine).toBeDefined();
    // Node-only mode returns node aliases, not property paths
    expect(returnLine).not.toContain(".");
  });
});

// ── Many-to-one relationship ──────────────────────────────────────────────────

test.describe("many-to-one join — ps__pets { assignment { breedName } }", () => {
  test.describe.configure({ timeout: 30_000 });

  test("aggregated SQL contains ARRAY_AGG for assignment", async ({ request }) => {
    const resp = await compile(request, Q_MANY_TO_ONE);
    const body = await resp.json();
    const r = firstResult(body);
    const sql: string = (r.semanticSql ?? r.sql ?? "").toLowerCase();
    expect(sql).toContain("array_agg");
    expect(sql).toContain("assignment");
  });

  test("flatSql=true joins assignments without ARRAY_AGG", async ({ request }) => {
    const resp = await compile(request, Q_MANY_TO_ONE, { flatSql: true });
    const body = await resp.json();
    const r = firstResult(body);
    const sql: string = (r.semanticSql ?? r.sql ?? "").toLowerCase();
    expect(sql).not.toContain("array_agg");
    expect(sql.toLowerCase()).toContain("assignment");
  });

  test("Cypher contains OPTIONAL MATCH for Assignments", async ({ request }) => {
    const resp = await compile(request, Q_MANY_TO_ONE);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(r.cypherError ?? null).toBeNull();
    expect(cypher).toContain("OPTIONAL MATCH");
    expect(cypher).toMatch(/Assignments?/);
  });

  test("flatCypher=true still contains Assignments", async ({ request }) => {
    const resp = await compile(request, Q_MANY_TO_ONE, { flatCypher: true });
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(cypher).toMatch(/Assignments?/);
  });

  test("nodeOnlyCypher=true RETURN includes both pets and assignments aliases", async ({
    request,
  }) => {
    const resp = await compile(request, Q_MANY_TO_ONE, { nodeOnlyCypher: true });
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    const returnLine = cypher.split("\n").find((l) => l.startsWith("RETURN")) ?? "";
    const aliases = returnLine.replace("RETURN", "").split(",").map((s) => s.trim());
    expect(aliases.length).toBeGreaterThanOrEqual(2);
  });
});

// ── Chained: assignment → employee (the bug-fix case) ────────────────────────

test.describe("chained many-to-one: assignment → employee — Cypher translation", () => {
  test.describe.configure({ timeout: 30_000 });

  test("aggregated SQL contains ARRAY_AGG subquery with inner JOIN for employee", async ({
    request,
  }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED);
    const body = await resp.json();
    const r = firstResult(body);
    const sql: string = (r.semanticSql ?? r.sql ?? "").toLowerCase();
    expect(sql).toContain("array_agg");
    expect(sql).toContain("employee");
  });

  test("Cypher contains OPTIONAL MATCH for Assignments", async ({ request }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(r.cypherError ?? null).toBeNull();
    expect(cypher).toMatch(/Assignments?/);
  });

  test("Cypher contains OPTIONAL MATCH for Employees (regression: was missing)", async ({
    request,
  }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(r.cypherError ?? null).toBeNull();
    expect(cypher).toMatch(/Employees?/);
  });

  test("Cypher has exactly 2 OPTIONAL MATCHes (assignments and employees)", async ({
    request,
  }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    const count = (cypher.match(/OPTIONAL MATCH/g) ?? []).length;
    expect(count).toBe(2);
  });

  test("Cypher OPTIONAL MATCHes are chained (Assignments → Employees)", async ({
    request,
  }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED);
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    const lines = cypher.split("\n");
    const optionals = lines.filter((l) => l.includes("OPTIONAL MATCH"));
    expect(optionals.length).toBe(2);
    // First optional: Pets → Assignments
    expect(optionals[0]).toMatch(/Assignments?/);
    // Second optional: Assignments → Employees (the chained hop)
    expect(optionals[1]).toMatch(/Assignments?/);
    expect(optionals[1]).toMatch(/Employees?/);
  });

  test("nodeOnlyCypher=true RETURN includes 3 aliases (pets, assignments, employees)", async ({
    request,
  }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED, { nodeOnlyCypher: true });
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    const returnLine = cypher.split("\n").find((l) => l.startsWith("RETURN")) ?? "";
    const aliases = returnLine.replace("RETURN", "").split(",").map((s) => s.trim());
    expect(aliases.length).toBeGreaterThanOrEqual(3);
  });

  test("flatCypher=true still includes employees", async ({ request }) => {
    const resp = await compile(request, Q_ONE_TO_MANY_NESTED, { flatCypher: true });
    const body = await resp.json();
    const r = firstResult(body);
    const cypher: string = r.compiledCypher ?? "";
    expect(cypher).toMatch(/Employees?/);
  });
});
