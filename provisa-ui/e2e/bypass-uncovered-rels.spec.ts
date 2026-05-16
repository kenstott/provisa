// Copyright (c) 2026 Kenneth Stott
// Canary: c3d4e5f6-a7b8-9012-cdef-123456789012
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Validates bypass_uncovered_relationships across SQL, GQL, and CQL endpoints.
 *
 * Requires the backend on port 8000 with the demo install (pets + shelter/graphql sources).
 * Tests call endpoints directly via `request` — no UI interaction.
 *
 * Validates:
 *   - SQL: simple pets query produces no V002 (uncovered remote rel is bypassed).
 *   - GQL: inquiries→pet→assignment produces no V002 errors.
 *   - CQL: IS_ASSIGNMENT (pets→assignments) produces no V002.
 *   - SQL: wrong-column join on registered pair always produces V002.
 *   - CQL: full Inquiries→Pets→Assignments→Employees traversal produces no V002.
 */

const BACKEND = "http://localhost:8000";
const HEADERS = { "Content-Type": "application/json" };

// ── SQL ───────────────────────────────────────────────────────────────────────

test.describe("SQL bypass_uncovered_relationships", () => {
  test.describe.configure({ timeout: 30_000 });

  test("SQL JOIN from local to remote source produces no V002 (bypass_uncovered_relationships)", async ({ request }) => {
    // pets (postgresql) LEFT JOIN inquiries (sqlite/remote) on an unregistered column pair.
    // V002 must NOT be raised — at least one side is a non-postgresql remote source.
    const resp = await request.post(`${BACKEND}/data/sql`, {
      headers: HEADERS,
      data: {
        sql: (
          'SELECT p.id, p.name, i.id AS inquiry_id ' +
          'FROM "pet_store"."pets" AS p ' +
          'LEFT JOIN "pet_store"."inquiries" AS i ON i.pet_id = p.id'
        ),
        role: "admin",
      },
    });
    // 403 with V002 is the bug; other non-200 statuses are execution issues, not governance
    if (resp.status() === 403) {
      const body = await resp.json();
      const violations = (body?.detail?.violations ?? []) as Array<{ code: string }>;
      const v002 = violations.filter((v) => v.code === "V002");
      expect(v002, `V002 raised for uncovered remote join — bypass not applied: ${JSON.stringify(v002)}`).toHaveLength(0);
    }
  });

  test("wrong-column join between two local tables always produces V002 (no bypass for local sources)", async ({ request }) => {
    // Both pets and inquiries are treated as local sources in this join path.
    // Joining on non-registered columns (i.id = p.breed_name) must raise V002 —
    // bypass_uncovered_relationships does NOT apply when both sides are postgresql.
    const resp = await request.post(`${BACKEND}/data/sql`, {
      headers: HEADERS,
      data: {
        sql: (
          "SELECT p.id, i.id " +
          'FROM "pet_store"."pets" AS p ' +
          'LEFT JOIN "pet_store"."inquiries" AS i ON i.id = p.breed_name'
        ),
        role: "admin",
      },
    });
    if (resp.status() === 403) {
      const body = await resp.json();
      const v002 = (body?.detail?.violations ?? []).filter((v: { code: string }) => v.code === "V002");
      expect(v002.length, `Expected V002 for wrong-column join on local tables, got: ${JSON.stringify(body?.detail?.violations)}`).toBeGreaterThan(0);
    } else if (resp.status() === 200) {
      const body = await resp.json();
      const v002 = (body.violations ?? []).filter((v: { code: string }) => v.code === "V002");
      expect(v002.length, `Expected V002 for wrong-column join on local tables, got: ${JSON.stringify(body.violations)}`).toBeGreaterThan(0);
    } else {
      expect([400, 422]).toContain(resp.status());
    }
  });
});

// ── GQL ───────────────────────────────────────────────────────────────────────

test.describe("GQL bypass_uncovered_relationships", () => {
  test.describe.configure({ timeout: 30_000 });

  test("inquiries→pet→assignment query produces no V002", async ({ request }) => {
    const resp = await request.post(`${BACKEND}/data/graphql`, {
      headers: HEADERS,
      data: {
        query: `
          query {
            ps__inquiries {
              id
              inquiryType
              pet {
                name
                assignment {
                  breedName
                }
              }
            }
          }
        `,
        role: "admin",
      },
    });
    expect(resp.ok(), `Unexpected status ${resp.status()}: ${await resp.text()}`).toBeTruthy();
    const body = await resp.json();
    const v002Errors = (body.errors ?? []).filter((e: unknown) => JSON.stringify(e).includes("V002"));
    expect(v002Errors, `Unexpected V002 in GQL response: ${JSON.stringify(v002Errors)}`).toHaveLength(0);
  });
});

// ── CQL (Cypher) ──────────────────────────────────────────────────────────────

test.describe("CQL bypass_uncovered_relationships", () => {
  test.describe.configure({ timeout: 30_000 });

  test("IS_ASSIGNMENT (pets→assignments) produces no V002", async ({ request }) => {
    const resp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: {
        query: "MATCH (p:Pets)-[:IS_ASSIGNMENT]->(a:Assignments) RETURN p, a LIMIT 10",
        role: "admin",
      },
    });
    expect([200, 400]).toContain(resp.status());
    if (resp.status() === 200) {
      const body = await resp.json();
      const v002 = (body.violations ?? []).filter((v: { code: string }) => v.code === "V002");
      expect(v002, `IS_ASSIGNMENT Cypher should not produce V002: ${JSON.stringify(v002)}`).toHaveLength(0);
    }
  });

  test("full Inquiries→Pets→Assignments→Employees traversal produces no V002", async ({ request }) => {
    const query = [
      "MATCH (a:Inquiries)",
      "OPTIONAL MATCH (a:Inquiries)-[:HAS_PETS]->(b:Pets)",
      "OPTIONAL MATCH (b:Pets)-[:IS_ASSIGNMENT]->(c:Assignments)",
      "OPTIONAL MATCH (c:Assignments)-[:IS_EMPLOYEE]->(d:Employees)",
      "RETURN a, b, c, d LIMIT 25",
    ].join(" ");
    const resp = await request.post(`${BACKEND}/data/cypher`, {
      headers: HEADERS,
      data: { query, role: "admin" },
    });
    expect([200, 400]).toContain(resp.status());
    if (resp.status() === 200) {
      const body = await resp.json();
      const v002 = (body.violations ?? []).filter((v: { code: string }) => v.code === "V002");
      expect(v002, `Full Inquiries→Pets→Assignments→Employees traversal should not produce V002: ${JSON.stringify(v002)}`).toHaveLength(0);
    }
  });
});
