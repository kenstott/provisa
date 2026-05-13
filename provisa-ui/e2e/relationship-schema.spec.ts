// Copyright (c) 2026 Kenneth Stott
// Canary: d7e8f9a0-b1c2-3456-def0-123456789abc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Live e2e tests: verify that cross-source GQL relationships defined in the
 * relationships table are reflected as fields in the admin SDL.
 *
 * These tests guard against regression where remote-table relationships are
 * silently dropped from the schema (e.g. when _build_visible_tables skips
 * tables with no Trino column metadata).
 */
test.describe("Relationship fields in GQL SDL", () => {
  test.describe.configure({ timeout: 30000 });

  test("PS__Pets type has assignment field (cross-source remote relationship)", async ({
    request,
  }) => {
    const resp = await request.post("http://localhost:8000/data/graphql", {
      headers: {
        "Content-Type": "application/json",
        "X-Role": "admin",
      },
      data: {
        query: `{ __type(name: "PS__Pets") { fields { name } } }`,
      },
    });

    expect(resp.ok()).toBeTruthy();
    const body = await resp.json();
    expect(body.errors).toBeUndefined();

    const fields: string[] = (body.data.__type.fields ?? []).map(
      (f: { name: string }) => f.name
    );
    expect(fields).toContain("assignment");
  });

  test("PS__Pets.assignment resolves without error", async ({ request }) => {
    const resp = await request.post("http://localhost:8000/data/graphql", {
      headers: {
        "Content-Type": "application/json",
        "X-Role": "admin",
      },
      data: {
        query: `{ ps__pets(limit: 1) { id assignment { id } } }`,
      },
    });

    expect(resp.ok()).toBeTruthy();
    const body = await resp.json();
    expect(body.errors).toBeUndefined();
    expect(body.data).toHaveProperty("ps__pets");
  });
});
