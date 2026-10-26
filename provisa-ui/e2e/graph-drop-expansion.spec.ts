// Copyright (c) 2026 Kenneth Stott
// Canary: d41c897c-56b1-4ed3-868a-08b07709fa12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, BACKEND_URL } from "./coverage";
import type { Request } from "@playwright/test";

/**
 * REQ-1586: dragging a label onto a Cypher result expands it along the relationship types the
 * server registers for the acting role between the two labels -- the types /data/graph-schema
 * lists, as the sidebar does -- and the expansion is answered, never refused for naming a type
 * the server does not know. The maintainer's case: Shelter:Employees drawn, Shelter:Assignments
 * dropped.
 */

const DRAWN = "Shelter:Employees";
const DROPPED = "Shelter:Assignments";

test("dropping a label expands along the server's types and is answered", async ({
  page,
  request,
}) => {
  test.setTimeout(180_000);
  const schema = (await (await request.get(`${BACKEND_URL}/data/graph-schema`)).json()) as {
    relationship_types: { type: string; source: string; target: string }[];
  };
  const between = new Set(
    schema.relationship_types
      .filter(
        (r) =>
          (r.source === DRAWN && r.target === DROPPED) ||
          (r.source === DROPPED && r.target === DRAWN),
      )
      .map((r) => r.type),
  );
  expect(between.size, "the server registers a type between the two labels").toBeGreaterThan(0);

  await page.goto("/graph");
  const drawn = page.getByTestId(`graph-label-${DRAWN}`);
  await expect(drawn).toBeVisible({ timeout: 90_000 });
  const first = page.waitForResponse(
    (r) => new URL(r.url()).pathname === "/data/cypher" && r.request().method() === "POST",
    { timeout: 90_000 },
  );
  await drawn.click();
  expect((await first).status(), "the drawn label is answered").toBe(200);

  const expanded = page.waitForRequest(
    (r: Request) =>
      new URL(r.url()).pathname === "/data/cypher" &&
      r.method() === "POST" &&
      ((r.postDataJSON() as { query?: string } | null)?.query ?? "").includes("OPTIONAL MATCH"),
    { timeout: 90_000 },
  );
  await page.getByTestId(`graph-label-${DROPPED}`).dragTo(page.locator(".gf-frame").first());
  const sent = await expanded;
  const query = (sent.postDataJSON() as { query: string }).query;
  const named = [...query.matchAll(/\[\w*:([A-Z0-9_]+)\]/g)].map((m) => m[1]);
  expect(named.length, query).toBeGreaterThan(0);
  for (const type of named) expect(between.has(type), `${type} in ${query}`).toBe(true);
  const answered = await sent.response();
  expect(answered?.status(), JSON.stringify(await answered?.json()).slice(0, 400)).toBe(200);
});
