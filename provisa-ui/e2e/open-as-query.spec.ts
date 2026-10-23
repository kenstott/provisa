// Copyright (c) 2026 Kenneth Stott
// Canary: 45aa86ad-cf1a-441a-840f-6ff2d71026f2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, type Page } from "./coverage";
import type { Request } from "@playwright/test";

/**
 * REQ-2136: a graph view opens as its own query, its filters inside it. Against the real backend
 * and its meta views: the Ontology View filtered (Taxonomy's two edge types, Orphans off) reads
 * one query; Open as query → GraphQL hands that query to the GraphQL page, and run there it
 * answers exactly what the view read and drew from.
 */

/** The next governed GraphQL read whose query satisfies ``match``, with its answer. */
async function nextRead(page: Page, match: (query: string) => boolean, act: () => Promise<void>) {
  const asked = page.waitForRequest(
    (r: Request) =>
      new URL(r.url()).pathname === "/data/graphql" &&
      r.method() === "POST" &&
      match((r.postDataJSON() as { query?: string } | null)?.query ?? ""),
    { timeout: 60_000 },
  );
  await act();
  const request = await asked;
  const response = await request.response();
  expect(response?.status(), "the read is answered").toBe(200);
  const body = (await response?.json()) as { data?: unknown; errors?: unknown };
  expect(body.errors, "the read is answered without errors").toBeUndefined();
  return { query: (request.postDataJSON() as { query: string }).query, data: body.data };
}

test.describe("Open as query", () => {
  test.setTimeout(180_000);

  test("the Ontology View's filtered query, run on the GraphQL page, answers what the view read", async ({
    page,
  }) => {
    await page.goto("/admin/ontology");
    await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 90_000 });
    await page.getByTestId("glossary-ontology-toggle").click();
    await expect(page.locator('[data-testid="ontology-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });

    // Taxonomy: two edge types and the Ontology layer; then Orphans off, all in the query's where.
    await nextRead(
      page,
      (q) => q.startsWith("query Ontology") && q.includes("relType"),
      () => page.getByTestId("ontology-preset").getByText("Taxonomy", { exact: true }).click(),
    );
    const viewed = await nextRead(
      page,
      (q) => q.startsWith("query Ontology") && q.includes("edgesOut"),
      () => page.getByTestId("ontology-orphans").click(),
    );
    expect(viewed.query).toContain('relType: { in: ["KIND_OF","PART_OF"] }');

    await page.getByTestId("ontology-open-as-query").click();
    await page.getByTestId("ontology-open-as-graphql").click();
    await page.waitForURL("**/query", { timeout: 60_000 });

    const run = await nextRead(
      page,
      (q) => q === viewed.query,
      async () => {
        await expect(page.locator(".graphiql-execute-button")).toBeEnabled({ timeout: 60_000 });
        // The editor holds the view's query once the hand-off has opened its tab.
        await expect(page.locator(".graphiql-query-editor")).toContainText("query Ontology", {
          timeout: 60_000,
        });
        await page.locator(".graphiql-execute-button").click();
      },
    );
    expect(run.data).toEqual(viewed.data);
  });
});
