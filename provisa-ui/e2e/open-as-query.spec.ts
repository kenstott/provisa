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
import type { Request, Response } from "@playwright/test";
import { addLateralEdge } from "./ontology-lateral-edge";

/**
 * REQ-2136: a graph view opens as its own query, its filters inside it. Against the real backend
 * and its meta views: the Ontology View filtered (Taxonomy's two edge types, Unconnected off) reads
 * one query with one root; Open as query → GraphQL hands that query to the GraphQL page, and run
 * there it answers exactly what the view read and drew from; → Cypher hands its one pattern to the
 * Cypher page, which answers the same terms and edges.
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

/** The view's read: the Ontology operation, not the OntologyCount read it makes first. */
const ontologyRead = (q: string) => /^query Ontology[\s({]/.test(q);

/** A hop of a search's neighbourhood, read after its root by the ids the hop before reached
 * (REQ-2143): its number, or null for any other read. */
const hopNumber = (q: string) => {
  const m = /^query OntologyHop(\d+)[\s({]/.exec(q);
  return m === null ? null : Number(m[1]);
};

/** Every governed GraphQL read answered from now on whose query satisfies ``match``, with its
 * answer, in the order asked; ``stop`` ends the collecting. */
function collectReads(page: Page, match: (query: string) => boolean) {
  const answered: Promise<{ query: string; data: Record<string, unknown> }>[] = [];
  const listener = (response: Response) => {
    const request = response.request();
    if (new URL(request.url()).pathname !== "/data/graphql" || request.method() !== "POST") return;
    const query = (request.postDataJSON() as { query?: string } | null)?.query ?? "";
    if (!match(query)) return;
    answered.push(
      response.json().then((body: { data?: Record<string, unknown>; errors?: unknown }) => {
        expect(response.status(), "the read is answered").toBe(200);
        expect(body.errors, "the read is answered without errors").toBeUndefined();
        return { query, data: body.data ?? {} };
      }),
    );
  };
  page.on("response", listener);
  return {
    stop: async () => {
      page.off("response", listener);
      return Promise.all(answered);
    },
  };
}

/** The Ontology View filtered (Taxonomy's two edge types, Unconnected off): the query it read and its
 * answer. One root: the terms, each with the edges leaving it. */
async function filteredView(page: Page) {
  await page.goto("/context/ontology");
  await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 90_000 });
  await page.getByTestId("glossary-view-graph").click();
  await expect(page.locator('[data-testid="ontology-stage"][data-ready]')).toBeVisible({
    timeout: 60_000,
  });
  // Taxonomy: two edge types; then Unconnected off, all in the query's where.
  await nextRead(
    page,
    (q) => ontologyRead(q) && q.includes("relType"),
    () => page.getByTestId("ontology-preset").getByText("Taxonomy", { exact: true }).click(),
  );
  const viewed = await nextRead(
    page,
    (q) => ontologyRead(q) && q.includes("edgesOut"),
    async () => {
      await page.getByTestId("ontology-terms-menu").click();
      await page.getByTestId("ontology-terms-menu-option-unconnected").click();
      await page.keyboard.press("Escape");
    },
  );
  expect(viewed.query).toContain('relType: { in: ["KIND_OF","PART_OF"] }');
  // One root: the edges are read with their terms.
  expect(viewed.query).not.toContain("on_termEdges");
  return viewed;
}

/** Every string value in ``v``, at any depth. */
function strings(v: unknown, out = new Set<string>()): Set<string> {
  if (typeof v === "string") out.add(v);
  else if (Array.isArray(v)) for (const x of v) strings(x, out);
  else if (v && typeof v === "object") for (const x of Object.values(v)) strings(x, out);
  return out;
}

test.describe("Open as query", () => {
  test.setTimeout(180_000);

  // A third edge type for Taxonomy to leave out: with PART_OF alone in the config, its two types
  // are every type there is, and the view reads that as no narrowing (no relType in its query).
  let removeLateralEdge: (() => Promise<void>) | null = null;
  test.beforeEach(async ({ page }) => {
    removeLateralEdge = await addLateralEdge(page.request);
  });
  test.afterEach(async () => {
    const remove = removeLateralEdge;
    removeLateralEdge = null;
    if (remove) await remove();
  });

  test("the Ontology View's filtered query, run on the GraphQL page, answers what the view read", async ({
    page,
  }) => {
    const viewed = await filteredView(page);

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

  test("a searched view's query states its search and neighbourhood, and run answers what it read", async ({
    page,
  }) => {
    await page.goto("/context/ontology");
    await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 90_000 });
    await page.getByTestId("glossary-view-graph").click();
    await expect(page.locator('[data-testid="ontology-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });
    // REQ-2143: the search is the query's root filter, read first; its neighbourhood is then read
    // a hop at a time, each hop by the ids the one before reached. "clinic" has a neighbourhood
    // whatever else the org holds: the config makes it PART_OF referral, and this suite's
    // beforeEach relates it to specialty.
    const hops = collectReads(page, (q) => hopNumber(q) !== null);
    const viewed = await nextRead(
      page,
      (q) => ontologyRead(q) && q.includes("ilike"),
      () => page.getByTestId("glossary-search").fill("clinic"),
    );
    expect(viewed.query).toContain('ilike: "%clinic%"');
    const terms = (viewed.data as { _terms: { name: string }[] })._terms;
    expect(terms.map((t) => t.name)).toContain("clinic");

    // Open as query is offered once the search's reads are in: what it opens is what they read.
    await expect(page.getByTestId("ontology-open-as-query")).toBeEnabled({ timeout: 60_000 });
    const hopReads = (await hops.stop()).sort(
      (a, b) => (hopNumber(a.query) as number) - (hopNumber(b.query) as number),
    );
    expect(hopReads.length, "the search's neighbourhood is read past its root").toBeGreaterThan(0);
    expect(hopReads.map((r) => hopNumber(r.query))).toEqual(hopReads.map((_, i) => i + 1));

    await page.getByTestId("ontology-open-as-query").click();
    await page.getByTestId("ontology-open-as-graphql").click();
    await page.waitForURL("**/query", { timeout: 60_000 });
    // REQ-2136: the view opens as one query -- its root, the search in its filter, and each hop it
    // read under _termsHop<n> -- and run, it answers what the view read and drew from.
    const run = await nextRead(
      page,
      (q) => ontologyRead(q) && q.includes("ilike") && q.includes("_termsHop1:"),
      async () => {
        await expect(page.locator(".graphiql-execute-button")).toBeEnabled({ timeout: 60_000 });
        await expect(page.locator(".graphiql-query-editor")).toContainText("query Ontology", {
          timeout: 60_000,
        });
        await page.locator(".graphiql-execute-button").click();
      },
    );
    expect(run.query).toContain('ilike: "%clinic%"');
    const answered = run.data as Record<string, unknown>;
    const root = viewed.data as Record<string, unknown>;
    expect(Object.keys(answered).sort()).toEqual(
      [...Object.keys(root), ...hopReads.map((_, i) => `_termsHop${i + 1}`)].sort(),
    );
    for (const field of Object.keys(root)) expect(answered[field]).toEqual(root[field]);
    hopReads.forEach((hop, i) => expect(answered[`_termsHop${i + 1}`]).toEqual(hop.data._terms));
  });

  test("its one Cypher pattern, run on the Cypher page, answers the same terms and edges", async ({
    page,
  }) => {
    const viewed = await filteredView(page);
    const terms = (
      viewed.data as {
        _terms: { name: string; edgesOut?: { relType: string }[] | null }[];
      }
    )._terms;

    await page.getByTestId("ontology-open-as-query").click();
    const cypher = page.getByTestId("ontology-open-as-cypher");
    await expect(cypher, "one root translates to one Cypher pattern").toBeEnabled({
      timeout: 60_000,
    });
    await cypher.click();
    await page.waitForURL("**/graph", { timeout: 60_000 });

    const asked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/cypher" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await page.locator(".graph-run-btn").click();
    const response = await (await asked).response();
    expect(response?.status(), "the Cypher pattern is answered").toBe(200);
    // The Cypher page asks for each statement's own result (results: "each"); the pattern is one.
    const { results } = (await response?.json()) as {
      results: { status: number; body: { error?: unknown; rows?: unknown } }[];
    };
    expect(results, "one statement, one result").toHaveLength(1);
    const [{ status, body }] = results;
    expect(status, "the statement is answered").toBe(200);
    expect(body.error).toBeUndefined();
    // The same graph: every term the view read, and every edge type it drew between them.
    const said = strings(body.rows);
    for (const t of terms) expect(said, `term ${t.name}`).toContain(t.name);
    for (const e of terms.flatMap((t) => t.edgesOut ?? []))
      expect(said, `edge ${e.relType}`).toContain(e.relType);
  });
});
