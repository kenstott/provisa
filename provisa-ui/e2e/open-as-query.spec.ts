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

/** The Ontology View filtered (Taxonomy's two edge types, Unconnected off): the query it read and its
 * answer. One root: the terms, each with the edges leaving it. */
async function filteredView(page: Page) {
  await page.goto("/context/ontology");
  await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 90_000 });
  await page.getByTestId("glossary-ontology-toggle").click();
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
    await page.getByTestId("glossary-ontology-toggle").click();
    await expect(page.locator('[data-testid="ontology-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });
    // REQ-2143: the search is the query's root filter. REQ-2174 (amended 2026-11-26): its
    // neighbourhood is nested under it, both ways, one root field read in one round trip. "clinic"
    // has a neighbourhood whatever else the org holds: the config makes it PART_OF referral, and
    // this suite's beforeEach relates it to specialty.
    const viewed = await nextRead(
      page,
      (q) => ontologyRead(q) && q.includes("ilike"),
      () => page.getByTestId("glossary-search").fill("clinic"),
    );
    expect(viewed.query).toContain('ilike: "%clinic%"');
    expect(Object.keys(viewed.data as Record<string, unknown>), "one root field").toEqual([
      "_terms",
    ]);
    const terms = (viewed.data as { _terms: { name: string }[] })._terms;
    expect(terms.map((t) => t.name)).toContain("clinic");
    // The neighbourhood is nested in the one read: a term a step from clinic is answered in it.
    expect(strings(viewed.data)).toContain("referral");

    // Open as query is offered once the search's read is in: what it opens is what it read.
    await expect(page.getByTestId("ontology-open-as-query")).toBeEnabled({ timeout: 60_000 });
    await page.getByTestId("ontology-open-as-query").click();
    await page.getByTestId("ontology-open-as-graphql").click();
    await page.waitForURL("**/query", { timeout: 60_000 });
    // REQ-2136: the view opens as the one query that read it, and run, it answers what it read.
    const run = await nextRead(
      page,
      (q) => q === viewed.query,
      async () => {
        await expect(page.locator(".graphiql-execute-button")).toBeEnabled({ timeout: 60_000 });
        await expect(page.locator(".graphiql-query-editor")).toContainText("query Ontology", {
          timeout: 60_000,
        });
        await page.locator(".graphiql-execute-button").click();
      },
    );
    expect(run.data).toEqual(viewed.data);
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

  test("its SPARQL, written from the view's own Cypher, run on the SPARQL page, answers its terms and links", async ({
    page,
  }) => {
    // REQ-2136 (maintainer 2026-11-28): the view opens as SPARQL wherever it opens as its Cypher.
    const viewed = await filteredView(page);
    const terms = (
      viewed.data as { _terms: { id: number; edgesOut?: { relType: string }[] | null }[] }
    )._terms;

    await page.getByTestId("ontology-open-as-query").click();
    const sparql = page.getByTestId("ontology-open-as-sparql");
    await expect(sparql, "the view's Cypher is written as SPARQL").toBeEnabled({
      timeout: 60_000,
    });
    await sparql.click();
    await page.waitForURL("**/sparql", { timeout: 60_000 });

    const asked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sparql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await page.locator(".graph-run-btn").click();
    const response = await (await asked).response();
    expect(response?.status(), "the SPARQL is answered").toBe(200);
    const body = (await response?.json()) as {
      head: { vars: string[] };
      results: { bindings: Record<string, { value: string }>[] };
    };
    expect(body.head.vars).toEqual(["a", "r", "b"]);
    // The same graph: a subject per term the view read, and every edge type it drew between them.
    // A term's subject IRI ends with its key.
    const subjects = new Set(body.results.bindings.map((b) => b.a.value.split("/").pop()));
    for (const t of terms) expect(subjects, `term ${t.id}`).toContain(String(t.id));
    const types = new Set(
      body.results.bindings.flatMap((b) => (b.r ? [b.r.value.split("/").pop()] : [])),
    );
    for (const e of terms.flatMap((t) => t.edgesOut ?? []))
      expect(types, `edge ${e.relType}`).toContain(e.relType);
  });

  test("its SQL opens formatted in a fresh SQL Explorer tab and runs at once", async ({ page }) => {
    await filteredView(page);

    await page.getByTestId("ontology-open-as-query").click();
    const sqlItem = page.getByTestId("ontology-open-as-sql");
    await expect(sqlItem).toBeEnabled({ timeout: 60_000 });
    const ran = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await sqlItem.click();
    await page.waitForURL("**/sql", { timeout: 60_000 });

    // Run with no click on Run, and shown in a tab of its own beside the blank first one.
    const response = await (await ran).response();
    expect(response?.status(), "the SQL is answered").toBe(200);
    await expect(page.getByText("Query 2", { exact: true })).toBeVisible();
    // Formatted: upper-case keywords, each clause on its own line.
    const editor = page.locator(".cm-content").first();
    await expect(editor).toContainText("SELECT", { timeout: 60_000 });
    const lines = await editor.locator(".cm-line").allTextContents();
    expect(lines.length, "the statement is laid out over lines").toBeGreaterThan(3);
    expect(lines.map((l) => l.trim())).toContain("FROM");
  });
});
