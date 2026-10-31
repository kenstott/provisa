// Copyright (c) 2026 Kenneth Stott
// Canary: 0e7b5c93-8d16-4a2f-b4e8-1c9a6f3d7e52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: the SPARQL tab under Explore -- the graph visualizer with the role's SPARQL
// vocabulary in its sidebar. A class pill runs its own query against /data/sparql as a result card,
// and a query typed in the bar runs there too: a SELECT answered as rows, a CONSTRUCT drawn.

import type { Page, Request } from "@playwright/test";
import { test, expect } from "./coverage";

/** The vocabulary's sections start closed: the meta domain's classes, opened. */
async function openMetaClasses(page: Page) {
  const vocabulary = page.getByTestId("sparql-vocabulary");
  await vocabulary.getByTestId("sparql-vocab-classes-toggle").click();
  await vocabulary.getByTestId("sparql-class-prefix-meta").click();
}

test.describe("the SPARQL tab", () => {
  test("shows the role's vocabulary, and a class pill's query runs", async ({ page }) => {
    await page.goto("/sparql");
    const vocabulary = page.getByTestId("sparql-vocabulary");
    // OnboardGate withholds the page until the auth bootstrap settles.
    await expect(vocabulary).toBeVisible({ timeout: 60_000 });
    // The meta domain's Domains table: always readable, and needing no parameter (the first class
    // listed may be a parameterized table, which a read gives no value for and which is refused).
    await openMetaClasses(page);
    const pill = vocabulary.getByTestId("sparql-class-meta:domains");
    await expect(pill, "the role reads the meta domain's class").toBeVisible({ timeout: 30_000 });
    const shown = (await pill.textContent())?.split("⊂")[0].trim() ?? "";
    const asked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sparql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    const cards = page.locator(".graph-frame");
    const before = await cards.count();
    await pill.click();
    const request = await asked;
    expect(request.headers()["content-type"]).toContain("application/sparql-query");
    expect(request.postData()).toContain(`?s a ${shown} .`);
    expect(request.postData()).toContain("LIMIT 25");
    const response = await request.response();
    expect(response?.status(), "the SELECT is answered").toBe(200);
    const body = (await response?.json()) as { head: { vars: string[] } };
    expect(body.head.vars).toEqual(expect.arrayContaining(["s"]));
    await expect(cards, "the pill's query is a result card of its own").toHaveCount(before + 1);
  });

  test("draws a CONSTRUCT's triples", async ({ page }) => {
    await page.goto("/sparql");
    await expect(page.getByTestId("sparql-vocabulary")).toBeVisible({ timeout: 60_000 });
    // A CONSTRUCT of a subject's triples, cut short, typed into the bar. Its subject is given a
    // class: untyped, ?s ranges over every class, past the graph union limit, and is refused.
    // The meta domain's Domains table: a table's class that needs no parameter. Its pill runs its
    // own query, whose PREFIX lines the typed CONSTRUCT then reuses.
    await openMetaClasses(page);
    const tableClass = page
      .getByTestId("sparql-vocabulary")
      .getByTestId("sparql-class-meta:domains");
    await expect(tableClass, "the role reads the meta domain's table class").toBeVisible({
      timeout: 30_000,
    });
    const shown = (await tableClass.textContent())?.split("⊂")[0].trim() ?? "";
    const pillAsked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sparql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await tableClass.click();
    const prefixes = ((await pillAsked).postData() ?? "")
      .split("\n")
      .filter((l) => l.startsWith("PREFIX"))
      .join(" ");
    await page.locator(".graph-query-bar .gf-header-query-collapsed").click();
    await page.locator(".graph-query-bar .cm-content").click();
    await page.keyboard.press("ControlOrMeta+a");
    await page.keyboard.insertText(
      `${prefixes} CONSTRUCT { ?s ?p ?o } WHERE { ?s a ${shown} . ?s ?p ?o } LIMIT 10`,
    );
    const asked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sparql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await page.locator(".graph-run-btn").click();
    const response = await (await asked).response();
    expect(response?.status(), "the CONSTRUCT is answered").toBe(200);
    expect(response?.headers()["content-type"]).toContain("application/n-triples");
    const triples = ((await response?.text()) ?? "").split("\n").filter((l) => l.trim());
    expect(triples.length, "the store holds triples to draw").toBeGreaterThan(0);
    // The frame draws them: its canvas, not a table alone.
    await expect(page.locator(".graph-frame canvas, .gf-canvas canvas").first()).toBeVisible({
      timeout: 30_000,
    });
  });
});
