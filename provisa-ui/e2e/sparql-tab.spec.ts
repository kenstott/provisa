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
// vocabulary in its sidebar. A class pill adds its triple pattern to the query in the bar, and the
// query runs against /data/sparql: a SELECT answered as rows, a CONSTRUCT drawn.

import type { Request } from "@playwright/test";
import { test, expect } from "./coverage";

test.describe("the SPARQL tab", () => {
  test("shows the role's vocabulary, and a class pill's query runs", async ({ page }) => {
    await page.goto("/sparql");
    const vocabulary = page.getByTestId("sparql-vocabulary");
    // OnboardGate withholds the page until the auth bootstrap settles.
    await expect(vocabulary).toBeVisible({ timeout: 60_000 });
    const classes = vocabulary.locator('[data-testid^="sparql-class-"]');
    await expect(classes.first(), "the role reads at least one class").toBeVisible({
      timeout: 30_000,
    });
    const pill = classes.first();
    const shown = (await pill.textContent())?.split("⊂")[0].trim() ?? "";
    await pill.click();
    // The bar holds the pill's pattern, its PREFIX declared.
    const bar = page.locator(".gf-header-query-collapsed").first();
    await expect(bar).toContainText(`?s a ${shown} .`);

    const asked = page.waitForRequest(
      (r: Request) => new URL(r.url()).pathname === "/data/sparql" && r.method() === "POST",
      { timeout: 60_000 },
    );
    await page.locator(".graph-run-btn").click();
    const request = await asked;
    expect(request.headers()["content-type"]).toContain("application/sparql-query");
    expect(request.postData()).toContain(`?s a ${shown} .`);
    const response = await request.response();
    expect(response?.status(), "the SELECT is answered").toBe(200);
    const body = (await response?.json()) as { head: { vars: string[] } };
    expect(body.head.vars).toEqual(expect.arrayContaining(["s", "p", "o"]));
  });

  test("draws a CONSTRUCT's triples", async ({ page }) => {
    await page.goto("/sparql");
    await expect(page.getByTestId("sparql-vocabulary")).toBeVisible({ timeout: 60_000 });
    // Typed into the bar: a CONSTRUCT of a subject's triples, cut short. Its subject is given a
    // class by a table class's pill, as a reader builds it: untyped, ?s ranges over every class,
    // past the graph union limit, and is refused.
    await page.locator(".graph-query-bar .gf-header-query-collapsed").click();
    const editor = page.locator(".graph-query-bar .cm-content");
    await editor.click();
    await page.keyboard.press("ControlOrMeta+a");
    await page.keyboard.insertText("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 10");
    const tableClass = page
      .getByTestId("sparql-vocabulary")
      .locator('[data-testid^="sparql-class-"]:not([data-testid^="sparql-class-term:"])')
      .first();
    await expect(tableClass, "the role reads at least one table's class").toBeVisible({
      timeout: 30_000,
    });
    const shown = (await tableClass.textContent())?.split("⊂")[0].trim() ?? "";
    await tableClass.click();
    await expect(page.locator(".gf-header-query-collapsed").first()).toContainText(
      `?s a ${shown} .`,
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
