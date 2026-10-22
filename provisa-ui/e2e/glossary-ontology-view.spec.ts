// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a2c61-9d37-4b05-b8e1-c3a7d6f2e914
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, type Page } from "./coverage";
import type { Locator } from "@playwright/test";

/**
 * REQ-2130: the Glossary page's Ontology View, against the real backend and its real meta views
 * (glossary_terms_meta with grounded, glossary_term_domains_meta, glossary_term_edges_meta,
 * glossary_term_refs_meta). Nothing is stubbed: the e2e config declares the pet store's and the
 * shelter's terms -- "personnel record" with "department" and "hire date" as its parts, "referral"
 * related to "clinic" and "specialty" -- and table registration derives a term per column.
 */

/** Every /data and /admin answer refused (status >= 400), whole: what a failure names. */
function recordRefusals(page: Page): string[] {
  const refused: string[] = [];
  page.on("response", async (r) => {
    const path = new URL(r.url()).pathname;
    if (r.status() < 400 || !/^\/(data|admin)\//.test(path)) return;
    const req = r.request();
    let body: string;
    try {
      body = await r.text();
    } catch (e) {
      body = `(body unreadable: ${String(e)})`;
    }
    refused.push(
      `${r.status()} ${req.method()} ${path} role=${req.headers()["x-provisa-role"] ?? "-"}` +
        `\n  sent: ${(req.postData() ?? "").slice(0, 600)}\n  answered: ${body.slice(0, 1200)}`,
    );
  });
  return refused;
}

/** ``locator`` comes into view, or the failure names every refused request on the way. */
async function shows(locator: Locator, refused: string[], timeout: number) {
  try {
    await expect(locator).toBeVisible({ timeout });
  } catch (e) {
    throw new Error(
      `${e instanceof Error ? e.message : String(e)}\n\nRefused requests (${refused.length}):\n` +
        (refused.join("\n") || "(none)"),
      { cause: e },
    );
  }
}

/** The view's legend entries, as drawn. */
async function legend(page: Page, stage: string): Promise<string[]> {
  return page.getByTestId(stage).locator(".p3d-legend span").allTextContents();
}

test.describe("the Glossary page's Ontology View, read from the meta domain", () => {
  // The first navigation compiles the admin chunk; the view's reads run once it is opened.
  test.setTimeout(180_000);

  test("opens in place of the term list, and draws the glossary's terms and domains", async ({
    page,
  }) => {
    const refused = recordRefusals(page);
    await page.goto("/admin/ontology");
    await shows(page.getByTestId("glossary-search"), refused, 90_000);
    // Nothing of the view is read or drawn before it is opened.
    await expect(page.getByTestId("ontology-view")).toHaveCount(0);
    await page.getByTestId("glossary-ontology-toggle").click();
    await shows(page.locator('[data-testid="ontology-stage"][data-ready]'), refused, 60_000);
    await expect(page.getByTestId("glossary-list")).toHaveCount(0);

    expect(await legend(page, "ontology-stage")).toEqual(
      expect.arrayContaining(["domain", "concrete term"]),
    );
    // The status filters, retired hidden until asked for.
    await expect(page.getByTestId("ontology-status-proposed")).toBeChecked();
    await expect(page.getByTestId("ontology-status-deprecated")).toBeChecked();
    await expect(page.getByTestId("ontology-status-retired")).not.toBeChecked();

    // The data layer, off by default, adds the bound columns' tables.
    await expect(page.getByTestId("ontology-layer-data")).not.toBeChecked();
    await page.getByTestId("ontology-layer-data").evaluate((el: HTMLElement) => el.click());
    await expect.poll(() => legend(page, "ontology-stage")).toContain("member table");
  });

  test("Taxonomy keeps the is-a and part-of edges, and the view expands", async ({ page }) => {
    const refused = recordRefusals(page);
    await page.goto("/admin/ontology");
    await shows(page.getByTestId("glossary-search"), refused, 90_000);
    await page.getByTestId("glossary-ontology-toggle").click();
    await shows(page.locator('[data-testid="ontology-stage"][data-ready]'), refused, 60_000);

    await page.getByTestId("ontology-preset").getByText("Taxonomy", { exact: true }).click();
    // The preset chooses its two edge types; the filter says so on one line.
    await expect(page.getByTestId("ontology-filter-edge-type")).toHaveText(/2/);
    await page.getByTestId("ontology-filter-edge-type").click();
    await expect(page.getByTestId("ontology-filter-edge-type-option-PART_OF")).toBeVisible();
    await page.keyboard.press("Escape");

    await page.getByTestId("ontology-expand").click();
    await shows(page.locator('[data-testid="graph3d-stage"][data-ready]'), refused, 60_000);
    await expect(
      page.getByTestId("graph3d-modal").getByText("Ontology", { exact: true }),
    ).toBeVisible();
  });
});
