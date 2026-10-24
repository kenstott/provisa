// Copyright (c) 2026 Kenneth Stott
// Canary: 1d7f4a92-6e38-4b05-9c21-a8e3f5b7d064
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
 * REQ-2127, REQ-2128: a data product's detail panel and its graph, against the real backend and
 * its real meta views (data_products_meta and its relationships, data_product_edges,
 * checked_table_id). Nothing is stubbed: the e2e config registers the pet store's "Pet Health"
 * product, whose members are pets and vets, whose command is add_pet and whose checkers
 * pets_quality and vets_quality scan pets and vets.
 *
 * The panel's core is read from the meta domain as the user: the GraphQL query it runs, replayed as
 * the same role, answers what the panel was answered.
 */

const PRODUCT = "pet_health";

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

test.describe("a data product's panel and graph, read from the meta domain", () => {
  // The first navigation compiles the page's chunks and the panel's reads run after the list's.
  test.setTimeout(180_000);

  test("the panel's core is the meta domain's answer, and its GraphQL query replayed answers the same", async ({
    page,
  }) => {
    const panelRead = page.waitForRequest(
      (r) => r.url().endsWith("/data/graphql") && (r.postData() ?? "").includes("DataProductPanel"),
    );
    const panelAnswer = page.waitForResponse(
      (r) =>
        r.url().endsWith("/data/graphql") &&
        (r.request().postData() ?? "").includes("DataProductPanel"),
    );
    const refused = recordRefusals(page);
    await page.goto(`/data-products?product=${PRODUCT}`);
    await shows(page.getByTestId(`data-product-detail-${PRODUCT}`), refused, 90_000);

    // The product's own fields (the SQL read of its row).
    await expect(page.getByTestId(`data-product-read-${PRODUCT}`)).toContainText("Pet Health", {
      timeout: 60_000,
    });
    // Its members, its command, and its checkers with the table each scans (the nested read).
    const ports = page.getByTestId(`data-product-detail-tables-${PRODUCT}`);
    await expect(ports).toContainText("pet-store.pets");
    await expect(ports).toContainText("pet-store.vets");
    await expect(ports).not.toContainText("pets_quality");
    await expect(page.getByTestId(`data-product-detail-commands-${PRODUCT}`)).toContainText(
      "add_pet",
    );
    const dq = page.getByTestId(`data-product-detail-dq-tables-${PRODUCT}`);
    await expect(dq).toContainText("pets_quality");
    await expect(dq).toContainText("scans pet-store.pets");

    // The GraphQL the panel ran, replayed as the same role, answers the same.
    const sent = await panelRead;
    const shown: string = JSON.parse(sent.postData() ?? "{}").query;
    const answered = await (await panelAnswer).json();
    const replay = await page.request.post("/data/graphql", {
      headers: {
        "Content-Type": "application/json",
        "X-Provisa-Role": (await sent.headerValue("x-provisa-role")) ?? "",
      },
      data: { query: shown, variables: {} },
    });
    expect(replay.ok()).toBe(true);
    expect(await replay.json()).toEqual(answered);
  });

  test("the panel's graph loads in view, and expands to the full graph on the product", async ({
    page,
  }) => {
    const refused = recordRefusals(page);
    await page.goto(`/data-products?product=${PRODUCT}`);
    await shows(page.getByTestId(`data-product-detail-${PRODUCT}`), refused, 90_000);
    const kg = page.getByTestId("kg-panel");
    await kg.scrollIntoViewIfNeeded();
    // data-ready: three.js has loaded and the scene is drawn.
    await shows(page.locator('[data-testid="kg-panel-stage"][data-ready]'), refused, 60_000);
    // The card is compact: its legend names what it draws (the product, its tables, its command),
    // and the explorer's controls are only in its expanded view.
    const legend = page.getByTestId("kg-panel-stage").locator(".p3d-legend");
    await expect(legend).toContainText("data product");
    await expect(legend).toContainText("member table");
    await expect(legend).toContainText("command");
    await expect(legend).not.toContainText("data quality check");
    await expect(page.getByTestId("kg-panel-layers-menu")).toHaveCount(0);

    await page.getByTestId("kg-panel-expand").click();
    await shows(page.locator('[data-testid="graph3d-stage"][data-ready]'), refused, 60_000);
    // Focused on the product, on the Knowledge graph preset, with the layers it has data for.
    await expect(page.getByTestId("graph3d-detail")).toContainText("Pet Health");
    const expanded = page.getByTestId("graph3d");
    await expect(expanded.getByRole("radio", { name: "Knowledge graph" })).toBeChecked();
    // The layers it has data for are offered in its Layers menu, and on.
    await page.getByTestId("graph3d-layers-menu").click();
    for (const layer of ["products", "tables", "commands", "quality"]) {
      await expect(page.getByTestId(`graph3d-layers-menu-option-${layer}`)).toHaveAttribute(
        "data-combobox-active",
      );
    }
  });

  test("the Product map draws every product", async ({ page }) => {
    const refused = recordRefusals(page);
    await page.goto("/data-products");
    await page.getByTestId("product-map-open").click({ timeout: 90_000 });
    await shows(page.locator('[data-testid="graph3d-stage"][data-ready]'), refused, 60_000);
    await expect(page.getByRole("radio", { name: "Product map" })).toBeChecked();
    // Both of the config's products, and their domains, are in the map's counts.
    const shown = page.getByTestId("graph3d").locator(".p3d-detail");
    await expect(shown).toContainText("data product");
    await expect(shown.locator(".p3d-row", { hasText: "data product" })).toContainText("2");
  });
});
