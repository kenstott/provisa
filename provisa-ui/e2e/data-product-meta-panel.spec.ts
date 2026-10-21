// Copyright (c) 2026 Kenneth Stott
// Canary: 1d7f4a92-6e38-4b05-9c21-a8e3f5b7d064
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * REQ-2127, REQ-2128: a data product's detail panel and its graph, against the real backend and
 * its real meta views (data_products_meta and its relationships, data_product_edges,
 * checked_table_id). Nothing is stubbed: the e2e config registers the pet store's "Pet Health"
 * product, whose members are pets and vets, whose command is add_pet and whose checkers
 * pets_quality and vets_quality scan pets and vets.
 *
 * The panel's core is read from the meta domain as the user, and the GraphQL query it shows is the
 * query it runs: replayed as the same role, it answers what the panel was answered.
 */

const PRODUCT = "pet_health";

test.describe("a data product's panel and graph, read from the meta domain", () => {
  // The first navigation compiles the page's chunks and the panel's reads run after the list's.
  test.setTimeout(180_000);

  test("the panel's core is the meta domain's answer, and its GraphQL query answers the same", async ({
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
    await page.goto(`/data-products?product=${PRODUCT}`);
    const panel = page.getByTestId(`data-product-detail-${PRODUCT}`);
    await expect(panel).toBeVisible({ timeout: 90_000 });

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

    // The GraphQL the panel shows is the query it ran, answered as the same role.
    const shown = await page
      .getByTestId(`data-product-detail-meta-queries-${PRODUCT}-graphql`)
      .textContent();
    const sent = await panelRead;
    expect(JSON.parse(sent.postData() ?? "{}").query).toBe(shown);
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
    await page.goto(`/data-products?product=${PRODUCT}`);
    await expect(page.getByTestId(`data-product-detail-${PRODUCT}`)).toBeVisible({
      timeout: 90_000,
    });
    const kg = page.getByTestId("kg-panel");
    await kg.scrollIntoViewIfNeeded();
    // data-ready: three.js has loaded and the scene is drawn.
    await expect(page.locator('[data-testid="kg-panel-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });
    // The panel's legend names what it draws: the product, its tables, its command.
    const legend = page.getByTestId("kg-panel-stage").locator(".p3d-legend");
    await expect(legend).toContainText("data product");
    await expect(legend).toContainText("member table");
    await expect(legend).toContainText("command");
    await expect(legend).not.toContainText("data quality check");

    await page.getByTestId("kg-panel-expand").click();
    await expect(page.locator('[data-testid="graph3d-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });
    // Focused on the product, on the Knowledge graph preset, with the layers it has data for.
    await expect(page.getByTestId("graph3d-detail")).toContainText("Pet Health");
    await expect(page.getByRole("radio", { name: "Knowledge graph" })).toBeChecked();
    for (const layer of ["products", "tables", "commands", "quality"]) {
      await expect(page.getByTestId(`graph3d-layer-${layer}`)).toBeVisible();
    }
  });

  test("the Product map draws every product", async ({ page }) => {
    await page.goto("/data-products");
    await page.getByTestId("product-map-open").click({ timeout: 90_000 });
    await expect(page.locator('[data-testid="graph3d-stage"][data-ready]')).toBeVisible({
      timeout: 60_000,
    });
    await expect(page.getByRole("radio", { name: "Product map" })).toBeChecked();
    // Both of the config's products, and their domains, are in the map's counts.
    const shown = page.getByTestId("graph3d").locator(".p3d-detail");
    await expect(shown).toContainText("data product");
    await expect(shown.locator(".p3d-row", { hasText: "data product" })).toContainText("2");
  });
});
