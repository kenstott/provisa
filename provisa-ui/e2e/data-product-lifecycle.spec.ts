// Copyright (c) 2026 Kenneth Stott
// Canary: 2fbf2783-6cc3-4d4b-bdbf-88bf072f2fdf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, type Page } from "./coverage";

/**
 * REQ-1634 STATUS, REQ-2146: a data product's lifecycle on its panel, against the real backend.
 * A fresh product, with no members, is taken through every action -- published, deprecated with a
 * reason, restored, retired (no successor needed: it has no members), and restored to a draft --
 * each answered by the server and shown in the list and the panel. Its findings put it under
 * To review.
 */

const PRODUCT = `e2e_lifecycle_${Date.now().toString(36)}`;

async function admin(page: Page, query: string, variables: Record<string, unknown>) {
  const res = await page.request.post("/admin/graphql", { data: { query, variables } });
  expect(res.ok(), await res.text()).toBeTruthy();
  const body = await res.json();
  expect(body.errors ?? [], JSON.stringify(body.errors)).toEqual([]);
  return body.data;
}

test.describe("a data product's lifecycle", () => {
  // The first navigation compiles the page's chunks.
  test.setTimeout(180_000);

  test.beforeEach(async ({ page }) => {
    const made = await admin(
      page,
      "mutation($input: DataProductInput!) { createDataProduct(input: $input) { success message } }",
      {
        input: {
          id: PRODUCT,
          domainId: "pet-store",
          name: "E2E Lifecycle",
          purpose: "",
          limitations: "",
          usage: "",
          customProperties: {},
        },
      },
    );
    expect(made.createDataProduct.success, made.createDataProduct.message).toBe(true);
  });

  test.afterEach(async ({ page }) => {
    await admin(page, "mutation($id: String!) { deleteDataProduct(id: $id) { success } }", {
      id: PRODUCT,
    });
  });

  test("takes a product through every status, each change answered and shown", async ({ page }) => {
    await page.goto(`/data-products?product=${PRODUCT}`);
    const badge = page.getByTestId(`data-products-status-${PRODUCT}`);
    const status = page.getByTestId(`data-product-status-${PRODUCT}`);
    await expect(badge).toHaveText("Draft", { timeout: 90_000 });
    await expect(status).toHaveText("Draft");

    // A new product with no members, owner or description is To review.
    await expect(page.getByTestId(`data-product-${PRODUCT}-to-review`)).toBeVisible();
    await page.getByTestId("data-products-to-review-only").check();
    await expect(page.getByTestId(`data-products-row-${PRODUCT}`)).toBeVisible();
    await page.getByTestId("data-products-to-review-only").uncheck();

    await page.getByTestId(`data-product-publish-${PRODUCT}`).click();
    await expect(badge).toHaveText("Live");

    await page.getByTestId(`data-product-deprecate-${PRODUCT}`).click();
    await page.getByTestId("data-product-deprecate-reason").fill("Replaced next quarter.");
    await page.getByTestId("data-product-deprecate-submit").click();
    await expect(badge).toHaveText("Deprecated");
    await expect(page.getByTestId(`data-product-read-${PRODUCT}`)).toContainText(
      "Replaced next quarter.",
    );

    await page.getByTestId(`data-product-restore-${PRODUCT}`).click();
    await expect(badge).toHaveText("Live");

    // No members: retiring it needs no successor.
    await page.getByTestId(`data-product-retire-${PRODUCT}`).click();
    await expect(page.getByTestId("data-product-retire-moves")).toContainText(
      "a successor is optional",
    );
    await page.getByTestId("data-product-retire-reason").fill("No longer offered.");
    await page.getByTestId("data-product-retire-submit").click();
    await expect(badge).toHaveText("Retired");

    // A retired product comes back a draft.
    await page.getByTestId(`data-product-restore-${PRODUCT}`).click();
    await expect(badge).toHaveText("Draft");
    await expect(page.getByTestId(`data-product-publish-${PRODUCT}`)).toBeVisible();
  });
});
