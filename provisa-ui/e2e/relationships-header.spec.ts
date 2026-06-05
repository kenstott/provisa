// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

async function checkHeaderLayout(page: import("@playwright/test").Page, route: string, screenshotName: string) {
  await page.goto(route);
  await page.waitForSelector(".page-header", { timeout: 10000 });
  await page.waitForSelector(".data-table", { timeout: 5000 });

  await page.screenshot({
    path: `e2e/${screenshotName}.png`,
    fullPage: false,
    clip: { x: 0, y: 0, width: 1280, height: 200 },
  });

  return page.evaluate(() => {
    const header = document.querySelector(".page-header")!.getBoundingClientRect();
    const filter = document.querySelector(".search-wrap")!.getBoundingClientRect();
    const actions = document.querySelector(".page-actions")!.getBoundingClientRect();
    return {
      header: { right: header.right, centerX: header.left + header.width / 2 },
      filter: { centerX: filter.left + filter.width / 2 },
      actions: { right: actions.right },
    };
  });
}

test("relationships header: filter centered, buttons at right edge", async ({ page }) => {
  const layout = await checkHeaderLayout(page, "/relationships", "relationships-header");
  console.log("relationships:", JSON.stringify(layout));
  expect(Math.abs(layout.filter.centerX - layout.header.centerX), `filter vs header centerX`).toBeLessThan(20);
  expect(Math.abs(layout.actions.right - layout.header.right), `actions.right vs header.right`).toBeLessThan(4);
});

test("tables header: filter centered, buttons at right edge", async ({ page }) => {
  const layout = await checkHeaderLayout(page, "/tables", "tables-header");
  console.log("tables:", JSON.stringify(layout));
  expect(Math.abs(layout.filter.centerX - layout.header.centerX), `filter vs header centerX`).toBeLessThan(20);
  expect(Math.abs(layout.actions.right - layout.header.right), `actions.right vs header.right`).toBeLessThan(4);
});

test("sources header: filter centered, buttons at right edge", async ({ page }) => {
  const layout = await checkHeaderLayout(page, "/sources", "sources-header");
  console.log("sources:", JSON.stringify(layout));
  expect(Math.abs(layout.filter.centerX - layout.header.centerX), `filter vs header centerX`).toBeLessThan(20);
  expect(Math.abs(layout.actions.right - layout.header.right), `actions.right vs header.right`).toBeLessThan(4);
});
