// Copyright (c) 2026 Kenneth Stott
// Canary: e3e7dd96-276a-4c37-aa6c-cc5650052eb2
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// The header wait below budgets 60 s for the identity bootstrap; the 30 s default per-test timeout
// would expire before that budget could be spent.
test.describe.configure({ timeout: 120_000 });

async function checkHeaderLayout(page: import("@playwright/test").Page, route: string, screenshotName: string) {
  await page.goto(route);
  // These routes sit behind a CapabilityGate, so the header does not render until the identity
  // bootstrap resolves — app boot, /setup/status, /auth/me and the roles query. That is not what
  // this spec measures; the layout assertions below run once the header is up, whenever that is.
  await page.waitForSelector(".page-header", { timeout: 60000 });
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
