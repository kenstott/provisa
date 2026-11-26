// Copyright (c) 2026 Kenneth Stott
// Canary: 15036868-0cf8-4710-89fc-81503dea1a84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: every Admin page that manages objects draws the standard header -- the title, the
// filter centred on it, the create action at its right edge -- over its list, at 1280 and at 1900
// wide, with nothing pushing the page wider than the window.

import { writeFileSync } from "node:fs";
import { test, expect } from "./coverage";

test.describe.configure({ timeout: 240_000 });

const PAGES: string[] = [
  "/admin/orgs",
  "/team",
  "/admin/domains",
  "/admin/environments",
  "/admin/scheduled-tasks",
  "/admin/secrets",
  // /admin/my-secrets is refused (403) on a deployment without authentication, as this one is.
  "/admin/platform",
  "/admin/package-registries",
  "/admin/container-registries",
  "/admin/reports",
  "/admin/requests",
];

for (const width of [1280, 1900]) {
  test(`the Admin object pages draw the standard header at ${width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    for (const route of PAGES) {
      await page.goto(route);
      // The pre-React splash stands until the app has mounted.
      await expect(page.locator("#provisa-splash")).toHaveCount(0, { timeout: 60_000 });
      const header = page.locator(".page-header").first();
      await expect(header, route).toBeVisible({ timeout: 90_000 });
      await expect(header.getByTestId("page-title"), route).toBeVisible();
      await expect(header.locator(".search-wrap"), route).toBeVisible({ timeout: 30_000 });
      const shot = await page.screenshot({ fullPage: false });
      if (process.env.PROVISA_E2E_RENDER_DIR)
        writeFileSync(
          `${process.env.PROVISA_E2E_RENDER_DIR}/${route.replaceAll("/", "_")}-${width}.png`,
          shot,
        );
      await testInfo.attach(`${route.replaceAll("/", "_")}-${width}.png`, {
        body: shot,
        contentType: "image/png",
      });
      const layout = await page.evaluate(() => {
        const head = document.querySelector(".page-header")!.getBoundingClientRect();
        const s = document.querySelector(".page-header .search-wrap")!.getBoundingClientRect();
        const actions = document.querySelector(".page-header .page-actions");
        const a = actions === null ? null : actions.getBoundingClientRect();
        return {
          headCenter: head.left + head.width / 2,
          headRight: head.right,
          searchCenter: s.left + s.width / 2,
          actionsRight: a === null || a.width === 0 ? null : a.right,
          overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        };
      });
      expect(layout.overflow, `${route}: page wider than the window`).toBeLessThanOrEqual(0);
      expect(
        Math.abs(layout.searchCenter - layout.headCenter),
        `${route}: filter centred on the header`,
      ).toBeLessThan(20);
      if (layout.actionsRight !== null)
        expect(
          Math.abs(layout.actionsRight - layout.headRight),
          `${route}: actions at the header's right edge`,
        ).toBeLessThan(4);
    }
  });
}
