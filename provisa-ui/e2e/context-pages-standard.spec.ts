// Copyright (c) 2026 Kenneth Stott
// Canary: cea049d6-d71e-42b4-8ffe-337fe7acc3ab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: every Context page, and the Admin pages that share its Tags and Classification lists,
// draws the standard header -- the title, the filter centred on it, the actions at its right edge
// -- at 1280 and at 1900 wide, with nothing pushing the page wider than the window.

import { test, expect } from "./coverage";

test.describe.configure({ timeout: 180_000 });

const PAGES: { route: string; filter: boolean }[] = [
  { route: "/context/health", filter: false },
  { route: "/context/glossary", filter: true },
  { route: "/context/ontology", filter: true },
  { route: "/context/metrics", filter: true },
  { route: "/context/tags", filter: true },
  { route: "/context/classification", filter: true },
  { route: "/context/documents", filter: true },
  { route: "/context/sync", filter: true },
  { route: "/admin/tags", filter: true },
  { route: "/admin/classification-kinds", filter: true },
];

for (const width of [1280, 1900]) {
  test(`the Context pages draw the standard header at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 900 });
    for (const { route, filter } of PAGES) {
      await page.goto(route);
      // Behind a CapabilityGate: the header renders once the identity bootstrap resolves.
      const header = page.locator(".page-header").first();
      await expect(header, route).toBeVisible({ timeout: 90_000 });
      await expect(header.getByTestId("page-title"), route).toBeVisible();
      await testInfo.attach(`${route.replaceAll("/", "_")}-${width}.png`, {
        body: await page.screenshot({ fullPage: false }),
        contentType: "image/png",
      });
      const layout = await page.evaluate(() => {
        const head = document.querySelector(".page-header")!.getBoundingClientRect();
        const search = document.querySelector(".page-header .search-wrap");
        const actions = document.querySelector(".page-header .page-actions");
        const box = (e: Element | null) => (e === null ? null : e.getBoundingClientRect());
        const s = box(search);
        const a = box(actions);
        return {
          headCenter: head.left + head.width / 2,
          headRight: head.right,
          searchCenter: s === null ? null : s.left + s.width / 2,
          actionsRight: a === null || a.width === 0 ? null : a.right,
          overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        };
      });
      expect(layout.overflow, `${route}: page wider than the window`).toBeLessThanOrEqual(0);
      if (filter) {
        expect(layout.searchCenter, `${route}: filter in the header`).not.toBeNull();
        expect(
          Math.abs((layout.searchCenter as number) - layout.headCenter),
          `${route}: filter centred on the header`,
        ).toBeLessThan(20);
      }
      if (layout.actionsRight !== null)
        expect(
          Math.abs(layout.actionsRight - layout.headRight),
          `${route}: actions at the header's right edge`,
        ).toBeLessThan(4);
    }
  });
}
