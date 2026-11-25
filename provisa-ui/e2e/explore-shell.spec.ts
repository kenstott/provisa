// Copyright (c) 2026 Kenneth Stott
// Canary: 5d9e2b70-1c84-4a3f-b6e7-3f0a8c2d9e15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: every Explore surface stands in the one Explore shell, at 1280 and 1900 wide. Its
// gutter is OData's (the management pages' --page-gutter); no card is drawn around its result
// panel; its side panel and its editor above the results are resized by dragging the divider, and
// the size is kept across a reload.

import { test, expect, type Page } from "./coverage";

test.describe.configure({ timeout: 300_000 });

interface Surface {
  route: string;
  /** The shell's test id. */
  shell: string;
  /** The result panel's test id, where the surface draws one on opening. */
  results?: string;
  /** The side panel's splitter, where it has one. */
  side?: string;
  /** The editor/results splitter, where it has one. */
  stack?: string;
}

const SURFACES: Surface[] = [
  { route: "/odata", shell: "odata-page" },
  { route: "/query", shell: "query-page" },
  { route: "/graph", shell: "graph-page", results: "cypher-results", side: "graph-sidebar-handle" },
  {
    route: "/sparql",
    shell: "sparql-page",
    results: "sparql-results",
    side: "graph-sidebar-handle",
  },
  {
    route: "/sql",
    shell: "sql-page",
    results: "sql-results",
    side: "sql-schema-side-handle",
    stack: "sql-stack-handle",
  },
  { route: "/grpc", shell: "grpc-page", results: "grpc-response", side: "grpc-side-handle" },
  {
    route: "/jsonapi",
    shell: "jsonapi-page",
    results: "jsonapi-response",
    side: "jsonapi-side-handle",
  },
  { route: "/openapi", shell: "openapi-page" },
  { route: "/explore", shell: "mcp-explore" },
  { route: "/nl", shell: "nl-page" },
  // The test instance serves no Python runtime: the tab says so in its shell (its editor and
  // output stack is held by DataSciencePage.test.tsx).
  { route: "/python", shell: "python-panel-shell" },
  { route: "/notebook", shell: "notebook-panel-shell" },
];

/** The shell's gutter: the distance from its box to its toolbar or body, on each side. */
async function gutter(page: Page, shell: string) {
  return page.getByTestId(shell).evaluate((el) => {
    const outer = el.getBoundingClientRect();
    const body = el.querySelector(":scope > .explore-shell__body")!.getBoundingClientRect();
    const top = (el.querySelector(":scope > .explore-shell__toolbar") ??
      el.querySelector(":scope > .explore-shell__body"))!.getBoundingClientRect();
    return {
      top: Math.round(top.top - outer.top),
      left: Math.round(body.left - outer.left),
      right: Math.round(outer.right - body.right),
      bottom: Math.round(outer.bottom - body.bottom),
    };
  });
}

/** Drags a splitter by (dx, dy) and returns how far the pane beside it moved its edge. */
async function drag(page: Page, handle: string, dx: number, dy: number) {
  const box = (await page.getByTestId(handle).boundingBox())!;
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await page.mouse.move(x, y);
  await page.mouse.down();
  await page.mouse.move(x + dx / 2, y + dy / 2);
  await page.mouse.move(x + dx, y + dy);
  await page.mouse.up();
  const after = (await page.getByTestId(handle).boundingBox())!;
  return { x: Math.round(after.x - box.x), y: Math.round(after.y - box.y) };
}

for (const width of [1280, 1900]) {
  test(`every Explore surface stands in the shared shell at ${width}px`, async ({ page }, info) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/odata");
    await expect(page.getByTestId("odata-page")).toBeVisible({ timeout: 90_000 });
    const reference = await gutter(page, "odata-page");
    // OData's gutter is the management pages' own.
    await page.goto("/tables");
    const managementGutter = await page
      .locator("main .page")
      .first()
      .evaluate((el) => parseFloat(getComputedStyle(el).paddingLeft));
    expect(reference.left).toBe(Math.round(managementGutter));

    for (const s of SURFACES) {
      await page.goto(s.route);
      const shell = page.getByTestId(s.shell);
      await expect(shell, s.route).toBeVisible({ timeout: 90_000 });
      await expect(shell, s.route).toHaveAttribute("data-explore-shell", "");
      await info.attach(`${s.route.replaceAll("/", "_")}-${width}.png`, {
        body: await page.screenshot({ fullPage: false }),
        contentType: "image/png",
      });
      expect(await gutter(page, s.shell), `${s.route}: gutter`).toEqual(reference);
      const overflow = await page.evaluate(
        () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
      );
      expect(overflow, `${s.route}: page wider than the window`).toBeLessThanOrEqual(0);

      if (s.results) {
        const panel = page.getByTestId(s.results);
        await expect(panel, s.route).toHaveAttribute("data-result-panel", "");
        // No card around the result: neither the panel nor anything between it and the shell
        // draws a rounded border, a shadow or padding.
        const cards = await panel.evaluate((el) => {
          const found: string[] = [];
          for (
            let n: Element | null = el;
            n && !n.hasAttribute("data-explore-shell");
            n = n.parentElement
          ) {
            const cs = getComputedStyle(n);
            const card =
              (parseFloat(cs.borderTopLeftRadius) > 0 && parseFloat(cs.borderTopWidth) > 0) ||
              cs.boxShadow !== "none" ||
              (n !== el && parseFloat(cs.paddingTop) > 0 && parseFloat(cs.paddingLeft) > 0);
            if (card) found.push(`${n.tagName}.${n.className}`);
          }
          return found;
        });
        expect(cards, `${s.route}: a card around the results`).toEqual([]);
      }

      for (const [handle, dx, dy] of [
        [s.side, 60, 0],
        [s.stack, 0, 60],
      ] as const) {
        if (!handle) continue;
        await expect(page.getByTestId(handle), s.route).toBeVisible({ timeout: 90_000 });
        const moved = await drag(page, handle, dx, dy);
        expect(moved.x + moved.y, `${s.route}: ${handle} follows the pointer`).toBeGreaterThan(40);
        const at = (await page.getByTestId(handle).boundingBox())!;
        await page.reload();
        await expect(page.getByTestId(handle)).toBeVisible({ timeout: 90_000 });
        const kept = (await page.getByTestId(handle).boundingBox())!;
        expect(
          Math.abs(kept.x - at.x) + Math.abs(kept.y - at.y),
          `${s.route}: ${handle} kept`,
        ).toBeLessThan(3);
      }
    }
  });
}
