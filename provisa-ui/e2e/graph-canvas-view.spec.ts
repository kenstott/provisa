// Copyright (c) 2026 Kenneth Stott
// Canary: 93287fd2-23b8-4793-8e78-1de31d71c9df
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277, on every graph surface (the lead's decision): the Cypher result graph keeps its view's
// mode when its canvas narrows or widens, as the Relationships ERD does. Fitted whole, it is fitted
// whole again; zoomed by the reader, it keeps the zoom and the point of the graph at the centre.
// Before, the canvas was only re-sized: a narrower canvas cut a fitted graph off at its edge.
//
// The graph is read from the cytoscape core the page exposes as window.__cy.

import type { Page } from "@playwright/test";
import { test, expect, BACKEND_URL } from "./coverage";
import { scopedStorageKey } from "./storageScope";

test.describe.configure({ timeout: 180_000 });
test.use({ viewport: { width: 1600, height: 1000 } });

const QUERY = "MATCH (n:PetStore:Pets) RETURN n LIMIT 25";

type Box = { x1: number; y1: number; x2: number; y2: number };

/** Where the graph is drawn, and the canvas it is drawn in, in canvas px. */
function graphInCanvas(page: Page) {
  return page.evaluate(() => {
    const cy = (window as unknown as Record<string, unknown>).__cy as {
      elements(): { renderedBoundingBox(o: object): Box };
      container(): HTMLElement;
      zoom(): number;
      pan(): { x: number; y: number };
    };
    const b = cy.elements().renderedBoundingBox({});
    const box = cy.container();
    const zoom = cy.zoom();
    const pan = cy.pan();
    return {
      drawing: { x1: b.x1, y1: b.y1, x2: b.x2, y2: b.y2 },
      width: box.clientWidth,
      height: box.clientHeight,
      zoom,
      // The graph's point at the canvas's centre, in model px.
      centre: {
        x: (box.clientWidth / 2 - pan.x) / zoom,
        y: (box.clientHeight / 2 - pan.y) / zoom,
      },
    };
  });
}

const inView = (d: Awaited<ReturnType<typeof graphInCanvas>>) =>
  d.drawing.x1 >= -1 &&
  d.drawing.y1 >= -1 &&
  d.drawing.x2 <= d.width + 1 &&
  d.drawing.y2 <= d.height + 1;

/** The result graph of QUERY, drawn and laid out. */
async function graphDrawn(page: Page) {
  await page.request.post(`${BACKEND_URL}/data/cypher`, {
    data: { query: QUERY, params: {} },
    timeout: 90_000,
  });
  await page.goto("/graph", { timeout: 30_000 });
  const pendingKey = await scopedStorageKey(page.request, "provisa.graph.pending_query");
  await page.evaluate(([k, q]) => localStorage.setItem(k, q), [pendingKey, QUERY]);
  await page.reload({ timeout: 30_000 });
  await page.waitForFunction(
    () => {
      const cy = (window as unknown as Record<string, unknown>).__cy as
        { nodes(): { length: number } } | undefined;
      return cy != null && cy.nodes().length >= 2;
    },
    undefined,
    { timeout: 90_000 },
  );
  // The layout has ended (it fits the graph whole) once the drawing stops moving.
  let last = "";
  await expect
    .poll(
      async () => {
        const now = JSON.stringify((await graphInCanvas(page)).drawing);
        const still = now === last;
        last = now;
        return still;
      },
      { timeout: 30_000, intervals: [500] },
    )
    .toBe(true);
}

test("fitted whole, the result graph stays whole on a narrower canvas, and again widened", async ({
  page,
}) => {
  await graphDrawn(page);
  await page.getByTestId("canvas-fit-all").click();
  const wide = await graphInCanvas(page);
  expect(inView(wide)).toBe(true);

  await page.setViewportSize({ width: 900, height: 1000 });
  await expect.poll(async () => (await graphInCanvas(page)).width).toBeLessThan(wide.width - 200);
  await expect.poll(async () => inView(await graphInCanvas(page))).toBe(true);

  await page.setViewportSize({ width: 1600, height: 1000 });
  await expect.poll(async () => (await graphInCanvas(page)).width).toBe(wide.width);
  await expect.poll(async () => inView(await graphInCanvas(page))).toBe(true);
  // Fitted whole again, at the size it was: as it was drawn before the canvas narrowed.
  expect((await graphInCanvas(page)).zoom).toBeCloseTo(wide.zoom, 3);
});

test("zoomed by the reader, a narrower canvas keeps the zoom and the graph's point at the centre", async ({
  page,
}) => {
  await graphDrawn(page);
  await page.getByTestId("canvas-zoom-in").click();
  await page.getByTestId("canvas-zoom-in").click();
  const before = await graphInCanvas(page);

  await page.setViewportSize({ width: 900, height: 1000 });
  await expect.poll(async () => (await graphInCanvas(page)).width).toBeLessThan(before.width - 200);
  const after = await graphInCanvas(page);
  expect(after.zoom).toBeCloseTo(before.zoom, 6);
  expect(after.centre.x).toBeCloseTo(before.centre.x, 0);
  expect(after.centre.y).toBeCloseTo(before.centre.y, 0);
});
