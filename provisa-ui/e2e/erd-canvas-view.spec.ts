// Copyright (c) 2026 Kenneth Stott
// Canary: 5a9c3e72-1b4d-4f86-9d20-7e6b8a1c3f59
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the Relationships ERD, as the maintainer used it.
// 1. Fitted to show the whole model, a drag of the splitter to the detail panel jumped the ERD to
//    a zoomed-in view. A resize keeps the view's mode: fitted whole, it is fitted whole again.
// 2. With SHELTER and PET-STORE collapsed, PETS_OF_BREED and BREED_INFO -- one each way between the
//    two boxes -- were drawn on top of each other and only PETS_OF_BREED's label showed.
//
// The drawing is read from the cytoscape core the canvas holds (cytoscape registers itself on its
// container as ``_cyreg``).

import type { Page } from "@playwright/test";
import { test, expect } from "./coverage";

test.describe.configure({ timeout: 120_000 });
// Wide enough that the ERD keeps a canvas a drag can narrow by hundreds of px beside the panel.
test.use({ viewport: { width: 1900, height: 1000 } });

type Box = { x1: number; y1: number; x2: number; y2: number };

/** The ERD drawn and laid out. */
async function erdDrawn(page: Page) {
  await expect(page.getByTestId("erd")).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("erd-laying-out")).toHaveCount(0, { timeout: 60_000 });
}

/** Where the visible diagram is drawn, and the canvas it is drawn in, in canvas px. */
function diagramInCanvas(page: Page) {
  return page.evaluate(() => {
    const host = document.querySelector('[data-testid="erd"] .pg-canvas > div') as
      | (HTMLElement & { _cyreg?: { cy: unknown } })
      | null;
    if (!host?._cyreg) throw new Error("the ERD's canvas holds no cytoscape core");
    const cy = host._cyreg.cy as {
      elements(sel: string): { renderedBoundingBox(o: object): Box };
      zoom(): number;
    };
    const b = cy.elements(":visible").renderedBoundingBox({});
    return {
      drawing: { x1: b.x1, y1: b.y1, x2: b.x2, y2: b.y2 },
      width: host.clientWidth,
      height: host.clientHeight,
      zoom: cy.zoom(),
    };
  });
}

const inView = (d: Awaited<ReturnType<typeof diagramInCanvas>>) =>
  d.drawing.x1 >= -1 && d.drawing.y1 >= -1 && d.drawing.x2 <= d.width + 1 && d.drawing.y2 <= d.height + 1;

test("fitted whole, the ERD stays whole when the splitter to the detail panel is dragged", async ({
  page,
}) => {
  await page.goto("/relationships");
  await page.waitForSelector(".data-table", { timeout: 60_000 });
  // The relationship's id, as its row's detail link names it.
  await page.locator(".data-table tbody tr", { hasText: "PETS_OF_BREED" }).first().click();
  await expect(page).toHaveURL(/[?&]relationship=/);
  const id = new URL(page.url()).searchParams.get("relationship")!;
  // The ERD with that relationship's detail open beside it: the splitter between the two.
  await page.goto(`/relationships?view=erd&relationship=${encodeURIComponent(id)}`);
  await erdDrawn(page);
  const handle = page.getByTestId("relationships-split-handle");
  await expect(handle).toBeVisible();

  await page.getByTestId("erd-fit-all").click();
  const fitted = await diagramInCanvas(page);
  expect(inView(fitted), JSON.stringify(fitted)).toBe(true);

  // The splitter dragged toward the ERD: the panel widens and the ERD narrows -- as far as the
  // whole model can still be drawn above the graph's least zoom (0.1; the demo model is some
  // thousands of px across), and no further than 300px.
  const modelWidth = (fitted.drawing.x2 - fitted.drawing.x1) / fitted.zoom;
  const narrowest = Math.ceil(modelWidth * 0.1 * 1.2 + 40);
  const by = Math.min(300, fitted.width - narrowest);
  expect(by, JSON.stringify({ fitted, narrowest })).toBeGreaterThanOrEqual(60);
  const h = (await handle.boundingBox())!;
  await page.mouse.move(h.x + h.width / 2, h.y + h.height / 2);
  await page.mouse.down();
  await page.mouse.move(h.x + h.width / 2 - by / 2, h.y + h.height / 2, { steps: 5 });
  await page.mouse.move(h.x + h.width / 2 - by, h.y + h.height / 2, { steps: 5 });
  await page.mouse.up();

  await expect
    .poll(async () => (await diagramInCanvas(page)).width, { timeout: 10_000 })
    .toBeLessThan(fitted.width - by / 2);
  // Every node still in view, fitted to the narrower canvas: no jump to a zoomed-in view.
  await expect
    .poll(async () => {
      const d = await diagramInCanvas(page);
      return inView(d) ? "whole" : JSON.stringify(d);
    })
    .toBe("whole");
  expect((await diagramInCanvas(page)).zoom).toBeLessThanOrEqual(fitted.zoom + 1e-9);
});

test("with SHELTER and PET-STORE collapsed, both lines between them and both labels are drawn", async ({
  page,
}) => {
  await page.goto("/relationships?view=erd&collapsed=shelter,pet-store");
  await erdDrawn(page);

  const lines = await page.evaluate(() => {
    const host = document.querySelector('[data-testid="erd"] .pg-canvas > div') as
      | (HTMLElement & { _cyreg?: { cy: unknown } })
      | null;
    if (!host?._cyreg) throw new Error("the ERD's canvas holds no cytoscape core");
    type Edge = {
      data(k: string): unknown;
      visible(): boolean;
      midpoint(): { x: number; y: number };
      renderedBoundingBox(o: object): Box;
      pstyle(k: string): { value: unknown };
    };
    const cy = host._cyreg.cy as { edges(sel?: string): { toArray(): Edge[] } };
    const ends = new Set(["d:shelter", "d:pet-store"]);
    return cy
      .edges()
      .toArray()
      .filter((e) => ends.has(String(e.data("source"))) && ends.has(String(e.data("target"))))
      .map((e) => {
        const b = e.renderedBoundingBox({
          includeNodes: false,
          includeEdges: false,
          includeLabels: true,
        });
        return {
          label: String(e.data("label")),
          visible: e.visible(),
          textOpacity: Number(e.pstyle("text-opacity").value),
          mid: e.midpoint(),
          box: { x1: b.x1, y1: b.y1, x2: b.x2, y2: b.y2 },
        };
      });
  });

  const labels = lines.map((l) => l.label);
  expect(labels).toEqual(expect.arrayContaining(["PETS_OF_BREED", "BREED_INFO"]));
  for (const l of lines) {
    expect(l.visible, l.label).toBe(true);
    expect(l.textOpacity, l.label).toBeGreaterThan(0);
    expect(l.box.x2 - l.box.x1, l.label).toBeGreaterThan(0);
  }
  // Each line on its own path, and no label drawn over another.
  for (let i = 0; i < lines.length; i++) {
    for (let j = i + 1; j < lines.length; j++) {
      const a = lines[i];
      const b = lines[j];
      const pair = `${a.label} / ${b.label}`;
      expect(Math.hypot(a.mid.x - b.mid.x, a.mid.y - b.mid.y), pair).toBeGreaterThan(10);
      const overlap =
        a.box.x1 < b.box.x2 && b.box.x1 < a.box.x2 && a.box.y1 < b.box.y2 && b.box.y1 < a.box.y2;
      expect(overlap, `${pair}: ${JSON.stringify([a.box, b.box])}`).toBe(false);
    }
  }
});
