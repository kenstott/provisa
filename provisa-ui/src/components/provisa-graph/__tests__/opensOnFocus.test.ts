// Copyright (c) 2026 Kenneth Stott
// Canary: 6a2f9c41-0d7e-4b85-93e1-c84b27f5d016
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph with a focus opens with its focus in view. J1227 (2026-11-12) found a term's
// graph at 1280 opened at the readable zoom floor, drawn from its left edge, with the term lane --
// the focus term in it -- past the right edge. focusShift is the pan that brings it in: the focus
// inside the canvas with a margin, and, of the pans that keep it so, the one showing the most of
// the nodes it joins directly, the least move from the opening fit among those.

import { describe, expect, it } from "vitest";
import type { CyInstance } from "../../graph/cytoscape-types";
import { fitClear, fitWhole, focusShift, showFocus, type Rect } from "../viewport";

const rect = (x1: number, y1: number, x2: number, y2: number): Rect => ({ x1, y1, x2, y2 });
const moved = (r: Rect, s: { x: number; y: number }): Rect =>
  rect(r.x1 + s.x, r.y1 + s.y, r.x2 + s.x, r.y2 + s.y);
const inside = (r: Rect, room: Rect) =>
  r.x1 >= room.x1 - 1e-9 && r.x2 <= room.x2 + 1e-9 && r.y1 >= room.y1 - 1e-9 && r.y2 <= room.y2 + 1e-9;

// The room a 1280 x 400 canvas shows the drawing in: 24px round it.
const ROOM = rect(24, 24, 1256, 376);

describe("focusShift", () => {
  it("brings a focus in the last lane of a drawing wider than the canvas inside it", () => {
    // Drawn from its left, 3000px across: the focus at 2800 is far past the right edge.
    const drawing = rect(24, 24, 3024, 376);
    const focus = rect(2800, 100, 2950, 130);
    const s = focusShift(ROOM, drawing, focus, []);
    expect(inside(moved(focus, s), ROOM)).toBe(true);
    // The drawing still covers the room: no blank band opens at its right.
    expect(moved(drawing, s).x2).toBeGreaterThanOrEqual(ROOM.x2 - 1e-9);
    expect(s.y).toBe(0);
  });

  it("brings a focus below the canvas's height inside it", () => {
    const drawing = rect(300, 24, 900, 3024);
    const focus = rect(500, 2500, 650, 2530);
    const s = focusShift(ROOM, drawing, focus, []);
    expect(inside(moved(focus, s), ROOM)).toBe(true);
    expect(s.x).toBe(0);
  });

  it("of the pans keeping the focus in, takes the one showing the most nodes it joins", () => {
    const drawing = rect(24, 24, 4024, 376);
    const focus = rect(2000, 100, 2100, 130);
    // Two joined nodes to the left of the focus, within a canvas's width of it; one far right.
    const joined = [rect(1100, 50, 1200, 80), rect(1300, 200, 1400, 230), rect(3900, 50, 4000, 80)];
    const s = focusShift(ROOM, drawing, focus, joined);
    expect(inside(moved(focus, s), ROOM)).toBe(true);
    expect(inside(moved(joined[0], s), ROOM)).toBe(true);
    expect(inside(moved(joined[1], s), ROOM)).toBe(true);
    expect(inside(moved(joined[2], s), ROOM)).toBe(false);
    // The least move from the opening fit that shows them: the focus at the room's right edge.
    expect(moved(focus, s).x2).toBeCloseTo(ROOM.x2, 9);
  });

  it("counts a joined node shown only when it is shown both across and down", () => {
    const drawing = rect(24, 24, 4024, 3024);
    const focus = rect(2000, 2000, 2100, 2030);
    // One node right of the focus at its height; two left of it, far above: no pan shows both
    // of those with the focus, one pan shows the first.
    const joined = [rect(2900, 2100, 3000, 2130), rect(1100, 100, 1200, 130), rect(1300, 120, 1400, 150)];
    const s = focusShift(ROOM, drawing, focus, joined);
    expect(inside(moved(focus, s), ROOM)).toBe(true);
    expect(inside(moved(joined[0], s), ROOM)).toBe(true);
  });

  it("does not move a drawing that fits the canvas", () => {
    const drawing = rect(100, 60, 1100, 340);
    const s = focusShift(ROOM, drawing, rect(1000, 300, 1050, 320), [rect(100, 60, 150, 80)]);
    expect(s).toEqual({ x: 0, y: 0 });
  });

  it("does not move a focus already in view when that shows as many of its joined nodes", () => {
    const drawing = rect(24, 24, 3024, 376);
    const s = focusShift(ROOM, drawing, rect(200, 100, 300, 130), [rect(600, 100, 700, 130)]);
    expect(s).toEqual({ x: 0, y: 0 });
  });

  it("puts a focus bigger than the room at the room's start", () => {
    const drawing = rect(24, 24, 3024, 376);
    const focus = rect(1500, 100, 3000, 130);
    const s = focusShift(ROOM, drawing, focus, []);
    expect(moved(focus, s).x1).toBeCloseTo(ROOM.x1, 9);
  });
});

// ── on a cytoscape stand-in: how a graph opens, and Reset view ─────────────────────────────

type BB = { x1: number; y1: number; x2: number; y2: number; w: number; h: number };
const bb = (x1: number, y1: number, x2: number, y2: number): BB => ({
  x1,
  y1,
  x2,
  y2,
  w: x2 - x1,
  h: y2 - y1,
});

/** A canvas ``w`` x ``h`` with no legend, drawing the model nodes ``nodes`` (id -> model box)
 * joined by ``edges``, as cytoscape renders them at its zoom and pan. */
function stage(w: number, h: number, nodes: Record<string, BB>, edges: [string, string][]) {
  const box = document.createElement("div");
  Object.defineProperty(box, "clientWidth", { value: w });
  Object.defineProperty(box, "clientHeight", { value: h });
  box.getBoundingClientRect = () => ({ top: 0, left: 0, bottom: h, right: w }) as DOMRect;
  let zoom = 1;
  let pan = { x: 0, y: 0 };
  const render = (m: BB): BB =>
    bb(m.x1 * zoom + pan.x, m.y1 * zoom + pan.y, m.x2 * zoom + pan.x, m.y2 * zoom + pan.y);
  const all = Object.values(nodes);
  const model = bb(
    Math.min(...all.map((n) => n.x1)),
    Math.min(...all.map((n) => n.y1)),
    Math.max(...all.map((n) => n.x2)),
    Math.max(...all.map((n) => n.y2)),
  );
  const node = (id: string) => ({
    empty: () => !(id in nodes),
    renderedBoundingBox: () => render(nodes[id]),
    neighborhood: () =>
      edges
        .filter(([a, b]) => a === id || b === id)
        .map(([a, b]) => node(a === id ? b : a)),
  });
  const cy = {
    container: () => box,
    elements: () => ({ boundingBox: () => model, renderedBoundingBox: () => render(model) }),
    getElementById: node,
    fit: () => {},
    zoom: (z?: number) => {
      if (z === undefined) return zoom;
      zoom = z;
      return undefined;
    },
    pan: (p?: { x: number; y: number }) => {
      if (p === undefined) return pan;
      pan = p;
      return undefined;
    },
  } as unknown as CyInstance;
  return { cy, at: (id: string) => render(nodes[id]), whole: () => render(model) };
}

// Three lanes left to right, 1200 model px apart: columns, tables, terms. At the readable zoom
// the drawing is wider than a 1280 canvas; the focus term is in the last lane.
const LANES: Record<string, BB> = {
  col: bb(0, 0, 200, 40),
  tbl: bb(1200, 0, 1400, 40),
  term: bb(2400, 0, 2600, 40),
  other: bb(2400, 300, 2600, 340),
};
const LINKS: [string, string][] = [
  ["col", "tbl"],
  ["tbl", "term"],
];

const PAD = 24;
const roomOf = (w: number, h: number) => rect(PAD, PAD, w - PAD, h - PAD);

describe("a graph with a focus opens, and resets, with its focus in view", () => {
  it("opens with a focus in the last lane of a drawing wider than the canvas inside it", () => {
    const { cy, at } = stage(1280, 400, LANES, LINKS);
    fitClear(cy, PAD);
    // As main opened it: from its left, the term past the right edge.
    expect(at("term").x1).toBeGreaterThan(1280);
    showFocus(cy, "term", PAD);
    expect(inside(at("term"), roomOf(1280, 400))).toBe(true);
    // Its table, joined to it, shows beside it.
    expect(inside(at("tbl"), roomOf(1280, 400))).toBe(true);
  });

  it("Reset view brings the focus back into view after the reader scrolled away", () => {
    const { cy, at } = stage(1280, 400, LANES, LINKS);
    fitClear(cy, PAD);
    showFocus(cy, "term", PAD);
    cy.pan({ x: 0, y: 0 });
    // Reset view: the opening fit, then the focus.
    fitClear(cy, PAD);
    showFocus(cy, "term", PAD);
    expect(inside(at("term"), roomOf(1280, 400))).toBe(true);
  });

  it("opens with a focus below the canvas's height inside it", () => {
    const tall: Record<string, BB> = {
      top: bb(0, 0, 200, 40),
      focus: bb(0, 4000, 200, 4040),
      near: bb(0, 3700, 200, 3740),
    };
    const { cy, at } = stage(1280, 720, tall, [["focus", "near"]]);
    fitClear(cy, PAD);
    expect(at("focus").y1).toBeGreaterThan(720);
    showFocus(cy, "focus", PAD);
    expect(inside(at("focus"), roomOf(1280, 720))).toBe(true);
    expect(inside(at("near"), roomOf(1280, 720))).toBe(true);
  });

  it("opens a drawing that fits exactly as it did", () => {
    const small: Record<string, BB> = { a: bb(0, 0, 200, 40), b: bb(800, 200, 1000, 240) };
    const plain = stage(1280, 720, small, [["a", "b"]]);
    const focused = stage(1280, 720, small, [["a", "b"]]);
    fitClear(plain.cy, PAD);
    fitClear(focused.cy, PAD);
    showFocus(focused.cy, "b", PAD);
    expect(focused.cy.pan()).toEqual(plain.cy.pan());
    expect(focused.cy.zoom()).toBe(plain.cy.zoom());
  });

  it("opens a graph with no focus exactly as it did", () => {
    const plain = stage(1280, 400, LANES, LINKS);
    const none = stage(1280, 400, LANES, LINKS);
    fitClear(plain.cy, PAD);
    fitClear(none.cy, PAD);
    showFocus(none.cy, null, PAD);
    expect(none.cy.pan()).toEqual(plain.cy.pan());
  });

  it("leaves the view as it is for a focus the drawing does not hold", () => {
    const plain = stage(1280, 400, LANES, LINKS);
    const gone = stage(1280, 400, LANES, LINKS);
    fitClear(plain.cy, PAD);
    fitClear(gone.cy, PAD);
    showFocus(gone.cy, "nowhere", PAD);
    expect(gone.cy.pan()).toEqual(plain.cy.pan());
  });

  it("fit still shows the whole image", () => {
    const { cy, whole } = stage(1280, 400, LANES, LINKS);
    fitClear(cy, PAD);
    showFocus(cy, "term", PAD);
    fitWhole(cy, 10);
    expect(inside(whole(), rect(10, 10, 1270, 390))).toBe(true);
  });
});
