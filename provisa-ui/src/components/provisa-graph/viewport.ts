// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7d2e95-4c18-4a3f-9e61-8f5c3a2d7b10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph's camera moves and downloads, as its controls call them.

import type { CyInstance } from "../graph/cytoscape-types";

/** The margin a fit leaves round the diagram. */
export const FIT_PADDING = 10;

export interface GraphExports {
  svg: () => void;
  png: () => void;
  jpeg: () => void;
  json?: () => void;
}

/** How far down the canvas the legend reaches (it sits over the canvas's top right); 0 with none. */
function legendBand(cy: CyInstance): number {
  const box = cy.container();
  const legend = box?.closest(".pg-stage")?.querySelector(".pg-legend");
  if (!box || !legend) return 0;
  return Math.max(0, legend.getBoundingClientRect().bottom - box.getBoundingClientRect().top);
}

/** The least zoom a fit takes: below it a label is too small to read, so a diagram too big for
 * its canvas at this zoom is drawn at it from its top left and scrolls (down, and across). */
export const READABLE_ZOOM = 0.8;

/** How a graph opens and Reset view: fit the diagram by its drawn extent, `padding` round it,
 * below the legend (which sits over the canvas's top right: a lane's or a group's title is never
 * under it). The whole diagram is fitted while that keeps the zoom at READABLE_ZOOM or more; past
 * that it is drawn at the readable zoom from its top (and, wider than the canvas, its left) and
 * scrolled (scrollBy). */
export function fitClear(cy: CyInstance, padding: number = FIT_PADDING): void {
  fitBelowLegend(cy, padding, READABLE_ZOOM);
}

/** The fit control: the whole diagram in the canvas, below the legend, however small that makes
 * it (REQ-2239, FIT MEANS THE WHOLE IMAGE). */
export function fitWhole(cy: CyInstance, padding: number = FIT_PADDING): void {
  fitBelowLegend(cy, padding, 0);
}

/** Fit by the drawn extent below the legend, at no less than `floor`. */
function fitBelowLegend(cy: CyInstance, padding: number, floor: number): void {
  const box = cy.container();
  if (!box || box.clientWidth <= 0 || box.clientHeight <= 0) {
    // A canvas not on the page (headless, or not yet laid out): cytoscape's own fit.
    cy.fit(undefined, padding);
    return;
  }
  const bb = cy.elements().boundingBox({});
  if (bb.w <= 0 || bb.h <= 0) {
    // Nothing drawn: nothing to fit to.
    cy.fit(undefined, padding);
    return;
  }
  const top = Math.max(legendBand(cy), padding);
  const width = box.clientWidth - 2 * padding;
  const height = box.clientHeight - top - padding;
  if (width <= 0 || height <= 0) {
    cy.fit(undefined, padding);
    return;
  }
  cy.zoom(Math.max(Math.min(width / bb.w, height / bb.h), floor));
  // The zoom the instance accepted: it holds its own limits.
  const z = cy.zoom();
  const wide = bb.w * z > width;
  const tall = bb.h * z > height;
  cy.pan({
    x: (wide ? padding : (box.clientWidth - bb.w * z) / 2) - bb.x1 * z,
    y: (tall ? top : top + (height - bb.h * z) / 2) - bb.y1 * z,
  });
}

/** A box in canvas px. */
export interface Rect {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

/** The shifts along one axis that keep ``[lo, hi]`` inside ``[low, high]``; a span longer than
 * the room is held at the room's start. */
function within(lo: number, hi: number, low: number, high: number): [number, number] {
  return hi - lo > high - low ? [low - lo, low - lo] : [low - lo, high - hi];
}

/** Of ``spans`` (each a range of shifts) clipped to ``[lo, hi]``: the shift lying in the most of
 * them, and of those the nearest 0, with how many it lies in. */
function bestShift(spans: [number, number][], lo: number, hi: number): [number, number] {
  const candidates = [lo, hi, Math.min(hi, Math.max(lo, 0))];
  for (const [a, b] of spans) candidates.push(a, b);
  let best: [number, number] = [0, -1];
  for (const s of candidates) {
    if (s < lo || s > hi) continue;
    const count = spans.filter(([a, b]) => a <= s && s <= b).length;
    if (count > best[1] || (count === best[1] && Math.abs(s) < Math.abs(best[0]))) best = [s, count];
  }
  return best;
}

/** REQ-2239: how far to move a drawing so a graph opens with its focus in view. ``room`` is what
 * the canvas shows the drawing in; ``drawing``, ``focus`` and ``joined`` (the nodes the focus
 * joins directly) are where they are drawn now, all in canvas px. Along an axis on which the
 * drawing fits the room it does not move; along one it runs past, it moves no further than it
 * scrolls (scrollBy). Of those moves, those that keep the focus inside the room; of those, the
 * ones showing the most of ``joined`` whole; of those, the least move. */
export function focusShift(
  room: Rect,
  drawing: Rect,
  focus: Rect,
  joined: Rect[],
): { x: number; y: number } {
  const range = (lo: number, hi: number, low: number, high: number, f1: number, f2: number) => {
    // A drawing that fits does not move; one that runs past scrolls as far as it runs.
    if (hi - lo <= high - low) return [0, 0] as const;
    const [a, b] = within(f1, f2, low, high);
    return [Math.max(high - hi, a), Math.min(low - lo, b)] as const;
  };
  const [xLo, xHi] = range(drawing.x1, drawing.x2, room.x1, room.x2, focus.x1, focus.x2);
  const [yLo, yHi] = range(drawing.y1, drawing.y2, room.y1, room.y2, focus.y1, focus.y2);
  const across = joined.map((r) => within(r.x1, r.x2, room.x1, room.x2));
  const down = joined.map((r) => within(r.y1, r.y2, room.y1, room.y2));
  // A joined node is shown when it is shown both across and down: each move across is tried,
  // with the best move down for the nodes it shows across.
  let best = { x: 0, y: 0, count: -1 };
  const tries = new Set([Math.min(xHi, Math.max(xLo, 0)), xLo, xHi, ...across.flat()]);
  for (const x of tries) {
    if (x < xLo || x > xHi) continue;
    const shown = down.filter((_, i) => across[i][0] <= x && x <= across[i][1]);
    const [y, count] = bestShift(shown, yLo, yHi);
    const better =
      count > best.count ||
      (count === best.count && Math.abs(x) + Math.abs(y) < Math.abs(best.x) + Math.abs(best.y));
    if (better) best = { x, y, count };
  }
  return { x: best.x, y: best.y };
}

/** Moves the drawing so the node ``focus`` is in view, after the opening fit (fitClear, with the
 * same ``padding``): focusShift on where it is drawn. No focus, a focus the drawing does not hold,
 * or a canvas not on the page: the view is left as it is. */
export function showFocus(
  cy: CyInstance,
  focus: string | null | undefined,
  padding: number = FIT_PADDING,
): void {
  const box = cy.container();
  if (!focus || !box || box.clientWidth <= 0 || box.clientHeight <= 0) return;
  const node = cy.getElementById(focus);
  if (node.empty()) return;
  const room = {
    x1: padding,
    y1: Math.max(legendBand(cy), padding),
    x2: box.clientWidth - padding,
    y2: box.clientHeight - padding,
  };
  const joined = node
    .neighborhood("node")
    .map((n) => n.renderedBoundingBox({}) as Rect);
  const s = focusShift(
    room,
    cy.elements().renderedBoundingBox({}),
    node.renderedBoundingBox({}),
    joined,
  );
  if (s.x === 0 && s.y === 0) return;
  const pan = cy.pan();
  cy.pan({ x: pan.x + s.x, y: pan.y + s.y });
}

/** Scroll the diagram by (`dx`, `dy`) screen px, as far as it runs past the canvas and no
 * further: its top stays below the legend, its bottom no higher than the canvas's. Whether it
 * moved: a diagram that fits its canvas does not, and a wheel over it scrolls the page. */
export function scrollBy(
  cy: CyInstance,
  dx: number,
  dy: number,
  padding: number = FIT_PADDING,
): boolean {
  const box = cy.container();
  if (!box) return false;
  const r = cy.elements().renderedBoundingBox({});
  const top = Math.max(legendBand(cy), padding);
  const clamp = (shift: number, lo: number, hi: number, size: number, low: number) => {
    // ``lo``..``hi``: the diagram's rendered span; ``low``..``size``: the room it is shown in.
    if (hi - lo <= size - low - padding) return 0;
    const most = low - lo;
    const least = size - padding - hi;
    return Math.min(most, Math.max(least, -shift));
  };
  const mx = clamp(dx, r.x1, r.x2, box.clientWidth, padding);
  const my = clamp(dy, r.y1, r.y2, box.clientHeight, top);
  if (Math.abs(mx) < 0.5 && Math.abs(my) < 0.5) return false;
  const pan = cy.pan();
  cy.pan({ x: pan.x + mx, y: pan.y + my });
  return true;
}

/** Zoom by `factor` about the middle of the canvas, as a pinch at its centre would. */
export function zoomAboutCentre(cy: CyInstance, factor: number): void {
  const box = cy.container();
  zoomAbout(cy, factor, { x: box.clientWidth / 2, y: box.clientHeight / 2 });
}

/** Zoom by `factor` about `at` (canvas px): what is under it stays there. */
export function zoomAbout(cy: CyInstance, factor: number, at: { x: number; y: number }): void {
  const cx = at.x;
  const cyMid = at.y;
  const before = cy.zoom();
  const pan = cy.pan();
  cy.zoom(before * factor);
  // The zoom the instance accepted: it holds its own limits.
  const after = cy.zoom();
  cy.pan({
    x: cx - ((cx - pan.x) * after) / before,
    y: cyMid - ((cyMid - pan.y) * after) / before,
  });
}
