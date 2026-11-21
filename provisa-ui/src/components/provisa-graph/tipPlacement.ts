// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9f4c-1d87-4a3b-9c50-7e8f2a1d4c96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: where a graph's hover card goes: beside what is hovered, covering least of what is
// drawn.

import type { FloatingPosition } from "@mantine/core";

/** A point on the page. */
export interface At {
  x: number;
  y: number;
}

/** A rectangle on the page. */
export interface Box {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

/** What a hover card should stay off: the nodes drawn and the lines between them, on the page. */
export interface DrawnGraph {
  boxes: Box[];
  lines: [At, At][];
}

const TIP_PLACES: FloatingPosition[] = [
  "right-start",
  "left-start",
  "bottom-start",
  "top-start",
  "right",
  "left",
  "bottom",
  "top",
  "right-end",
  "left-end",
  "bottom-end",
  "top-end",
];
/** The card's distance from the pointer (Tip's offset and arrow) and from the window's edge. */
const TIP_GAP = 8;
const TIP_MARGIN = 4;
/** How much of a line counts as drawn beneath the card: a band this wide, as a node's area. */
const LINE_BAND = 10;

/** The length of the segment ``a``-``b`` inside ``r`` (Liang-Barsky). */
function clipped(a: At, b: At, r: Box): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  let lo = 0;
  let hi = 1;
  for (const [p, q] of [
    [-dx, a.x - r.left],
    [dx, r.right - a.x],
    [-dy, a.y - r.top],
    [dy, r.bottom - a.y],
  ]) {
    if (p === 0) {
      if (q < 0) return 0;
    } else if (p < 0) lo = Math.max(lo, q / p);
    else hi = Math.min(hi, q / p);
  }
  return hi <= lo ? 0 : (hi - lo) * Math.hypot(dx, dy);
}

/** A card of ``size`` beside the ``target`` box covers this box in ``place``. */
function placeBox(place: FloatingPosition, target: Box, size: { width: number; height: number }): Box {
  const [side, align] = place.split("-") as [string, string | undefined];
  const across = (lo: number, hi: number, length: number) =>
    align === "start" ? lo : align === "end" ? hi - length : (lo + hi - length) / 2;
  const left =
    side === "right"
      ? target.right + TIP_GAP
      : side === "left"
        ? target.left - TIP_GAP - size.width
        : across(target.left, target.right, size.width);
  const top =
    side === "bottom"
      ? target.bottom + TIP_GAP
      : side === "top"
        ? target.top - TIP_GAP - size.height
        : across(target.top, target.bottom, size.height);
  return { left, top, right: left + size.width, bottom: top + size.height };
}

/** Where beside ``target`` (the node or the point hovered) a card of ``size`` covers least of the
 * drawn graph: the first of TIP_PLACES, among those inside the window, covering fewest nodes and
 * lines. */
export function tipPlacement(
  target: Box,
  size: { width: number; height: number },
  drawn: DrawnGraph,
  win: { width: number; height: number },
): FloatingPosition {
  const cover = (r: Box) => {
    let sum = 0;
    for (const b of drawn.boxes) {
      const w = Math.min(r.right, b.right) - Math.max(r.left, b.left);
      const h = Math.min(r.bottom, b.bottom) - Math.max(r.top, b.top);
      if (w > 0 && h > 0) sum += w * h;
    }
    for (const [a, b] of drawn.lines) sum += clipped(a, b, r) * LINE_BAND;
    return sum;
  };
  const inWindow = (r: Box) =>
    r.left >= TIP_MARGIN &&
    r.top >= TIP_MARGIN &&
    r.right <= win.width - TIP_MARGIN &&
    r.bottom <= win.height - TIP_MARGIN;
  let best: FloatingPosition = TIP_PLACES[0];
  let least = Infinity;
  for (const p of TIP_PLACES) {
    const r = placeBox(p, target, size);
    if (!inWindow(r)) continue;
    const c = cover(r);
    if (c < least) {
      least = c;
      best = p;
    }
  }
  return best;
}
