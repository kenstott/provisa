// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8a1c57-9d24-4b6f-a1e0-6f2c7b9d5a48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: lines that share a pair of nodes, in either direction, are drawn apart and each of their
// labels is drawn clear of the others. The maintainer collapsed the SHELTER and PET-STORE domains of
// the Relationships ERD: PETS_OF_BREED and BREED_INFO, one each way between the two boxes, were
// drawn almost on top of each other, and PETS_OF_BREED's label covered BREED_INFO's. Cytoscape's own
// bundling set the two curves 20px apart at their middles, and the stylesheet's label nudge (a hash
// of the label) put the two labels on the same spot.

import type { CyElement, CyInstance } from "../graph/cytoscape-types";
import { textWidth } from "./model";
import { SIZE } from "./tokens";

type Pt = { x: number; y: number };

/** How far apart the curves of a pair's adjacent lines bend, at their control points (model px). */
export const PAIR_BEND = 48;
/** The least room left between two labels of one pair (model px). */
export const LABEL_GAP = 6;
/** The padding the stylesheet draws round an edge's label (``text-background-padding``). */
const LABEL_PAD = 2;

/** A label's box as drawn: its text and the padding round it (model px). */
export function labelSize(label: string): { w: number; h: number } {
  return {
    w: textWidth(label, SIZE.edgeLabel) + 2 * LABEL_PAD,
    h: SIZE.edgeLabel * 1.5 + 2 * LABEL_PAD,
  };
}

/** The lines drawn between each pair of nodes that has more than one, by the pair (its two ids,
 * in order), in the order the view gave them. */
export function linePairs(cy: CyInstance): Map<string, CyElement[]> {
  const pairs = new Map<string, CyElement[]>();
  cy.edges(".pg-edge").forEach((e) => {
    const s = String(e.data("source"));
    const t = String(e.data("target"));
    const key = s < t ? `${s}|${t}` : `${t}|${s}`;
    const list = pairs.get(key);
    if (list) list.push(e);
    else pairs.set(key, [e]);
  });
  for (const [key, list] of pairs) {
    if (list.length < 2) pairs.delete(key);
    else list.sort((a, b) => (a.data("pathIndex") as number) - (b.data("pathIndex") as number));
  }
  return pairs;
}

/** The unit normal of the line from the pair's first node to its second, to its left as cytoscape
 * reckons it, (-dy, dx); straight down for two nodes at one point. */
export function pairNormal(first: Pt, second: Pt): Pt {
  const dx = second.x - first.x;
  const dy = second.y - first.y;
  const len = Math.hypot(dx, dy);
  if (len < 1e-9) return { x: 0, y: 1 };
  return { x: -dy / len, y: dx / len };
}

/** A pair's lines, one each: ``edge`` and where its label is anchored (model px). */
export interface PairLabel {
  edge: CyElement;
  label: string;
  anchor: Pt;
}

/** REQ-2277: how far to move each label of one pair (``text-margin-x``/``-y``) so none overlaps
 * another: in order along ``dir``, each is moved along ``dir`` the least that clears the ones before
 * it, and the set is then centred on where it was anchored. */
export function labelMargins(labels: PairLabel[], dir: Pt): Pt[] {
  const order = labels
    .map((l, i) => ({ i, along: l.anchor.x * dir.x + l.anchor.y * dir.y }))
    .sort((a, b) => a.along - b.along)
    .map((o) => o.i);
  const shift = new Array<number>(labels.length).fill(0);
  const placed: number[] = [];
  for (const i of order) {
    const a = labels[i].anchor;
    const size = labelSize(labels[i].label);
    // The shifts at which this label would overlap each placed one: an open interval each.
    const blocked: [number, number][] = [];
    for (const j of placed) {
      const other = labelSize(labels[j].label);
      const c = { x: labels[j].anchor.x + shift[j] * dir.x, y: labels[j].anchor.y + shift[j] * dir.y };
      const across = (a1: number, c1: number, d: number, half: number): [number, number] | null => {
        if (Math.abs(d) < 1e-9) return Math.abs(a1 - c1) < half ? [-Infinity, Infinity] : null;
        const lo = (c1 - half - a1) / d;
        const hi = (c1 + half - a1) / d;
        return lo < hi ? [lo, hi] : [hi, lo];
      };
      const x = across(a.x, c.x, dir.x, (size.w + other.w) / 2 + LABEL_GAP);
      const y = across(a.y, c.y, dir.y, (size.h + other.h) / 2 + LABEL_GAP);
      if (!x || !y) continue;
      const lo = Math.max(x[0], y[0]);
      const hi = Math.min(x[1], y[1]);
      if (lo < hi) blocked.push([lo, hi]);
    }
    // The least shift, from the last one placed on, inside none of them.
    const from = placed.length > 0 ? shift[placed[placed.length - 1]] : 0;
    const candidates = [from, ...blocked.map(([, hi]) => hi)].filter((t) => t >= from);
    candidates.sort((p, q) => p - q);
    const free = candidates.find((t) => blocked.every(([lo, hi]) => t <= lo || t >= hi));
    if (free === undefined) throw new Error("labelMargins: no clear place for a label");
    shift[i] = free;
    placed.push(i);
  }
  // Centred: the labels move apart about where they were anchored, not all to one side.
  const mid = (Math.min(...shift) + Math.max(...shift)) / 2;
  return shift.map((t) => ({ x: (t - mid) * dir.x, y: (t - mid) * dir.y }));
}

/** Sets each label's margins (text-margin-x/-y) from labelMargins. */
function placeLabels(labels: PairLabel[], dir: Pt): void {
  const margins = labelMargins(labels, dir);
  labels.forEach((l, i) => {
    l.edge.style({ "text-margin-x": margins[i].x, "text-margin-y": margins[i].y });
  });
}

/** The slot of the ``k``th of ``n`` lines: its place across the pair, centred on the straight line. */
const slot = (k: number, n: number) => k - (n - 1) / 2;

/** REQ-2277: one pair's ``lines`` (``key`` its two ids) bend by their slots, PAIR_BEND apart, the
 * slots read across the pair's first-to-second line whichever way a line runs, so two lines in
 * opposite directions bend to opposite sides; their labels are then moved clear of each other. */
export function bendPair(cy: CyInstance, key: string, lines: CyElement[]): void {
  const [firstId, secondId] = key.split("|");
  const p = (cy.getElementById(firstId) as unknown as CyElement).position();
  const q = (cy.getElementById(secondId) as unknown as CyElement).position();
  const normal = pairNormal(p, q);
  const mid = { x: (p.x + q.x) / 2, y: (p.y + q.y) / 2 };
  const labels = lines.map((edge, k) => {
    const across = slot(k, lines.length) * PAIR_BEND;
    // A line from the second node to the first has the opposite normal (cytoscape's distances
    // are to a line's own left).
    const forward = String(edge.data("source")) === firstId;
    edge.style({
      "curve-style": "unbundled-bezier",
      "control-point-distances": forward ? across : -across,
      "control-point-weights": 0.5,
    });
    // A one-control-point curve passes half its bend from the middle of its ends.
    return {
      edge,
      label: String(edge.data("label") ?? ""),
      anchor: { x: mid.x + (normal.x * across) / 2, y: mid.y + (normal.y * across) / 2 },
    };
  });
  placeLabels(labels, normal);
}

/** REQ-2277, bezier routing: every pair's lines bent apart (bendPair). */
export function bendPairs(cy: CyInstance): void {
  for (const [key, lines] of linePairs(cy)) bendPair(cy, key, lines);
}

/** REQ-2277, right-angle routing: the labels of one pair's lines, anchored where cytoscape draws
 * them (``anchors``, by edge id: the middle of each route's cross-run), moved clear of each other. */
export function placePairLabels(
  cy: CyInstance,
  key: string,
  lines: CyElement[],
  anchors: Map<string, Pt>,
): void {
  const [firstId, secondId] = key.split("|");
  const normal = pairNormal(
    (cy.getElementById(firstId) as unknown as CyElement).position(),
    (cy.getElementById(secondId) as unknown as CyElement).position(),
  );
  const labels = lines.map((edge) => {
    const anchor = anchors.get(edge.id());
    if (!anchor) throw new Error(`placePairLabels: no anchor for ${edge.id()}`);
    return { edge, label: String(edge.data("label") ?? ""), anchor };
  });
  placeLabels(labels, normal);
}
