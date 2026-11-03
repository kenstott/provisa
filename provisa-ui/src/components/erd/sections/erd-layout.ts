// Copyright (c) 2026 Kenneth Stott
// Canary: f572f0bd-8e40-4402-bb51-3e2c8835d351
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CyInstance } from "../../graph/cytoscape-types";

type Pt = { x: number; y: number };

// Returns the set of node IDs that have at least one edge (any kind).
// True orphans (no edges) are excluded from fCoSE and placed in a grid instead.
export function nodesWithEdges(cy: CyInstance): Set<string> {
  const ids = new Set<string>();
  cy.edges(".erd-rel").forEach((e) => {
    ids.add(e.data("source") as string);
    ids.add(e.data("target") as string);
  });
  return ids;
}

// Push overlapping compound (.erd-domain) nodes apart so they don't visually
// overlap. Translates children of the "right/bottom" compound in each pair.
// Must be called after all children have been positioned (including orphan grid).
export function resolveCompoundOverlaps(cy: CyInstance): void {
  type BB = { x1: number; y1: number; x2: number; y2: number };
  const PAD = 60;
  const MAX_ITER = 20;

  const domainArr: ReturnType<typeof cy.nodes>[number][] = [];
  cy.nodes(".erd-domain").forEach((d) => {
    if (!(d.children() as unknown as { empty(): boolean }).empty()) domainArr.push(d);
  });
  if (domainArr.length < 2) return;

  for (let iter = 0; iter < MAX_ITER; iter++) {
    const bbs = domainArr.map((d) =>
      (d as unknown as { boundingBox(o: object): BB }).boundingBox({ includeLabels: true }),
    );
    let moved = false;

    for (let i = 0; i < domainArr.length; i++) {
      for (let j = i + 1; j < domainArr.length; j++) {
        const a = bbs[i];
        const b = bbs[j];
        const overlapX = Math.min(a.x2, b.x2) - Math.max(a.x1, b.x1) + PAD;
        const overlapY = Math.min(a.y2, b.y2) - Math.max(a.y1, b.y1) + PAD;
        if (overlapX <= 0 || overlapY <= 0) continue;

        // Push j away from i along the axis with the smaller overlap.
        let dx = 0,
          dy = 0;
        if (overlapX <= overlapY) {
          dx = b.x1 < a.x1 ? -overlapX : overlapX;
        } else {
          dy = b.y1 < a.y1 ? -overlapY : overlapY;
        }
        domainArr[j].children().forEach((child) => {
          const p = (child as { position(): Pt }).position();
          (child as { position(p: Pt): void }).position({ x: p.x + dx, y: p.y + dy });
        });
        // Update j's bbox in place for subsequent pairs this iteration.
        bbs[j] = { x1: b.x1 + dx, y1: b.y1 + dy, x2: b.x2 + dx, y2: b.y2 + dy };
        moved = true;
      }
    }
    if (!moved) break;
  }
}

export type PackBox = { id: string; w: number; h: number };

/** Shelf packing of `boxes` into rows no wider than `rowW`, tallest first; the top-left of each
 * box, and the packing's width and height. */
function shelf(
  boxes: PackBox[],
  rowW: number,
  pad: number,
): { at: Map<string, { x: number; y: number }>; w: number; h: number } {
  const at = new Map<string, { x: number; y: number }>();
  let cursorX = 0,
    cursorY = 0,
    rowH = 0,
    w = 0;
  for (const box of boxes) {
    if (cursorX > 0 && cursorX + box.w > rowW) {
      cursorX = 0;
      cursorY += rowH + pad;
      rowH = 0;
    }
    at.set(box.id, { x: cursorX, y: cursorY });
    w = Math.max(w, cursorX + box.w);
    cursorX += box.w + pad;
    rowH = Math.max(rowH, box.h);
  }
  return { at, w, h: cursorY + rowH };
}

/**
 * Where each domain box goes so that the whole is drawn as large as a pane of `aspectRatio`
 * (width / height) allows. Every row width a run of boxes can make is tried, and the packing whose
 * fit into the pane is largest wins -- a wide pane puts domains side by side, a tall one stacks
 * them. (A row width derived from the boxes' total area is the width of a packing with no gaps
 * and no ragged rows; the shelf, with both, wraps nearly every box under it and stacks them.)
 */
export function packBoxes(
  boxes: PackBox[],
  aspectRatio: number,
  pad: number,
): Map<string, { x: number; y: number }> {
  const ordered = [...boxes].sort((a, b) => b.h - a.h);
  const widths = new Set<number>();
  for (let i = 0; i < ordered.length; i++) {
    let run = -pad;
    for (let j = i; j < ordered.length; j++) {
      run += ordered[j].w + pad;
      widths.add(run);
    }
  }
  let best: ReturnType<typeof shelf> | null = null;
  let bestScale = 0;
  for (const rowW of [...widths].sort((a, b) => a - b)) {
    const packed = shelf(ordered, rowW, pad);
    // The scale the packing is drawn at in a pane `aspectRatio` wide and 1 high.
    const scale = Math.min(aspectRatio / packed.w, 1 / packed.h);
    if (scale > bestScale) {
      best = packed;
      bestScale = scale;
    }
  }
  return best?.at ?? new Map();
}

// Pass 2 of the two-pass layout: arrange whole-domain boxes to fill the container's aspect ratio
// instead of letting ELK's single flow axis stack them. Each domain is laid out internally by ELK
// (pass 1); here each domain box is translated as a rigid group (all children moved by the same
// delta), so internal table positions and edges are preserved.
export function packDomains(cy: CyInstance, aspectRatio: number): void {
  type BB = { x1: number; y1: number; x2: number; y2: number; w: number; h: number };
  const PAD = 60;
  const domains: Array<{ node: ReturnType<typeof cy.nodes>[number]; bb: BB }> = [];
  cy.nodes(".erd-domain").forEach((d) => {
    if ((d.children() as unknown as { empty(): boolean }).empty()) return;
    const bb = (d as unknown as { boundingBox(o: object): BB }).boundingBox({
      includeLabels: true,
    });
    domains.push({ node: d, bb });
  });
  if (domains.length < 2) return;

  const placements = packBoxes(
    domains.map((d) => ({ id: (d.node as { id(): string }).id(), w: d.bb.w, h: d.bb.h })),
    aspectRatio,
    PAD,
  );

  // Translate each domain's children so its bbox top-left lands on the placement.
  cy.batch(() => {
    for (const d of domains) {
      const target = placements.get((d.node as { id(): string }).id());
      if (!target) continue;
      const dx = target.x - d.bb.x1;
      const dy = target.y - d.bb.y1;
      if (dx === 0 && dy === 0) continue;
      d.node.children().forEach((child) => {
        const p = (child as { position(): Pt }).position();
        (child as { position(p: Pt): void }).position({ x: p.x + dx, y: p.y + dy });
      });
    }
  });
}

// Phase 2: place isolated nodes (no cross-domain edges) in a compact grid
// below each domain's post-layout bounding box. domainBboxes must be computed
// BEFORE isolated nodes are shown (while only connected nodes are visible),
// so the bbox reflects only the connected-node region.
export function placeIsolatedGrid(
  cy: CyInstance,
  isolatedIds: Set<string>,
  domainBboxes: Map<string, { x1: number; x2: number; y2: number }>,
): void {
  const PAD = 20;
  cy.nodes(".erd-domain").forEach((domain) => {
    const domainId = (domain as { id(): string }).id();
    const isolated: Array<{ id: string }> = [];
    domain.children().forEach((n) => {
      if (isolatedIds.has((n as { id(): string }).id()))
        isolated.push({ id: (n as { id(): string }).id() });
    });
    if (isolated.length === 0) return;

    // Measure actual node sizes (nodes are visible at this point).
    type NS = { w: number; h: number };
    const sizes: NS[] = isolated.map(({ id }) => {
      const n = cy.getElementById(id) as unknown as { width(): number; height(): number };
      return { w: n.width(), h: n.height() };
    });
    const cols = Math.ceil(Math.sqrt(isolated.length));
    const rows = Math.ceil(isolated.length / cols);
    const colWidths = Array.from({ length: cols }, (_, c) =>
      Math.max(...isolated.map((_, i) => (i % cols === c ? sizes[i].w : 0))),
    );
    const rowHeights = Array.from({ length: rows }, (_, r) =>
      Math.max(...isolated.map((_, i) => (Math.floor(i / cols) === r ? sizes[i].h : 0))),
    );
    const colX = colWidths.reduce<number[]>((acc, _w, i) => {
      acc.push(i === 0 ? 0 : acc[i - 1] + colWidths[i - 1] + PAD);
      return acc;
    }, []);
    const rowY = rowHeights.reduce<number[]>((acc, _h, i) => {
      acc.push(i === 0 ? 0 : acc[i - 1] + rowHeights[i - 1] + PAD);
      return acc;
    }, []);
    const totalW = colX[cols - 1] + colWidths[cols - 1];

    // Use the pre-captured connected-only bbox. If the domain had no connected
    // nodes, fall back to the domain node's own position.
    const dbb = domainBboxes.get(domainId);
    let gridOriginX: number, gridOriginY: number;
    if (dbb) {
      gridOriginX = (dbb.x1 + dbb.x2) / 2 - totalW / 2;
      gridOriginY = dbb.y2 + PAD * 2;
    } else {
      const dpos = (domain as unknown as { position(): Pt }).position();
      gridOriginX = dpos.x - totalW / 2;
      gridOriginY = dpos.y;
    }

    cy.batch(() => {
      isolated.forEach(({ id }, i) => {
        const col = i % cols;
        const row = Math.floor(i / cols);
        const newPos = {
          x: gridOriginX + colX[col] + colWidths[col] / 2,
          y: gridOriginY + rowY[row] + rowHeights[row] / 2,
        };
        (cy.getElementById(id) as unknown as { position(p: Pt): void }).position(newPos);
      });
    });
  });
}
