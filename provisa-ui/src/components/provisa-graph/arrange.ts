// Copyright (c) 2026 Kenneth Stott
// Canary: f572f0bd-8e40-4402-bb51-3e2c8835d351
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CyInstance } from "../graph/cytoscape-types";

type Pt = { x: number; y: number };

// Returns the set of node IDs that have at least one edge (any kind).
// True orphans (no edges) are excluded from fCoSE and placed in a grid instead.
export function nodesWithEdges(cy: CyInstance): Set<string> {
  const ids = new Set<string>();
  cy.edges(".pg-edge").forEach((e) => {
    ids.add(e.data("source") as string);
    ids.add(e.data("target") as string);
  });
  return ids;
}

// Push overlapping compound (.pg-group) nodes apart so they don't visually
// overlap. Translates children of the "right/bottom" compound in each pair.
// Must be called after all children have been positioned (including orphan grid).
export function resolveCompoundOverlaps(cy: CyInstance): void {
  type BB = { x1: number; y1: number; x2: number; y2: number };
  const PAD = 60;
  const MAX_ITER = 20;

  const domainArr: ReturnType<typeof cy.nodes>[number][] = [];
  cy.nodes(".pg-group").forEach((d) => {
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
  cy.nodes(".pg-group").forEach((d) => {
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
  cy.nodes(".pg-group").forEach((domain) => {
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

// Push connected nodes apart on each axis independently.
// Primary axis gets MIN_LABEL_GAP; secondary axis gets 0 (just clear overlap).
// Runs after re-snap so the re-snap cannot undo the push.
// Uses Math.ceil so tiny gaps always produce at least one grid step of movement.
export function enforceEdgeGaps(cy: CyInstance, gridSnap: number): void {
  type BB = { x1: number; x2: number; y1: number; y2: number };
  const MIN_LABEL_GAP = 60;
  const g = gridSnap > 0 ? gridSnap : 1;
  cy.edges(".pg-edge").forEach((edge) => {
    const s = cy.getElementById(edge.data("source") as string) as unknown as {
      position(): Pt;
      position(p: Pt): void;
      boundingBox(o: object): BB;
    };
    const t = cy.getElementById(edge.data("target") as string) as unknown as {
      position(): Pt;
      position(p: Pt): void;
      boundingBox(o: object): BB;
    };
    let sp = s.position();
    let tp = t.position();
    const sbb = s.boundingBox({});
    const tbb = t.boundingBox({});
    const xSep = tp.x > sp.x ? tbb.x1 - sbb.x2 : sbb.x1 - tbb.x2;
    const ySep = tp.y > sp.y ? tbb.y1 - sbb.y2 : sbb.y1 - tbb.y2;
    const primaryY = Math.abs(sp.x - tp.x) <= Math.abs(sp.y - tp.y);
    // Secondary axis is only pushed when there is actual 2D overlap (both seps negative).
    // This avoids splitting nodes that share a row/column but don't visually overlap.
    const has2dOverlap = xSep < 0 && ySep < 0;

    // Y axis: MIN_LABEL_GAP if primary; clear 2D overlap only on secondary.
    if (primaryY ? ySep < MIN_LABEL_GAP : has2dOverlap && ySep < 0) {
      const yNeeded = primaryY ? MIN_LABEL_GAP : 0;
      const push = Math.ceil((yNeeded - ySep) / 2 / g) * g;
      if (tp.y > sp.y) {
        s.position({ ...sp, y: sp.y - push });
        t.position({ ...tp, y: tp.y + push });
      } else {
        s.position({ ...sp, y: sp.y + push });
        t.position({ ...tp, y: tp.y - push });
      }
      sp = s.position();
      tp = t.position();
    }

    // X axis: MIN_LABEL_GAP if primary; clear 2D overlap only on secondary.
    if (!primaryY ? xSep < MIN_LABEL_GAP : has2dOverlap && xSep < 0) {
      const xNeeded = !primaryY ? MIN_LABEL_GAP : 0;
      const push = Math.ceil((xNeeded - xSep) / 2 / g) * g;
      if (tp.x > sp.x) {
        s.position({ ...sp, x: sp.x - push });
        t.position({ ...tp, x: tp.x + push });
      } else {
        s.position({ ...sp, x: sp.x + push });
        t.position({ ...tp, x: tp.x - push });
      }
    }
  });
}

/**
 * Moves tables apart so that no two tables of a domain overlap, nor come closer than `TABLE_GAP`.
 * Tables are taken top to bottom, then left to right; one that meets a table already taken moves
 * right, past it, in whole grid steps -- along its row, so ELK's layers stay rows. Each move takes
 * a table past one more of the finitely many tables already taken, so the pass ends.
 */
export function separateTables(cy: CyInstance, skip: Set<string>, gridSnap: number): void {
  type BB = { x1: number; x2: number; y1: number; y2: number };
  type Table = { node: { position(): Pt; position(p: Pt): void }; bb: BB };
  const TABLE_GAP = 20;
  const g = gridSnap > 0 ? gridSnap : 1;
  const byDomain = new Map<string, Table[]>();
  cy.nodes(".pg-node").forEach((n) => {
    if (skip.has((n as { id(): string }).id())) return;
    const domainId = n.data("parent") as string;
    const node = n as unknown as Table["node"] & { boundingBox(o: object): BB };
    const tables = byDomain.get(domainId) ?? [];
    tables.push({ node, bb: node.boundingBox({}) });
    byDomain.set(domainId, tables);
  });
  const near = (a: BB, b: BB) =>
    a.x1 < b.x2 + TABLE_GAP &&
    b.x1 < a.x2 + TABLE_GAP &&
    a.y1 < b.y2 + TABLE_GAP &&
    b.y1 < a.y2 + TABLE_GAP;
  cy.batch(() => {
    for (const tables of byDomain.values()) {
      tables.sort((a, b) => a.bb.y1 - b.bb.y1 || a.bb.x1 - b.bb.x1);
      const taken: BB[] = [];
      for (const { node, bb } of tables) {
        let dx = 0;
        for (;;) {
          const at = { ...bb, x1: bb.x1 + dx, x2: bb.x2 + dx };
          const hit = taken.find((t) => near(at, t));
          if (!hit) break;
          dx = Math.ceil((hit.x2 + TABLE_GAP - bb.x1) / g) * g;
        }
        if (dx !== 0) {
          const p = node.position();
          node.position({ x: p.x + dx, y: p.y });
        }
        taken.push({ ...bb, x1: bb.x1 + dx, x2: bb.x2 + dx });
      }
    }
  });
}

/**
 * The automatic layout after ELK: snaps to the grid, places each domain's orphans in a grid below
 * its connected tables, keeps related tables apart, separates the tables of a domain, shows or
 * hides the orphans, and packs the domains to a pane of `aspectRatio` (width / height; none
 * leaves them where they are). Returns the orphans and each domain's connected extent, which the
 * orphan toggle places against later.
 */
export function arrangeLayered(
  cy: CyInstance,
  {
    gridSnap,
    showOrphans,
    aspectRatio,
  }: { gridSnap: number; showOrphans: boolean; aspectRatio: number | null },
): {
  isolatedIds: Set<string>;
  connectedBboxes: Map<string, { x1: number; x2: number; y2: number }>;
} {
  if (gridSnap > 0) {
    const g = gridSnap;
    cy.nodes(".pg-node").forEach((n) => {
      const p = (n as { position(): { x: number; y: number } }).position();
      (n as { position(p: { x: number; y: number }): void }).position({
        x: Math.round(p.x / g) * g,
        y: Math.round(p.y / g) * g,
      });
    });
    // Align connected pairs that are within 1 grid step in x or y — eliminates
    // unnecessary bends in right-angle edge routing after fCoSE snap jitter.
    const ALIGN_THRESHOLD = g;
    cy.edges(".pg-edge").forEach((edge) => {
      const s = cy.getElementById(edge.data("source") as string) as unknown as {
        position(): Pt;
        position(p: Pt): void;
      };
      const t = cy.getElementById(edge.data("target") as string) as unknown as {
        position(): Pt;
        position(p: Pt): void;
      };
      const sp = s.position();
      const tp = t.position();
      const dx = Math.abs(sp.x - tp.x);
      const dy = Math.abs(sp.y - tp.y);
      if (dx <= ALIGN_THRESHOLD && dx < dy) {
        const ax = Math.round((sp.x + tp.x) / 2 / g) * g;
        s.position({ ...sp, x: ax });
        t.position({ ...tp, x: ax });
      } else if (dy <= ALIGN_THRESHOLD && dy < dx) {
        const ay = Math.round((sp.y + tp.y) / 2 / g) * g;
        s.position({ ...sp, y: ay });
        t.position({ ...tp, y: ay });
      }
    });
  }

  // Collapsed domain leaf nodes placed inside an expanded compound by fCoSE render
  // behind its background and become invisible. Move them outside.
  cy.nodes(".pg-group").forEach((leafDomain) => {
    if (!(leafDomain.children() as unknown as { empty(): boolean }).empty()) return;
    cy.nodes(".pg-group").forEach((compound) => {
      if (compound.id() === leafDomain.id()) return;
      if ((compound.children() as unknown as { empty(): boolean }).empty()) return;
      const cbb = (
        compound as unknown as {
          boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number };
        }
      ).boundingBox({});
      const pos = (leafDomain as { position(): { x: number; y: number } }).position();
      if (pos.x > cbb.x1 && pos.x < cbb.x2 && pos.y > cbb.y1 && pos.y < cbb.y2) {
        (leafDomain as { position(p: { x: number; y: number }): void }).position({
          x: cbb.x2 + 120,
          y: (cbb.y1 + cbb.y2) / 2,
        });
      }
    });
  });

  // Capture per-domain bboxes NOW — true orphans are still hidden (display:none),
  // so the compound bbox reflects only edge-connected nodes.
  const edgeConnectedNow = nodesWithEdges(cy);
  const domainBboxes = new Map<string, { x1: number; x2: number; y2: number }>();
  cy.nodes(".pg-group").forEach((domain) => {
    let hasConnected = false;
    domain.children().forEach((n) => {
      if (edgeConnectedNow.has((n as { id(): string }).id())) hasConnected = true;
    });
    if (!hasConnected) return;
    const bb = (
      domain as unknown as { boundingBox(o: object): { x1: number; x2: number; y2: number } }
    ).boundingBox({ includeLabels: false });
    domainBboxes.set((domain as { id(): string }).id(), bb);
  });

  // Phase 2: place orphan grid, then apply showOrphans visibility.
  const edgeConnected = nodesWithEdges(cy);
  const isolatedIds = new Set(
    cy
      .nodes(".pg-node")
      .filter((n) => !edgeConnected.has((n as { id(): string }).id()))
      .map((n) => (n as { id(): string }).id()),
  );
  placeIsolatedGrid(cy, isolatedIds, domainBboxes);

  // Re-snap all nodes to grid after post-processing moves.
  if (gridSnap > 0) {
    const g = gridSnap;
    cy.nodes(".pg-node").forEach((n) => {
      const p = (n as { position(): Pt }).position();
      (n as { position(p: Pt): void }).position({
        x: Math.round(p.x / g) * g,
        y: Math.round(p.y / g) * g,
      });
    });
  }

  enforceEdgeGaps(cy, gridSnap);
  // The gap pass moves each edge's two tables without regard to the tables around them, and can
  // push one onto another in its domain: separate them before the orphan grid is placed below.
  separateTables(cy, isolatedIds, gridSnap);

  // Refresh per-domain bboxes after gap enforcement moved connected nodes,
  // then re-place orphan grid so it lands below the final connected positions.
  type BBxy = { x1: number; x2: number; y2: number };
  const freshBboxes = new Map<string, BBxy>();
  cy.nodes(".pg-group").forEach((domain) => {
    let x1 = Infinity,
      x2 = -Infinity,
      y2 = -Infinity,
      hasConnected = false;
    domain.children().forEach((child) => {
      if (isolatedIds.has((child as { id(): string }).id())) return;
      const bb = (
        child as unknown as {
          boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number };
        }
      ).boundingBox({});
      x1 = Math.min(x1, bb.x1);
      x2 = Math.max(x2, bb.x2);
      y2 = Math.max(y2, bb.y2);
      hasConnected = true;
    });
    if (hasConnected) freshBboxes.set((domain as { id(): string }).id(), { x1, x2, y2 });
  });
  placeIsolatedGrid(cy, isolatedIds, freshBboxes);

  // Apply final orphan visibility without triggering re-layout.
  if (showOrphans) {
    cy.nodes(".pg-node").style("display", "element");
  } else {
    isolatedIds.forEach((id) => {
      (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style(
        "display",
        "none",
      );
    });
  }

  // Pack domain boxes to the container aspect ratio (side-by-side vs stacked)
  // now that each domain's full extent — connected nodes + orphan grid — is
  // known. resolveCompoundOverlaps below is a safety net; packing already
  // separates boxes.
  if (aspectRatio !== null) packDomains(cy, aspectRatio);

  // Resolve compound overlaps AFTER orphans are shown so compound bboxes
  // include the full orphan grid area (hidden nodes are excluded from bbox).
  resolveCompoundOverlaps(cy);

  return { isolatedIds, connectedBboxes: freshBboxes };
}
