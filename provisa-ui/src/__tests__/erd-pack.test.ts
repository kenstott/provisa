// Copyright (c) 2026 Kenneth Stott
// Canary: 6d1f9a42-8b3e-4c75-a0d2-3e9b7c5f1a86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD's domains packed to the pane: side by side in a wide one, stacked in a tall one, so the
// diagram is drawn as large as the pane allows. The boxes are the five domains of the maintainer's
// screenshot (2026-10-31), which the area-derived row width stacked one under another.

import { describe, expect, it } from "vitest";
import { packDomains } from "../components/erd/sections/erd-layout";
import type { CyInstance } from "../components/graph/cytoscape-types";

type Box = { id: string; w: number; h: number };
type Placed = { id: string; x1: number; y1: number; x2: number; y2: number };

const PAD = 60;

/** A diagram of domain boxes, each with one table at its centre, stacked as ELK leaves them. */
function diagram(boxes: Box[]) {
  let y = 0;
  const domains = boxes.map((b) => {
    const at = { x: b.w / 2, y: y + b.h / 2 };
    y += b.h + PAD;
    const child = {
      position: (p?: { x: number; y: number }) => {
        if (p) {
          at.x = p.x;
          at.y = p.y;
        }
        return at;
      },
    };
    return {
      box: b,
      at,
      node: {
        id: () => b.id,
        children: () => ({ empty: () => false, forEach: (f: (c: unknown) => void) => f(child) }),
        boundingBox: () => ({
          x1: at.x - b.w / 2,
          y1: at.y - b.h / 2,
          x2: at.x + b.w / 2,
          y2: at.y + b.h / 2,
          w: b.w,
          h: b.h,
        }),
      },
    };
  });
  const cy = {
    nodes: () => ({ forEach: (f: (n: unknown) => void) => domains.forEach((d) => f(d.node)) }),
    batch: (f: () => void) => f(),
  } as unknown as CyInstance;
  const placed = (): Placed[] =>
    domains.map((d) => ({
      id: d.box.id,
      x1: d.at.x - d.box.w / 2,
      y1: d.at.y - d.box.h / 2,
      x2: d.at.x + d.box.w / 2,
      y2: d.at.y + d.box.h / 2,
    }));
  return { cy, placed };
}

const extent = (placed: Placed[]) => ({
  w: Math.max(...placed.map((p) => p.x2)) - Math.min(...placed.map((p) => p.x1)),
  h: Math.max(...placed.map((p) => p.y2)) - Math.min(...placed.map((p) => p.y1)),
});

/** The scale the packing is drawn at in a pane `aspect` wide and 1 high. */
const scaleIn = (placed: Placed[], aspect: number) => {
  const { w, h } = extent(placed);
  return Math.min(aspect / w, 1 / h);
};

const rows = (placed: Placed[]) => new Set(placed.map((p) => Math.round(p.y1))).size;

const SCREENSHOT: Box[] = [
  { id: "d:sales", w: 400, h: 190 },
  { id: "d:finance", w: 340, h: 150 },
  { id: "d:hr", w: 280, h: 150 },
  { id: "d:marketing", w: 330, h: 145 },
  { id: "d:ops", w: 215, h: 90 },
];

describe("domains packed to the pane", () => {
  it("go side by side in a wide pane, and the diagram is drawn larger than stacked", () => {
    const aspect = 1245 / 785;
    const stacked = diagram(SCREENSHOT);
    const stackedScale = scaleIn(stacked.placed(), aspect);
    const { cy, placed } = diagram(SCREENSHOT);
    packDomains(cy, aspect);
    expect(rows(placed())).toBeLessThanOrEqual(3);
    expect(scaleIn(placed(), aspect)).toBeGreaterThan(stackedScale * 1.5);
  });

  it("stack in a tall pane", () => {
    const { cy, placed } = diagram(SCREENSHOT);
    packDomains(cy, 0.3);
    expect(rows(placed())).toBe(SCREENSHOT.length);
  });

  it("never overlap, and keep the gap between them", () => {
    for (const aspect of [0.3, 0.8, 1, 1.6, 2.4, 4]) {
      const { cy, placed } = diagram(SCREENSHOT);
      packDomains(cy, aspect);
      const p = placed();
      for (let i = 0; i < p.length; i++)
        for (let j = i + 1; j < p.length; j++) {
          const apartX = p[i].x2 + PAD <= p[j].x1 + 1e-9 || p[j].x2 + PAD <= p[i].x1 + 1e-9;
          const apartY = p[i].y2 + PAD <= p[j].y1 + 1e-9 || p[j].y2 + PAD <= p[i].y1 + 1e-9;
          expect(apartX || apartY, `${p[i].id} and ${p[j].id} at ${aspect}`).toBe(true);
        }
    }
  });

  it("is drawn at least as large as any single row or single column of the same boxes", () => {
    for (const aspect of [0.5, 1, 1.6, 3]) {
      const { cy, placed } = diagram(SCREENSHOT);
      packDomains(cy, aspect);
      const oneRowW = SCREENSHOT.reduce((s, b) => s + b.w, 0) + PAD * (SCREENSHOT.length - 1);
      const oneRowH = Math.max(...SCREENSHOT.map((b) => b.h));
      const oneColW = Math.max(...SCREENSHOT.map((b) => b.w));
      const oneColH = SCREENSHOT.reduce((s, b) => s + b.h, 0) + PAD * (SCREENSHOT.length - 1);
      const best = Math.max(
        Math.min(aspect / oneRowW, 1 / oneRowH),
        Math.min(aspect / oneColW, 1 / oneColH),
      );
      expect(scaleIn(placed(), aspect)).toBeGreaterThanOrEqual(best * (1 - 1e-14));
    }
  });
});
