// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the legend sits over the canvas's top right; a fit keeps the diagram below it, so the
// legend never covers a lane's or a group's title (the ontology's TERMS lane at 1900).

import { describe, it, expect } from "vitest";
import type { CyInstance } from "../components/graph/cytoscape-types";
import { fitClear } from "../components/provisa-graph/viewport";

type BB = { x1: number; y1: number; x2: number; y2: number; w: number; h: number };

/** A canvas ``w`` x ``h`` in a stage, with a legend reaching ``legendBottom`` px down it (none at
 * 0), and a cytoscape stand-in drawing the model box ``model`` as cytoscape's fit would. */
function stage(w: number, h: number, legendBottom: number, model: BB) {
  const root = document.createElement("div");
  root.className = "pg-stage";
  const box = document.createElement("div");
  Object.defineProperty(box, "clientWidth", { value: w });
  Object.defineProperty(box, "clientHeight", { value: h });
  box.getBoundingClientRect = () => ({ top: 100, left: 0, bottom: 100 + h, right: w }) as DOMRect;
  root.appendChild(box);
  if (legendBottom > 0) {
    const legend = document.createElement("div");
    legend.className = "pg-legend";
    legend.getBoundingClientRect = () => ({ top: 108, bottom: 100 + legendBottom }) as DOMRect;
    root.appendChild(legend);
  }
  let zoom = 1;
  let pan = { x: 0, y: 0 };
  const rendered = (): BB => {
    const r = {
      x1: model.x1 * zoom + pan.x,
      y1: model.y1 * zoom + pan.y,
      x2: model.x2 * zoom + pan.x,
      y2: model.y2 * zoom + pan.y,
    };
    return { ...r, w: r.x2 - r.x1, h: r.y2 - r.y1 };
  };
  const cy = {
    container: () => box,
    elements: () => ({ boundingBox: () => model, renderedBoundingBox: rendered }),
    fit: (_e: unknown, p = 0) => {
      zoom = Math.min((w - 2 * p) / model.w, (h - 2 * p) / model.h);
      pan = {
        x: (w - model.w * zoom) / 2 - model.x1 * zoom,
        y: (h - model.h * zoom) / 2 - model.y1 * zoom,
      };
    },
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
  return { cy, rendered };
}

// A wide diagram: a plain fit fills the width and centres it, its top 10px under the canvas's.
const WIDE: BB = { x1: 0, y1: 0, x2: 1200, y2: 700, w: 1200, h: 700 };

describe("fitClear", () => {
  it("is a plain fit with no legend", () => {
    const { cy, rendered } = stage(1200, 720, 0, WIDE);
    fitClear(cy, 10);
    expect(rendered().y1).toBeCloseTo(10, 9);
  });

  it("fits the diagram below the legend when a plain fit runs under it", () => {
    const { cy, rendered } = stage(1200, 720, 44, WIDE);
    fitClear(cy, 10);
    const bb = rendered();
    expect(bb.y1).toBeGreaterThanOrEqual(44 - 1e-9);
    expect(bb.y2).toBeLessThanOrEqual(720 - 10 + 1e-9);
    expect(bb.x1).toBeGreaterThanOrEqual(10 - 1e-9);
    expect(bb.x2).toBeLessThanOrEqual(1200 - 10 + 1e-9);
  });

  it("leaves a fit already clear of the legend as it is", () => {
    // A tall pane: the diagram is centred well below the legend.
    const { cy, rendered } = stage(1200, 1400, 44, WIDE);
    fitClear(cy, 10);
    const bb = rendered();
    expect(bb.w).toBeCloseTo(1180, 9);
    expect(bb.y1).toBeGreaterThan(44);
  });
});
