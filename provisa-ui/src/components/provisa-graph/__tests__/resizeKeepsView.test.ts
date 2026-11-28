// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3b9e14-2a6c-4f58-b0e1-5c9a8d2f4e73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: a resize keeps the view's mode. The maintainer fitted the Relationships ERD to show the
// whole model, dragged the splitter to the detail panel, and the ERD jumped to a zoomed-in view:
// every resize ran the opening fit (fitClear), whose readable-zoom floor draws a diagram too big for
// the narrower canvas from its top left. Fitted whole, a resize fits it whole again; zoomed or
// moved by the reader, it keeps the zoom and the point of the diagram at the canvas's centre.

import { describe, expect, it } from "vitest";
import type { CyInstance } from "../../graph/cytoscape-types";
import {
  GRAPH_READABLE_ZOOM,
  fitClear,
  fitWhole,
  followResize,
  zoomAbout,
  scrollBy,
  viewMode,
  zoomAboutCentre,
} from "../viewport";

type BB = { x1: number; y1: number; x2: number; y2: number; w: number; h: number };
const bb = (x1: number, y1: number, x2: number, y2: number): BB => ({
  x1,
  y1,
  x2,
  y2,
  w: x2 - x1,
  h: y2 - y1,
});

/** A canvas with no legend, its size changeable, drawing ``model`` as cytoscape renders it at its
 * zoom and pan. */
function stage(width: number, height: number, model: BB, least = 0) {
  const size = { width, height };
  // The instance's least zoom: like cytoscape, it holds every zoom to it.
  let minZoom = least;
  const box = document.createElement("div");
  Object.defineProperty(box, "clientWidth", { get: () => size.width });
  Object.defineProperty(box, "clientHeight", { get: () => size.height });
  box.getBoundingClientRect = () =>
    ({ top: 0, left: 0, bottom: size.height, right: size.width }) as DOMRect;
  let zoom = 1;
  let pan = { x: 0, y: 0 };
  const render = (m: BB): BB =>
    bb(m.x1 * zoom + pan.x, m.y1 * zoom + pan.y, m.x2 * zoom + pan.x, m.y2 * zoom + pan.y);
  const cy = {
    container: () => box,
    elements: () => ({ boundingBox: () => model, renderedBoundingBox: () => render(model) }),
    fit: () => {},
    zoom: (z?: number) => {
      if (z === undefined) return zoom;
      zoom = Math.max(z, minZoom);
      return undefined;
    },
    minZoom: (z?: number) => {
      if (z === undefined) return minZoom;
      minZoom = z;
      return undefined;
    },
    pan: (p?: { x: number; y: number }) => {
      if (p === undefined) return pan;
      pan = p;
      return undefined;
    },
  } as unknown as CyInstance;
  /** The canvas resized to ``w`` x ``h``: the view follows, from the size it was. */
  const resize = (w: number, h: number) => {
    const was = { ...size };
    size.width = w;
    size.height = h;
    return followResize(cy, was);
  };
  /** The model point drawn at the canvas's centre. */
  const centre = () => ({
    x: (size.width / 2 - pan.x) / zoom,
    y: (size.height / 2 - pan.y) / zoom,
  });
  return { cy, resize, centre, whole: () => render(model), size };
}

const inside = (r: BB, w: number, h: number, pad: number) =>
  r.x1 >= pad - 1e-9 && r.y1 >= pad - 1e-9 && r.x2 <= w - pad + 1e-9 && r.y2 <= h - pad + 1e-9;

// An ERD 1600 x 600 model px: fits a 1900 canvas whole above the readable zoom, not a 900 one.
const MODEL = bb(0, 0, 1600, 600);
const PAD = 10;

describe("a resize keeps the view's mode (REQ-2277)", () => {
  it("fitted whole, a narrower canvas fits it whole again, under the readable zoom", () => {
    const g = stage(1900, 800, MODEL);
    fitWhole(g.cy, PAD);
    expect(inside(g.whole(), 1900, 800, PAD)).toBe(true);
    // The splitter dragged: the canvas narrows beside the detail panel.
    expect(g.resize(900, 800)).toBe("whole");
    expect(inside(g.whole(), 900, 800, PAD)).toBe(true);
    // It is not drawn at the readable zoom from its top left, as main drew it.
    expect(g.cy.zoom()).toBeLessThan(GRAPH_READABLE_ZOOM);
  });

  it("opened showing the whole diagram, a narrower canvas still shows it whole", () => {
    const g = stage(1900, 800, MODEL);
    fitClear(g.cy, PAD);
    expect(inside(g.whole(), 1900, 800, PAD)).toBe(true);
    expect(g.resize(900, 800)).toBe("whole");
    expect(inside(g.whole(), 900, 800, PAD)).toBe(true);
  });

  it("opened at the readable zoom (too big to fit), a resize opens it again", () => {
    const g = stage(900, 800, MODEL);
    fitClear(g.cy, PAD);
    expect(viewMode(g.cy).kind).toBe("opening");
    // The caller opens it again: fitClear, its focus, its marks.
    expect(g.resize(1000, 800)).toBe("opening");
  });

  it("zoomed by the reader, a resize keeps the zoom and the diagram's point at the centre", () => {
    const g = stage(1900, 800, MODEL);
    fitWhole(g.cy, PAD);
    zoomAboutCentre(g.cy, 1.3);
    const zoom = g.cy.zoom();
    const centre = g.centre();
    expect(g.resize(900, 800)).toBe("kept");
    expect(g.cy.zoom()).toBe(zoom);
    expect(g.centre().x).toBeCloseTo(centre.x, 6);
    expect(g.centre().y).toBeCloseTo(centre.y, 6);
  });

  it("scrolled by the reader, a resize keeps the zoom and the diagram's point at the centre", () => {
    const g = stage(900, 400, MODEL);
    fitClear(g.cy, PAD);
    expect(scrollBy(g.cy, 200, 0, PAD)).toBe(true);
    const zoom = g.cy.zoom();
    const centre = g.centre();
    expect(g.resize(1200, 500)).toBe("kept");
    expect(g.cy.zoom()).toBe(zoom);
    expect(g.centre().x).toBeCloseTo(centre.x, 6);
    expect(g.centre().y).toBeCloseTo(centre.y, 6);
  });

  it("fit again after a zoom, a resize fits it whole again", () => {
    const g = stage(1900, 800, MODEL);
    fitWhole(g.cy, PAD);
    zoomAboutCentre(g.cy, 2);
    fitWhole(g.cy, PAD);
    expect(g.resize(700, 600)).toBe("whole");
    expect(inside(g.whole(), 700, 600, PAD)).toBe(true);
  });
});

// The lead's decision (REQ-2239, FIT MEANS THE WHOLE IMAGE): the least zoom (0.1 on the ERD) bounds
// the reader's own zooming only. A model too big for its canvas at that zoom was fitted at it,
// cut off at the canvas's edges, and a resize kept it so.
describe("a fit shows the whole model below the instance's least zoom", () => {
  // 20000 model px across: whole in a 900 px canvas only at about 0.044.
  const HUGE = bb(0, 0, 20000, 4000);
  const LEAST = 0.1;

  it("the fit control zooms below the least zoom to show the whole model", () => {
    const g = stage(900, 800, HUGE, LEAST);
    fitWhole(g.cy, PAD);
    expect(g.cy.zoom()).toBeLessThan(LEAST);
    expect(inside(g.whole(), 900, 800, PAD)).toBe(true);
  });

  it("fitted whole, a narrower canvas fits it whole again, lower still", () => {
    const g = stage(1900, 800, HUGE, LEAST);
    fitWhole(g.cy, PAD);
    expect(g.resize(700, 800)).toBe("whole");
    expect(inside(g.whole(), 700, 800, PAD)).toBe(true);
  });

  it("the reader zooms out no further than the least zoom, or the whole fit when that is lower", () => {
    const g = stage(900, 800, HUGE, LEAST);
    fitWhole(g.cy, PAD);
    const fitted = g.cy.zoom();
    zoomAboutCentre(g.cy, 0.5);
    expect(g.cy.zoom()).toBeCloseTo(fitted, 9);
    // A model that fits above the least zoom: the reader is held to it.
    const small = stage(900, 800, bb(0, 0, 1600, 600), LEAST);
    fitWhole(small.cy, PAD);
    zoomAbout(small.cy, 0.001, { x: 0, y: 0 });
    expect(small.cy.zoom()).toBe(LEAST);
  });
});
