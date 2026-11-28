// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7b9c21-3d58-4a06-b1f2-8c6d0e9a5f37
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the one image export every graph uses: the whole drawing on the
// canvas colour with a margin, named after the view, and the hulls drawn over the canvas painted where
// they sit on the drawing.
//
// The graph is a stand-in: its elements' box is (0,0)-(200,100), the view panned to (100,50) at zoom 2.
// A hull centred at (-20,50) in the drawing with radii (40,30) is drawn on the canvas at (60,150) with
// radii (80,60); it reaches (-60,20)-(20,80) in the drawing, past the elements' left edge, so the image
// spans x -60..200 and y 0..100, with the 10 px margin round it: 280 x 120, the graph at (70,10).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CyInstance } from "../../graph/cytoscape-types";

const saved = vi.hoisted(() => ({ files: [] as { name: string; blob: Blob }[] }));
vi.mock("../../graph/graph-export", async (actual) => ({
  ...(await actual<typeof import("../../graph/graph-export")>()),
  downloadBlob: (blob: Blob, name: string) => saved.files.push({ name, blob }),
}));

import { exportJpeg, exportPng, exportSvg } from "../exportImage";

const SVG_NS = "http://www.w3.org/2000/svg";
const BG = "#f8f9fb";

const cy = {
  pan: () => ({ x: 100, y: 50 }),
  zoom: () => 2,
  elements: () => ({ boundingBox: () => ({ x1: 0, y1: 0, x2: 200, y2: 100, w: 200, h: 100 }) }),
  svg: vi.fn(() => `<svg xmlns="${SVG_NS}" width="200" height="100"><path d="M0 0"/></svg>`),
  png: vi.fn(() => new Blob(["png"])),
  jpg: vi.fn(() => new Blob(["jpg"])),
} as unknown as CyInstance;

function hullOverlay(): SVGSVGElement {
  const svg = document.createElementNS(SVG_NS, "svg") as SVGSVGElement;
  svg.setAttribute("class", "gf-hulls");
  const g = document.createElementNS(SVG_NS, "g");
  const e = document.createElementNS(SVG_NS, "ellipse");
  for (const [k, v] of Object.entries({ cx: 60, cy: 150, rx: 80, ry: 60, fill: "#4c6ef5" }))
    e.setAttribute(k, String(v));
  g.appendChild(e);
  svg.appendChild(g);
  return svg;
}

const text = (b: Blob) => b.text();

describe("an SVG download", () => {
  beforeEach(() => {
    saved.files = [];
  });

  it("is the whole drawing on the canvas colour with a margin, named after the view", async () => {
    exportSvg({ cy, bg: BG, name: "ontology", hulls: null });
    expect(cy.svg).toHaveBeenLastCalledWith({ full: true, bg: BG });
    expect(saved.files.map((f) => f.name)).toEqual(["ontology.svg"]);
    const svg = await text(saved.files[0].blob);
    expect(svg).toContain('width="220" height="120"');
    expect(svg).toContain(`<rect width="220" height="120" fill="${BG}"/>`);
    expect(svg).toContain('<g transform="translate(10,10)"><path d="M0 0"/></g>');
    expect(svg).not.toContain("gf-hulls");
  });

  it("paints the hulls where they sit on the drawing, the image grown to hold them", async () => {
    exportSvg({ cy, bg: BG, name: "cypher-graph", hulls: hullOverlay() });
    expect(saved.files.map((f) => f.name)).toEqual(["cypher-graph.svg"]);
    const svg = await text(saved.files[0].blob);
    expect(svg).toContain('width="280" height="120"');
    expect(svg).toContain(`<rect width="280" height="120" fill="${BG}"/>`);
    expect(svg).toContain('<g transform="translate(70,10)"><path d="M0 0"/></g>');
    expect(svg).toMatch(
      /<g class="gf-hulls" transform="translate\(70,10\) scale\(0.5\) translate\(-100,-50\)"><g[^>]*><ellipse[^>]*cx="60"[^>]*\/><\/g><\/g><\/svg>$/,
    );
  });

  it("an overlay with no hull drawn paints nothing more", async () => {
    exportSvg({
      cy,
      bg: BG,
      name: "sparql-graph",
      hulls: document.createElementNS(SVG_NS, "svg") as SVGSVGElement,
    });
    const svg = await text(saved.files[0].blob);
    expect(svg).toContain('width="220" height="120"');
    expect(svg).not.toContain("gf-hulls");
  });
});

describe("a PNG or JPEG download", () => {
  const drawn: { fill: string[]; images: [string, number, number][]; size: number[] } = {
    fill: [],
    images: [],
    size: [],
  };
  const types: (string | undefined)[] = [];

  beforeEach(() => {
    saved.files = [];
    drawn.fill = [];
    drawn.images = [];
    drawn.size = [];
    types.length = 0;
    let n = 0;
    const urls = new Map<string, Blob>();
    vi.spyOn(URL, "createObjectURL").mockImplementation((b) => {
      const url = `blob:${n++}`;
      urls.set(url, b as Blob);
      return url;
    });
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    // An image loads at once, its size its source's: the graph's raster is 200 x 100.
    vi.stubGlobal(
      "Image",
      class {
        onload: (() => void) | null = null;
        width = 200;
        height = 100;
        src = "";
        constructor() {
          return new Proxy(this, {
            set: (t, k, v) => {
              (t as Record<string | symbol, unknown>)[k] = v;
              if (k === "src") queueMicrotask(() => t.onload?.());
              return true;
            },
          });
        }
      },
    );
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(function (
      this: HTMLCanvasElement,
    ) {
      drawn.size = [this.width, this.height];
      const ctx = {
        set fillStyle(v: string) {
          drawn.fill.push(v);
        },
        fillRect: () => {},
        drawImage: (img: { src: string }, x: number, y: number) =>
          drawn.images.push([img.src, x, y]),
      };
      return ctx as unknown as CanvasRenderingContext2D;
    } as unknown as typeof HTMLCanvasElement.prototype.getContext);
    vi.spyOn(HTMLCanvasElement.prototype, "toBlob").mockImplementation(function (cb, type) {
      types.push(type);
      cb(new Blob(["out"]));
    });
    (globalThis as Record<string, unknown>).__urls = urls;
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("a PNG is the drawing on the canvas colour with a margin, named after the view", async () => {
    exportPng({ cy, bg: BG, name: "erd", hulls: null });
    await vi.waitFor(() => expect(saved.files.map((f) => f.name)).toEqual(["erd.png"]));
    expect(cy.png).toHaveBeenLastCalledWith({ output: "blob", full: true, bg: BG });
    expect(drawn.size).toEqual([220, 120]);
    expect(drawn.fill).toEqual([BG]);
    expect(drawn.images).toEqual([["blob:0", 10, 10]]);
    expect(types).toEqual(["image/png"]);
  });

  it("a JPEG is on the canvas colour too, never white", async () => {
    exportJpeg({ cy, bg: BG, name: "lineage", hulls: null });
    await vi.waitFor(() => expect(saved.files.map((f) => f.name)).toEqual(["lineage.jpg"]));
    expect(cy.jpg).toHaveBeenLastCalledWith({ output: "blob", full: true, bg: BG, quality: 0.92 });
    expect(drawn.fill).toEqual([BG]);
    expect(types).toEqual(["image/jpeg"]);
  });

  it("paints the hulls over the drawing where they sit on it", async () => {
    exportPng({ cy, bg: BG, name: "cypher-graph", hulls: hullOverlay() });
    await vi.waitFor(() => expect(saved.files.map((f) => f.name)).toEqual(["cypher-graph.png"]));
    expect(drawn.size).toEqual([280, 120]);
    expect(drawn.images).toEqual([
      ["blob:0", 70, 10],
      ["blob:1", 0, 0],
    ]);
    const urls = (globalThis as Record<string, unknown>).__urls as Map<string, Blob>;
    const hulls = await text(urls.get("blob:1")!);
    expect(hulls).toContain('width="280" height="120"');
    expect(hulls).toContain(
      'class="gf-hulls" transform="translate(70,10) scale(0.5) translate(-100,-50)"',
    );
    expect(hulls).toContain("<ellipse");
  });
});
