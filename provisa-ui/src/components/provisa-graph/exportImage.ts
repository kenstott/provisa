// Copyright (c) 2026 Kenneth Stott
// Canary: d17a5c3e-8b42-4f69-a0d1-5c9e3b7f2a84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the one image export every 2D graph's command bar calls: the whole
// drawing as SVG, PNG or JPEG on the canvas colour, with a margin round it, named after the view.
// A graph that draws cluster hulls over its canvas (the network graph's HullSvgOverlay) passes that
// overlay, and the hulls are painted in the image where they sit on the drawing.

import { downloadBlob } from "../graph/graph-export";
import type { CyInstance } from "../graph/cytoscape-types";

const PAD = 10;
const SVG_NS = "http://www.w3.org/2000/svg";

/** What an image is drawn from: the graph, the canvas colour, the view's name and the hull overlay
 * drawn over the canvas (null where the view draws none). */
export interface GraphImage {
  cy: CyInstance;
  bg: string;
  name: string;
  hulls: SVGSVGElement | null;
}

interface Box {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

/** The hulls' extent in the drawing's own (model) coordinates. The overlay is drawn in the canvas's
 * pixels over the viewport (HullSvgOverlay draws each hull as an ellipse), so a point there is
 * (pixel - pan) / zoom in the drawing. Null when no hull is drawn. */
function hullBox(cy: CyInstance, hulls: SVGSVGElement | null): Box | null {
  const ellipses = hulls ? Array.from(hulls.querySelectorAll("ellipse")) : [];
  if (ellipses.length === 0) return null;
  const pan = cy.pan();
  const zoom = cy.zoom();
  const num = (e: Element, a: string) => Number(e.getAttribute(a));
  const boxes = ellipses.map((e) => ({
    x1: (num(e, "cx") - num(e, "rx") - pan.x) / zoom,
    y1: (num(e, "cy") - num(e, "ry") - pan.y) / zoom,
    x2: (num(e, "cx") + num(e, "rx") - pan.x) / zoom,
    y2: (num(e, "cy") + num(e, "ry") - pan.y) / zoom,
  }));
  return {
    x1: Math.min(...boxes.map((b) => b.x1)),
    y1: Math.min(...boxes.map((b) => b.y1)),
    x2: Math.max(...boxes.map((b) => b.x2)),
    y2: Math.max(...boxes.map((b) => b.y2)),
  };
}

/** Where things go in the image. A full export of the graph (cytoscape's ``full``) is its elements'
 * bounding box at scale 1: its pixel (0, 0) is the box's top left. The image covers that and the
 * hulls, with the margin round both. */
interface Frame {
  width: number;
  height: number;
  /** Where the graph's own image is drawn. */
  graphAt: { x: number; y: number };
  /** The transform taking the overlay's pixels to the image's. */
  hullTransform: string | null;
}

function frame(cy: CyInstance, hulls: SVGSVGElement | null, graphW: number, graphH: number): Frame {
  const bb = cy.elements().boundingBox();
  const graph: Box = { x1: bb.x1, y1: bb.y1, x2: bb.x1 + graphW, y2: bb.y1 + graphH };
  const h = hullBox(cy, hulls);
  const all: Box = h
    ? {
        x1: Math.min(graph.x1, h.x1),
        y1: Math.min(graph.y1, h.y1),
        x2: Math.max(graph.x2, h.x2),
        y2: Math.max(graph.y2, h.y2),
      }
    : graph;
  const pan = cy.pan();
  return {
    width: Math.ceil(all.x2 - all.x1) + PAD * 2,
    height: Math.ceil(all.y2 - all.y1) + PAD * 2,
    graphAt: { x: graph.x1 - all.x1 + PAD, y: graph.y1 - all.y1 + PAD },
    hullTransform: h
      ? `translate(${PAD - all.x1},${PAD - all.y1}) scale(${1 / cy.zoom()}) translate(${-pan.x},${-pan.y})`
      : null,
  };
}

/** The overlay's hulls as one SVG group placed by ``transform``. */
function hullGroup(hulls: SVGSVGElement, transform: string): string {
  const content = Array.from(hulls.children)
    .map((c) => new XMLSerializer().serializeToString(c))
    .join("");
  return `<g class="gf-hulls" transform="${transform}">${content}</g>`;
}

function loadImage(src: string, then: (img: HTMLImageElement) => void) {
  const img = new Image();
  img.onload = () => then(img);
  img.src = src;
}

/** ``blob``'s image on ``bg`` with a margin and the hulls, saved as ``filename``. */
function raster(g: GraphImage, blob: Blob, mimeType: string, quality: number, filename: string) {
  const url = URL.createObjectURL(blob);
  loadImage(url, (img) => {
    URL.revokeObjectURL(url);
    const f = frame(g.cy, g.hulls, img.width, img.height);
    const canvas = document.createElement("canvas");
    canvas.width = f.width;
    canvas.height = f.height;
    const ctx = canvas.getContext("2d")!;
    ctx.fillStyle = g.bg;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, f.graphAt.x, f.graphAt.y);
    const save = () => canvas.toBlob((b) => b && downloadBlob(b, filename), mimeType, quality);
    if (!g.hulls || !f.hullTransform) {
      save();
      return;
    }
    const svg =
      `<svg xmlns="${SVG_NS}" width="${f.width}" height="${f.height}">` +
      `${hullGroup(g.hulls, f.hullTransform)}</svg>`;
    const svgUrl = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml;charset=utf-8" }));
    loadImage(svgUrl, (hullImg) => {
      URL.revokeObjectURL(svgUrl);
      ctx.drawImage(hullImg, 0, 0);
      save();
    });
  });
}

export function exportSvg(g: GraphImage): void {
  const raw = g.cy.svg({ full: true, bg: g.bg }) as string;
  const size = raw.match(/<svg[^>]*\swidth="(\d+(?:\.\d+)?)"[^>]*\sheight="(\d+(?:\.\d+)?)"[^>]*>/);
  if (!size) throw new Error("the graph's SVG has no width and height");
  const f = frame(g.cy, g.hulls, parseFloat(size[1]), parseFloat(size[2]));
  const end = raw.lastIndexOf("</svg>");
  const opened = raw
    .slice(0, end)
    .replace(
      size[0],
      size[0]
        .replace(/\swidth="[^"]*"/, ` width="${f.width}"`)
        .replace(/\sheight="[^"]*"/, ` height="${f.height}"`) +
        `<rect width="${f.width}" height="${f.height}" fill="${g.bg}"/>` +
        `<g transform="translate(${f.graphAt.x},${f.graphAt.y})">`,
    );
  const hulls = g.hulls && f.hullTransform ? hullGroup(g.hulls, f.hullTransform) : "";
  downloadBlob(
    new Blob([`${opened}</g>${hulls}</svg>`], { type: "image/svg+xml" }),
    `${g.name}.svg`,
  );
}

export function exportPng(g: GraphImage): void {
  raster(
    g,
    g.cy.png({ output: "blob", full: true, bg: g.bg }) as unknown as Blob,
    "image/png",
    1,
    `${g.name}.png`,
  );
}

export function exportJpeg(g: GraphImage): void {
  raster(
    g,
    g.cy.jpg({ output: "blob", full: true, bg: g.bg, quality: 0.92 }) as unknown as Blob,
    "image/jpeg",
    0.92,
    `${g.name}.jpg`,
  );
}

export function exportJson(body: unknown, name: string): void {
  downloadBlob(
    new Blob([JSON.stringify(body, null, 2)], { type: "application/json" }),
    `${name}.json`,
  );
}
