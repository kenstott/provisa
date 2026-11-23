// Copyright (c) 2026 Kenneth Stott
// Canary: 01ce5d3a-a9a5-41cd-99f2-132fae3bc668
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { labelColor, darkenColor, propertyValue } from "./graph-model";
import type { GNode, RelLineOverride } from "./graph-model";
import type { CyElement, CyInstance } from "./cytoscape-types";
import { resolveNodeLabel } from "./canvas/canvas-helpers";
import type { GraphTokens } from "../provisa-graph/tokens";
import { META_LABEL_PAD, META_LABEL_SIZE } from "./canvas/edge-length";
import { textWidth } from "../provisa-graph/model";

export const PIN_SVG =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(
    `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'>` +
      `<circle cx='8' cy='6' r='5' fill='%23fbbf24' stroke='%2392400e' stroke-width='1.5'/>` +
      `<circle cx='8' cy='6' r='2' fill='%2392400e'/>` +
      `<line x1='8' y1='11' x2='8' y2='16' stroke='%2392400e' stroke-width='1.5' stroke-linecap='round'/>` +
      `</svg>`,
  );

interface StylesheetRefs {
  colorOverridesRef: { current: Record<string, string> };
  sizeOverridesRef: { current: Record<string, number> };
  labelPropertyRef: { current: Record<string, string> };
  relLineOverridesRef: { current: Record<string, RelLineOverride> };
}

/** The text a node is captioned with: its label property, else its name, else its id. */
export function nodeCaption(
  n: GNode | undefined,
  label: unknown,
  labelProperty: Record<string, string>,
): string {
  if (!n) return String(label ?? "");
  const prop = labelProperty[n.label];
  if (!prop) return resolveNodeLabel(n);
  const value = propertyValue(n, prop);
  if (value !== null && value !== undefined) return String(value);
  // A SPARQL node's id is a hash of its IRI: it keeps its name until its value is read, if it has one.
  return n.propertyIris ? resolveNodeLabel(n) : String(n.id);
}

/** Average glyph width as a share of the font size, for the labels' sans-serif face. */
const GLYPH_EM = 0.58;
/** How far past its node's width a caption beneath it may run, in pixels. */
const OUTSIDE_MARGIN = 24;
const LABEL_FONT = 10;
/** A relationship type's font size, in graph units like every label (it scales with the zoom). */
export const EDGE_LABEL_FONT = 8;
const CLUSTER_FONT = 7;

/** Whether ``caption`` fits inside a node ``width`` pixels wide at ``font`` size. */
export function captionFits(caption: string, width: number, font = LABEL_FONT): boolean {
  return caption.length * GLYPH_EM * font <= width - 8;
}

/** ``caption`` broken into at most two lines of ``available`` pixels, the second ending in an
 * ellipsis where the caption runs on; breaks fall at spaces where there is one. */
export function captionLines(caption: string, available: number, font = LABEL_FONT): string {
  const per = Math.max(1, Math.floor(available / (GLYPH_EM * font)));
  if (caption.length <= per) return caption;
  const cut = caption.lastIndexOf(" ", per);
  const first = cut > 0 ? caption.slice(0, cut) : caption.slice(0, per);
  const rest = caption.slice(first.length).trimStart();
  const second = rest.length <= per ? rest : `${rest.slice(0, Math.max(1, per - 1)).trimEnd()}…`;
  return `${first}\n${second}`;
}

/** The caption's font: the canvas's normal one, smaller only in a collapsed cluster. */
const fontOf = (ele: CyElement): number => (ele.data("_inCluster") ? CLUSTER_FONT : LABEL_FONT);

/** Whether a node's caption fits inside its circle. */
function insideCaption(ele: CyElement, labelProperty: Record<string, string>): boolean {
  const cap = nodeCaption(ele.data("_node") as GNode | undefined, ele.data("label"), labelProperty);
  return captionFits(cap, ele.width(), fontOf(ele));
}

/** Restyle every node from the stylesheet: its caption, and where and how it is set (inside the
 * circle or beneath it), read the label property and the node's width. Cytoscape caches a mapped
 * style until the element is marked dirty, and removing a bypass a node never had marks nothing,
 * so a caption-field or size change restyles every node here, the same way. */
export function restyleCaptions(cy: CyInstance): void {
  cy.nodes().updateStyle();
}

/** Space left clear at each end of an edge's label, between it and its node's rim. */
const EDGE_LABEL_GAP = 4;

/** How far ``node`` reaches from its centre toward ``to``: to the edge of its box with its caption,
 * which hangs beneath a node it does not fit. */
function reachToward(node: CyElement, to: { x: number; y: number }): number {
  const c = node.position();
  const dx = to.x - c.x;
  const dy = to.y - c.y;
  const len = Math.hypot(dx, dy);
  if (len === 0) return 0;
  const b = node.boundingBox({ includeLabels: true, includeOverlays: false });
  const ux = dx / len;
  const uy = dy / len;
  const tx = ux === 0 ? Infinity : (ux > 0 ? b.x2 - c.x : c.x - b.x1) / Math.abs(ux);
  const ty = uy === 0 ? Infinity : (uy > 0 ? b.y2 - c.y : c.y - b.y1) / Math.abs(uy);
  return Math.min(tx, ty);
}

/** The room an edge's label has: its line between its two nodes and their captions, less a gap at
 * each end. A loop's label sits beyond its node, clear of it, and keeps its own width. */
export function edgeLabelRoom(ele: CyElement): number {
  const s = ele.source();
  const t = ele.target();
  if (s.id() === t.id()) return Math.ceil(textWidth(String(ele.data("label")), EDGE_LABEL_FONT));
  const a = s.position();
  const b = t.position();
  const length = Math.hypot(b.x - a.x, b.y - a.y) - reachToward(s, b) - reachToward(t, a);
  return Math.max(0, Math.floor(length - 2 * EDGE_LABEL_GAP));
}

/** Keep each edge label's room (edgeLabelRoom) current as nodes move: a layout, a drag, a restored
 * drawing. The moves of one frame restyle the edges once, at the next frame. */
export function trackEdgeLabelRoom(cy: CyInstance): void {
  let pending = false;
  const restyle = () => {
    pending = false;
    if (!cy.destroyed()) cy.edges().updateStyle();
  };
  cy.on("position", "node", () => {
    if (pending) return;
    pending = true;
    requestAnimationFrame(restyle);
  });
}

export function buildGraphStylesheet(
  { colorOverridesRef, sizeOverridesRef, labelPropertyRef, relLineOverridesRef }: StylesheetRefs,
  t: GraphTokens,
): unknown[] {
  return [
    {
      selector: "node",
      style: {
        "background-color": (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return colorOverridesRef.current[lbl] ?? labelColor(lbl);
        },
        // A caption that fits its circle stays inside; a longer one goes beneath the node, two lines
        // at most at its width and a margin, the full caption on hover (GraphCanvas).
        label: (ele: CyElement) => {
          const cap = nodeCaption(
            ele.data("_node") as GNode | undefined,
            ele.data("label"),
            labelPropertyRef.current,
          );
          const font = fontOf(ele);
          return captionFits(cap, ele.width(), font)
            ? cap
            : captionLines(cap, ele.width() + OUTSIDE_MARGIN, font);
        },
        color: (ele: CyElement) => (insideCaption(ele, labelPropertyRef.current) ? "#fff" : t.text),
        "font-size": fontOf,
        "text-valign": (ele: CyElement) =>
          insideCaption(ele, labelPropertyRef.current) ? "center" : "bottom",
        "text-halign": "center",
        "text-margin-y": (ele: CyElement) => (insideCaption(ele, labelPropertyRef.current) ? 0 : 4),
        width: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return sizeOverridesRef.current[lbl] ?? 44;
        },
        height: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return sizeOverridesRef.current[lbl] ?? 44;
        },
        "text-wrap": "wrap",
        "text-max-width": (ele: CyElement) =>
          `${insideCaption(ele, labelPropertyRef.current) ? ele.width() - 8 : ele.width() + OUTSIDE_MARGIN}px`,
        "border-width": 2,
        "border-color": (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
          return darkenColor(base, 0.75);
        },
      },
    },
    {
      selector: "node:selected",
      style: {
        "border-width": 4,
        "border-color": "#fff",
      },
    },
    {
      selector: "node:active",
      style: {
        "overlay-opacity": 0,
      },
    },
    {
      selector: "node.hovered",
      style: {
        "border-width": 4,
        "border-color": "rgba(255,255,255,0.6)",
        "border-opacity": 1,
      },
    },
    {
      selector: "node.pinned",
      style: {
        "background-image": PIN_SVG,
        "background-width": "14px",
        "background-height": "14px",
        "background-position-x": "88%",
        "background-position-y": "8%",
        "background-fit": "none",
        "background-clip": "none",
        "background-image-opacity": 1,
      },
    },
    {
      selector: "edge",
      style: {
        "line-color": "#3a3d4e",
        "target-arrow-color": "#3a3d4e",
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        label: "data(label)",
        "font-size": EDGE_LABEL_FONT,
        color: t.textMuted,
        // A relationship's type is written along its line, centred on it and turned with it, on a
        // strip of the canvas so the line does not run through the text. Like every label it is
        // drawn at the graph's scale and follows the zoom with the nodes, as in the schema graph.
        "text-rotation": "autorotate",
        "text-margin-y": 0,
        // A type longer than the line between its nodes' rims ends in an ellipsis there, so it never
        // runs under a node or over its caption (the full type is in the inspector).
        "text-wrap": "ellipsis",
        "text-max-width": (ele: CyElement) => `${edgeLabelRoom(ele)}px`,
        "text-background-color": t.canvas,
        "text-background-opacity": 1,
        "text-background-padding": "1px",
        /* eslint-disable @typescript-eslint/no-explicit-any -- cytoscape style mappers receive untyped elements; the stylesheet function signature is not modeled by our local CyInstance shim */
        width: ((ele: any) =>
          relLineOverridesRef.current[ele.data("label") as string]?.width ?? 1.5) as any,
        "line-style": ((ele: any) =>
          relLineOverridesRef.current[ele.data("label") as string]?.style ?? "solid") as any,
        /* eslint-enable @typescript-eslint/no-explicit-any */
      },
    },
    {
      selector: "edge:selected",
      style: {
        "line-color": "#6366f1",
        "target-arrow-color": "#6366f1",
        width: 2.5,
      },
    },
    {
      selector: "edge.hovered",
      style: {
        "line-color": "#818cf8",
        "target-arrow-color": "#818cf8",
        width: 2.5,
      },
    },
    {
      selector: "edge[?_metaEdge]",
      style: {
        "line-color": t.accent,
        "target-arrow-color": t.accent,
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        label: "data(label)",
        "font-size": META_LABEL_SIZE,
        color: t.accent,
        "text-background-opacity": 0.7,
        "text-background-color": t.canvas,
        "text-background-padding": `${META_LABEL_PAD}px`,
        // A cluster edge's label is level and whole: the layout gives its line room for it
        // (edge-length.ts).
        "text-rotation": "none",
        "text-wrap": "none",
        /* eslint-disable @typescript-eslint/no-explicit-any -- cytoscape style mapper receives an untyped element; the stylesheet function signature is not modeled by our local CyInstance shim */
        width: ((ele: any) =>
          Math.min(0.75 + Math.log1p(ele.data("_metaCount") as number) * 0.5, 3)) as any,
        /* eslint-enable @typescript-eslint/no-explicit-any */
        "line-style": "dashed",
        "line-dash-pattern": [6, 3],
      },
    },
    {
      selector: "node[?_port]",
      style: {
        shape: "ellipse" as const,
        "background-opacity": 0,
        "border-width": 0,
        label: "",
        events: "no" as const,
      },
    },
    {
      selector: "node[?_cluster]",
      style: {
        "background-opacity": 0,
        "border-width": 0,
        label: "",
        padding: "36px",
        events: "no" as const,
      },
    },
    {
      selector: "node[?_inCluster]",
      style: {
        width: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return (sizeOverridesRef.current[lbl] ?? 44) / 2;
        },
        height: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return (sizeOverridesRef.current[lbl] ?? 44) / 2;
        },
      },
    },
    {
      selector: "node[?_collapsed]",
      style: {
        shape: "ellipse" as const,
        "background-color": "data(_color)",
        "background-opacity": 0.85,
        "border-width": 2,
        "border-color": "data(_color)",
        "border-opacity": 1,
        width: 64,
        height: 64,
        color: "#fff",
        "font-size": 9,
        "text-wrap": "wrap" as const,
        "text-max-width": "56px",
        "text-margin-y": 0,
        "text-valign": "center" as const,
        "text-halign": "center" as const,
      },
    },
    // An Overview pill's highlight: its label's nodes (or its type's edges) stand out, the rest dim.
    {
      selector: ".gc-dim",
      style: { opacity: 0.15 },
    },
    {
      selector: "node.gc-lit",
      style: { "border-width": 4, "border-color": "#fff", "border-opacity": 1 },
    },
    {
      selector: "edge.gc-lit",
      style: { width: 4 },
    },
  ];
}
