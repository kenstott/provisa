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
import type { CyElement } from "./cytoscape-types";
import { resolveNodeLabel } from "./canvas/canvas-helpers";

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

export function buildGraphStylesheet({
  colorOverridesRef,
  sizeOverridesRef,
  labelPropertyRef,
  relLineOverridesRef,
}: StylesheetRefs): unknown[] {
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
        color: (ele: CyElement) =>
          insideCaption(ele, labelPropertyRef.current) ? "#fff" : "#d6d8e2",
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
        "font-size": 8,
        color: "#6b6f82",
        "text-background-opacity": 0,
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
        "line-color": "#6366f1",
        "target-arrow-color": "#6366f1",
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        label: "data(label)",
        "font-size": 9,
        color: "#a5b4fc",
        "text-background-opacity": 0.7,
        "text-background-color": "#1a1d2e",
        "text-background-padding": "2px",
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
