// Copyright (c) 2026 Kenneth Stott
// Canary: 01ce5d3a-a9a5-41cd-99f2-132fae3bc668
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { labelColor, darkenColor } from "./graph-model";
import type { GNode, RelLineOverride } from "./graph-model";
import type { CyElement } from "./cytoscape-types";

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
        label: (ele: CyElement) => {
          const n = ele.data("_node") as GNode | undefined;
          if (!n) return String(ele.data("label") ?? "");
          const prop = labelPropertyRef.current[n.label];
          if (prop) return String(n.properties[prop] ?? n.id);
          return String(n.properties["name"] ?? n.properties["title"] ?? n.id);
        },
        color: "#fff",
        "font-size": 10,
        "text-valign": "center",
        "text-halign": "center",
        width: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return sizeOverridesRef.current[lbl] ?? 44;
        },
        height: (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return sizeOverridesRef.current[lbl] ?? 44;
        },
        "text-wrap": "ellipsis",
        "text-max-width": (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          const sz = sizeOverridesRef.current[lbl] ?? 44;
          return `${sz - 8}px`;
        },
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
        "font-size": 7,
        "text-max-width": (ele: CyElement) => {
          const lbl = ele.data("label") as string;
          return `${(sizeOverridesRef.current[lbl] ?? 44) / 2 - 4}px`;
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
        "text-valign": "center" as const,
        "text-halign": "center" as const,
      },
    },
  ];
}
