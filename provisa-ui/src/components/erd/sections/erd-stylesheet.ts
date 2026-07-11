// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { labelColor, darkenColor } from "../../graph/graph-model";

// ── stylesheet ────────────────────────────────────────────────────────────────
export function buildErdStylesheet() {
  return [
    {
      selector: "node",
      style: { "text-wrap": "wrap", "font-family": "monospace" },
    },
    {
      selector: ".erd-domain",
      style: {
        shape: "roundrectangle",
        "background-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string),
        "background-opacity": 0.13,
        "border-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string),
        "border-width": 2,
        "border-style": "solid",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "text-valign": "top",
        "text-halign": "center",
        color: "#e2e8f0",
        "font-size": 13,
        "font-weight": "bold",
        padding: "32px",
        "compound-sizing-wrt-labels": "include",
        "min-width": 120,
        "min-height": 80,
      },
    },
    {
      selector: ".erd-table",
      style: {
        shape: "rectangle",
        "background-color": "#1e293b",
        "border-color": (ele: { data(k: string): unknown }) =>
          darkenColor(labelColor(ele.data("domainId") as string), 1.2),
        "border-width": 1,
        label: (ele: { data(k: string): unknown }) => ele.data("displayLabel") as string,
        "text-valign": "center",
        "text-halign": "center",
        "text-justification": "left",
        color: "#e2e8f0",
        "font-size": 10,
        "text-wrap": "wrap",
        width: 170,
        height: (ele: { data(k: string): unknown }) =>
          Math.max(24, ((ele.data("lineCount") as number) ?? 1) * 13 + 6),
      },
    },
    {
      selector: ".erd-table:selected",
      style: { "border-color": "#60a5fa", "border-width": 2 },
    },
    {
      selector: ".erd-rel",
      style: {
        "curve-style": "bezier",
        "line-color": "#475569",
        width: 1.5,
        "target-arrow-color": "#475569",
        "target-arrow-shape": "triangle",
        "source-arrow-color": "#475569",
        "source-arrow-shape": (ele: { data(k: string): unknown }) =>
          (ele.data("cardinality") as string) === "many_to_many" ||
          (ele.data("cardinality") as string) === "many_to_one"
            ? "triangle"
            : "none",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "font-size": 9,
        color: "#94a3b8",
        "text-rotation": "none",
        "text-margin-y": (ele: { data(k: string): unknown }) => {
          const label = (ele.data("label") as string) ?? "";
          const hash = label.split("").reduce((s: number, c: string) => s + c.charCodeAt(0), 0);
          return (hash % 3 - 1) * 14;
        },
        "text-background-color": "#1e293b",
        "text-background-opacity": 1,
        "text-background-padding": "3px",
      },
    },
    {
      // proxy edges (collapsed-domain → table/domain) rendered dashed
      selector: ".erd-rel--proxy",
      style: {
        "line-style": "dashed",
        "line-dash-pattern": [6, 3],
        "line-color": "#334155",
        "target-arrow-color": "#334155",
        "source-arrow-color": "#334155",
        color: "#475569",
      },
    },
  ];
}
