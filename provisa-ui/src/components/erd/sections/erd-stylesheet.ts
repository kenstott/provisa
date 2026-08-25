// Copyright (c) 2026 Kenneth Stott
// Canary: 6513c4bf-9764-4f64-900b-d714f9db8b0b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { labelColor, darkenColor } from "../../graph/graph-model";
import { getErdPalette } from "./erd-palette";

// ── stylesheet ────────────────────────────────────────────────────────────────
export function buildErdStylesheet(isDark: boolean) {
  const p = getErdPalette(isDark);
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
        color: p.text,
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
        "background-color": p.tableBg,
        "border-color": (ele: { data(k: string): unknown }) =>
          darkenColor(labelColor(ele.data("domainId") as string), 1.2),
        "border-width": 1,
        label: (ele: { data(k: string): unknown }) => ele.data("displayLabel") as string,
        "text-valign": "center",
        "text-halign": "center",
        "text-justification": "left",
        color: p.text,
        "font-size": 10,
        "text-wrap": "wrap",
        width: 170,
        height: (ele: { data(k: string): unknown }) =>
          Math.max(24, ((ele.data("lineCount") as number) ?? 1) * 13 + 6),
      },
    },
    {
      selector: ".erd-table:selected",
      style: { "border-color": p.accent, "border-width": 2 },
    },
    {
      selector: ".erd-rel",
      style: {
        "curve-style": "bezier",
        "line-color": p.relLine,
        width: 1.5,
        "target-arrow-color": p.relLine,
        "target-arrow-shape": "triangle",
        "source-arrow-color": p.relLine,
        "source-arrow-shape": (ele: { data(k: string): unknown }) =>
          (ele.data("cardinality") as string) === "many_to_many" ||
          (ele.data("cardinality") as string) === "many_to_one"
            ? "triangle"
            : "none",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "font-size": 9,
        color: p.textMuted,
        "text-rotation": "none",
        "text-margin-y": (ele: { data(k: string): unknown }) => {
          const label = (ele.data("label") as string) ?? "";
          const hash = label.split("").reduce((s: number, c: string) => s + c.charCodeAt(0), 0);
          return ((hash % 3) - 1) * 14;
        },
        "text-background-color": p.tableBg,
        "text-background-opacity": 1,
        "text-background-padding": "3px",
      },
    },
    {
      // REQ-1588: a junction table is a waypoint on an edge, not an entity, so it is drawn as a
      // grape diamond carrying its name only — the same grape as the VIA badge on the
      // relationships table, so the two surfaces name the same thing the same way.
      selector: ".erd-junction",
      style: {
        shape: "diamond",
        "background-color": p.junctionBg,
        "border-color": p.junction,
        "border-width": 2,
        color: p.junction,
        "font-weight": "bold",
        width: 130,
        height: 90,
      },
    },
    {
      selector: ".erd-junction:selected",
      style: { "border-color": p.accent, "border-width": 3 },
    },
    {
      // The legs of a junction-backed path. The type is written once per path, at the junction end
      // of the inbound leg, so the two legs are readable as one relationship.
      selector: ".erd-rel--via",
      style: {
        "line-color": p.junction,
        "target-arrow-color": p.junction,
        "source-arrow-color": p.junction,
        "target-label": (ele: { data(k: string): unknown }) => ele.data("pathLabel") as string,
        "target-text-offset": 46,
        "target-text-margin-y": -10,
        "font-size": 9,
        "text-background-color": p.tableBg,
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
        "line-color": p.relLineFaint,
        "target-arrow-color": p.relLineFaint,
        "source-arrow-color": p.relLineFaint,
        color: p.textFaint,
      },
    },
  ];
}
