// Copyright (c) 2026 Kenneth Stott
// Canary: c84a1e3f-6d29-4b75-9a0e-3f5b8d2c7e91
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the one stylesheet every 2D graph is drawn with (SPEC.md). Selectors read only what
// graphElements writes: the kind of element, its type, and the marks the component sets.

import { NODE_TYPES, SIZE, typeIcon, type GraphTokens, type NodeType } from "./tokens";
import { TEXT_LEFT, TEXT_RIGHT } from "./model";

type Ele = { data(k: string): unknown };

/** The classes the component marks elements with. */
export const MARK = {
  /** Picked (selected). */
  picked: "picked",
  /** Lit: connected to what is hovered or picked. */
  lit: "lit",
  /** Faded: not connected to what is hovered or picked. */
  faded: "faded",
  /** Named by the view's focus. */
  focused: "focused",
  /** Matched by the view's filter. */
  match: "match",
  /** Not matched by the view's filter. */
  dim: "dim",
} as const;

const FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
const MONO = "ui-monospace, SFMono-Regular, Menlo, monospace";
/** Where the band ends across a box ``w`` wide. */
const band = (e: Ele) => `${((SIZE.band / (e.data("w") as number)) * 100).toFixed(2)}%`;
/** The label's room: the node's own (model.ts sizes the box for its label). */
const textRoom = (e: Ele) => `${e.data("textW") as number}px`;
/** The label's room is centred between the icon's right and the box's right edge. */
const TEXT_SHIFT = (TEXT_LEFT - TEXT_RIGHT) / 2;

export function graphStylesheet(t: GraphTokens) {
  const types = Object.keys(NODE_TYPES) as NodeType[];
  return [
    {
      selector: 'node[kind = "node"], node[kind = "more"]',
      style: {
        shape: "round-rectangle",
        "corner-radius": SIZE.radius,
        width: "data(w)",
        height: "data(h)",
        "background-color": t.nodeFill,
        "border-width": 1,
        "border-color": t.nodeBorder,
        "underlay-color": t.shadow,
        "underlay-opacity": t.shadowOpacity,
        "underlay-padding": 2,
        "underlay-shape": "round-rectangle",
        label: "data(label)",
        "font-family": FONT,
        "font-size": SIZE.name,
        "font-weight": 500,
        color: t.text,
        // The label runs from past the icon to short of the right edge, and wraps rather than
        // run past either: a name with no space in it wraps anywhere.
        "text-wrap": "wrap",
        "text-overflow-wrap": "anywhere",
        "text-max-width": textRoom,
        "text-valign": "center",
        "text-halign": "center",
        "text-justification": "left",
        "text-margin-x": TEXT_SHIFT,
        "line-height": 1.25,
        "background-width": SIZE.icon,
        "background-height": SIZE.icon,
        "background-position-x": SIZE.band + 8,
        "background-position-y": "50%",
        "background-clip": "none",
        "background-fit": "none",
        cursor: "pointer",
      },
    },
    // The band on the leading edge and the icon, by type.
    ...types.map((type) => ({
      selector: `node[kind = "node"][type = "${type}"]`,
      style: {
        "background-fill": "linear-gradient",
        "background-gradient-direction": "to-right",
        "background-gradient-stop-colors": `${NODE_TYPES[type].colour} ${NODE_TYPES[type].colour} ${t.nodeFill} ${t.nodeFill}`,
        "background-gradient-stop-positions": (e: Ele) => `0% ${band(e)} ${band(e)} 100%`,
        "background-image": typeIcon(type),
      },
    })),
    {
      // The one exception to the single shape (REQ-2239, A JUNCTION IS A DIAMOND): a junction is
      // Chen's relationship diamond. It keeps its band's colour (the border; the fill a tint of it),
      // its icon above its name, and its legs (edge[tone = "via"]).
      selector: 'node[kind = "node"][shape = "diamond"]',
      style: {
        shape: "diamond",
        "background-fill": "solid",
        "background-color": t.junctionFill,
        "border-width": 1.5,
        "border-color": (e: Ele) => NODE_TYPES[e.data("type") as NodeType].colour,
        // Cytoscape draws no diamond underlay; the diamond goes without the box's under-glow.
        "underlay-opacity": 0,
        "text-max-width": `${SIZE.diamondWidth - 40}px`,
        "text-justification": "center",
        "text-margin-x": 0,
        "text-margin-y": 7,
        "background-position-x": "50%",
        "background-position-y": 18,
      },
    },
    {
      // A node with a body: its lines in monospace, its icon beside its name.
      selector: 'node[body = "yes"]',
      style: {
        "font-family": MONO,
        "font-size": 10,
        "background-position-y": 10,
      },
    },
    {
      // A fold node: the same box, dashed, with no band.
      selector: 'node[kind = "more"]',
      style: {
        "border-style": "dashed",
        color: t.textMuted,
        "text-margin-x": 0,
        "text-justification": "center",
      },
    },
    { selector: 'node[status = "draft"]', style: { "font-style": "italic" } },
    { selector: 'node[status = "deprecated"]', style: { opacity: 0.55 } },
    { selector: 'node[status = "retired"]', style: { opacity: 0.35 } },
    {
      // A group: a dashed, lightly tinted box, its name uppercase on the top border.
      selector: 'node[kind = "group"], node[kind = "lane"]',
      style: {
        shape: "round-rectangle",
        "corner-radius": SIZE.radius * 2,
        "background-color": (e: Ele) => e.data("hue") as string,
        "background-opacity": 0.06,
        "border-color": (e: Ele) => e.data("hue") as string,
        "border-opacity": 0.55,
        "border-width": 1.5,
        "border-style": "dashed",
        label: "data(title)",
        "font-family": FONT,
        "font-size": SIZE.groupLabel,
        "font-weight": 700,
        color: (e: Ele) => e.data("hue") as string,
        "text-valign": "top",
        "text-halign": "center",
        "text-margin-y": 7,
        "text-background-color": t.canvas,
        "text-background-opacity": 1,
        "text-background-padding": "3px",
        padding: "28px",
        "compound-sizing-wrt-labels": "include",
        "min-width": 120,
        "min-height": 80,
      },
    },
    {
      // A lane's box is drawn behind what is in it, and takes no pointer.
      selector: 'node[kind = "lane"]',
      style: {
        width: "data(w)",
        height: "data(h)",
        "z-index": 0,
        "z-compound-depth": "bottom",
        events: "no",
      },
    },
    {
      // A compound the layout needs but the reader does not see (the stacked lane).
      selector: 'node[kind = "frame"]',
      style: { "background-opacity": 0, "border-width": 0, padding: "8px", events: "no" },
    },
    {
      selector: "edge",
      style: {
        width: SIZE.edge,
        "curve-style": "bezier",
        "line-color": t.edge,
        "target-arrow-color": t.edge,
        "source-arrow-color": t.edge,
        "target-arrow-shape": "none",
        "source-arrow-shape": "none",
        "arrow-scale": 0.8,
        label: "data(label)",
        "font-family": FONT,
        "font-size": SIZE.edgeLabel,
        color: t.textMuted,
        "text-rotation": "none",
        "text-background-color": t.canvas,
        "text-background-opacity": 1,
        "text-background-padding": "2px",
        // Labels of lines that pass near each other are spread by a hash of the label, and the
        // lines that share a pair of nodes step apart, so no label lands on another.
        "text-margin-y": (e: Ele) => {
          const label = (e.data("label") as string) ?? "";
          const hash = label.split("").reduce((s, c) => s + c.charCodeAt(0), 0);
          return ((hash % 3) - 1) * 14 + ((e.data("pathIndex") as number) ?? 0) * 13;
        },
      },
    },
    { selector: 'edge[arrow = "target"], edge[arrow = "both"]', style: { "target-arrow-shape": "triangle" } },
    { selector: 'edge[arrow = "source"], edge[arrow = "both"]', style: { "source-arrow-shape": "triangle" } },
    { selector: "edge[?dashed]", style: { "line-style": "dashed", "line-dash-pattern": [5, 3] } },
    {
      selector: 'edge[tone = "via"]',
      style: { "line-color": t.via, "target-arrow-color": t.via, "source-arrow-color": t.via },
    },
    {
      // A line laid over the placed nodes: an arc beside them.
      selector: "edge[?overlay]",
      style: {
        "curve-style": "unbundled-bezier",
        "control-point-distances": 36,
        "control-point-weights": 0.5,
      },
    },
    // The one hover and selection state.
    {
      selector: `node.${MARK.picked}, node.${MARK.focused}`,
      style: {
        "border-color": t.accent,
        "border-width": 2,
        "underlay-color": t.accent,
        "underlay-opacity": 0.18,
        "underlay-padding": 5,
      },
    },
    {
      // The diamond's selection: its border, without the box-shaped halo.
      selector: `node[shape = "diamond"].${MARK.picked}, node[shape = "diamond"].${MARK.focused}`,
      style: { "underlay-opacity": 0, "border-width": 2.5 },
    },
    {
      selector: `edge.${MARK.lit}, edge.${MARK.focused}, edge.${MARK.picked}`,
      style: {
        "line-color": t.accent,
        "target-arrow-color": t.accent,
        "source-arrow-color": t.accent,
        width: 2,
      },
    },
    { selector: `.${MARK.faded}`, style: { opacity: 0.2 } },
    { selector: `.${MARK.dim}`, style: { opacity: 0.15 } },
    {
      selector: `node.${MARK.match}`,
      style: { "border-color": t.accent, "border-width": 2 },
    },
    {
      selector: `edge.${MARK.match}`,
      style: { "line-color": t.accent, "target-arrow-color": t.accent, width: 2 },
    },
  ];
}
