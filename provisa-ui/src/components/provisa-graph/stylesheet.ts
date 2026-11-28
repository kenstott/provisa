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

import { NODE_TYPES, SIZE, type GraphTokens, type NodeType } from "./tokens";
import { GROUP_PAD, TEXT_LEFT } from "./model";

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

const escapeXml = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/'/g, "&apos;");

/** A box's text, drawn as one image over its fill (REQ-2239, amended 2026-11-28, THE SHARED NODE
 * STYLE IS COMPACT): its name bold on the first row, its body's rows under it in a small
 * monospace, tight, all from TEXT_LEFT. Cytoscape draws one font per label, so the two sizes and
 * weights of a box are drawn as an image; model.ts has already cut each row to the box's room. */
export function cardImage(e: Ele, t: GraphTokens): string {
  const w = e.data("w") as number;
  const h = e.data("h") as number;
  const rows = (e.data("rows") as string[] | undefined) ?? [];
  const italic = e.data("status") === "draft" ? " font-style='italic'" : "";
  const name =
    `<text x='${TEXT_LEFT}' y='${SIZE.padY + SIZE.nameStep / 2}' dominant-baseline='central'` +
    ` font-family='${FONT}' font-size='${SIZE.name}' font-weight='600' fill='${t.text}'${italic}>` +
    `${escapeXml(e.data("title") as string)}</text>`;
  const top = SIZE.padY + SIZE.nameStep + SIZE.bodyGap;
  const body = rows
    .map(
      (r, i) =>
        `<text x='${TEXT_LEFT}' y='${top + i * SIZE.lineStep + SIZE.lineStep / 2}'` +
        ` dominant-baseline='central' font-family='${MONO}' font-size='${SIZE.lineFont}'` +
        ` fill='${t.textMuted}' xml:space='preserve'>${escapeXml(r)}</text>`,
    )
    .join("");
  const svg =
    `<svg xmlns='http://www.w3.org/2000/svg' width='${w}' height='${h}'` +
    ` viewBox='0 0 ${w} ${h}'>${name}${body}</svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

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
        // A diamond's or a fold node's name, centred (a box's text is its card); model.ts has
        // already cut it to the room, so it never wraps.
        label: "data(caption)",
        "font-family": FONT,
        "font-size": SIZE.name,
        "font-weight": 600,
        color: t.text,
        "text-wrap": "none",
        "text-valign": "center",
        "text-halign": "center",
        cursor: "pointer",
      },
    },
    {
      // A box: its text is its card, drawn over the whole box.
      selector: 'node[kind = "node"][shape = "box"]',
      style: {
        "background-image": (e: Ele) => cardImage(e, t),
        "background-width": "100%",
        "background-height": "100%",
        "background-position-x": 0,
        "background-position-y": 0,
        "background-fit": "none",
        "background-clip": "node",
      },
    },
    // The thin band on the leading edge, by type.
    ...types.map((type) => ({
      selector: `node[kind = "node"][type = "${type}"]`,
      style: {
        "background-fill": "linear-gradient",
        "background-gradient-direction": "to-right",
        "background-gradient-stop-colors": `${NODE_TYPES[type].colour} ${NODE_TYPES[type].colour} ${t.nodeFill} ${t.nodeFill}`,
        "background-gradient-stop-positions": (e: Ele) => `0% ${band(e)} ${band(e)} 100%`,
      },
    })),
    {
      // The one exception to the single shape (REQ-2239, A JUNCTION IS A DIAMOND): a junction is
      // Chen's relationship diamond. It keeps its band's colour (the border; the fill a tint of it),
      // its name, and its legs, drawn as every edge is (REQ-2239: ERD edges take the shared edge
      // colour).
      selector: 'node[kind = "node"][shape = "diamond"]',
      style: {
        shape: "diamond",
        "background-fill": "solid",
        "background-color": t.junctionFill,
        "border-width": 1.5,
        "border-color": (e: Ele) => NODE_TYPES[e.data("type") as NodeType].colour,
        // Cytoscape draws no diamond underlay; the diamond goes without the box's under-glow.
        "underlay-opacity": 0,
      },
    },
    {
      // A fold node: the same box, dashed, with no band, its name muted.
      selector: 'node[kind = "more"]',
      style: {
        "border-style": "dashed",
        color: t.textMuted,
        "font-weight": 400,
      },
    },
    // A withdrawn table (REQ-1921): the same box, its border dashed.
    { selector: 'node[withdrawn = "yes"]', style: { "border-style": "dashed" } },
    // A draft's name is italic (its card draws it so; a diamond's label here).
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
        // Inside the room ELK keeps round a group (flow.ts GROUP_OPTIONS, presets.ts), so a drawn box
        // never reaches past the place it was laid out in.
        padding: `${GROUP_PAD}px`,
        "compound-sizing-wrt-labels": "include",
        // The least size of what is inside the padding: a collapsed group, empty, is a one-row node's.
        "min-width": SIZE.nodeWidth,
        "min-height": SIZE.nodeHeight,
      },
    },
    {
      // REQ-2239 (amended 2026-11-28): a group's box is at least as wide as its title
      // (groupMinWidth), widened evenly round its nodes, as ELK keeps the room (groupSizeOptions).
      selector: 'node[kind = "group"]',
      style: { "min-width": "data(minInner)" },
    },
    {
      // A lane's box is drawn behind what is in it, and takes no pointer.
      selector: 'node[kind = "lane"]',
      style: {
        // Its size is the box round its own nodes, as placed (lanes.ts): no padding past it.
        width: "data(w)",
        height: "data(h)",
        padding: "0px",
        "z-index": 0,
        "z-compound-depth": "bottom",
        events: "no",
      },
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
    {
      selector: 'edge[arrow = "target"], edge[arrow = "both"]',
      style: { "target-arrow-shape": "triangle" },
    },
    {
      selector: 'edge[arrow = "source"], edge[arrow = "both"]',
      style: { "source-arrow-shape": "triangle" },
    },
    { selector: "edge[?dashed]", style: { "line-style": "dashed", "line-dash-pattern": [5, 3] } },
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
