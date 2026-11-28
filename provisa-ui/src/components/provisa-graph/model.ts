// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2c8e1a-93d7-4b60-a4f8-2e7b1c9d6a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: what a view hands the graph (nodes, edges, groups, each node's type) and the elements
// the graph draws from it. A view says what a thing is; this file alone says how it is drawn.

import { NODE_TYPES, SIZE, groupHue, typeShape, type NodeType } from "./tokens";

/** A line of a node's body (an ERD table's column); ``key`` lines stay at detail "key". */
export interface GraphLine {
  text: string;
  key?: boolean;
}

export interface GraphNodeSpec {
  id: string;
  type: NodeType;
  name: string;
  /** "more": a fold node standing for nodes not drawn; a click is the view's ``onMore``. */
  kind?: "node" | "more";
  /** The group it is drawn in. */
  parent?: string;
  /** Its lane, under the lanes preset. */
  lane?: string;
  lines?: GraphLine[];
  /** A term's status: how far it is drawn back. */
  status?: "draft" | "deprecated" | "retired";
  /** Out of production (REQ-1921): its border is dashed, as a pending line is. */
  withdrawn?: boolean;
  /** Hover text. */
  description?: string;
  /** The view's own identifiers, handed back on a click; never styled. */
  data?: Record<string, unknown>;
}

export interface GraphEdgeSpec {
  id: string;
  source: string;
  target: string;
  label?: string;
  /** Where its arrowheads are; only where direction means something. */
  arrow?: "none" | "target" | "source" | "both";
  /** Pending or indirect. */
  dashed?: boolean;
  /** Drawn over the placed nodes once laid out; places no node. */
  overlay?: boolean;
  data?: Record<string, unknown>;
}

export interface GraphGroupSpec {
  id: string;
  label: string;
  description?: string;
  /** The key its hue is drawn from; its id when not given. */
  hue?: string;
  data?: Record<string, unknown>;
}

export interface GraphSpec {
  nodes: GraphNodeSpec[];
  edges: GraphEdgeSpec[];
  groups?: GraphGroupSpec[];
}

/** How much of a node's body is drawn: every line, the key lines, none. */
export type Detail = "all" | "key" | "none";

export interface Element {
  group: "nodes" | "edges";
  data: Record<string, unknown>;
  classes: string;
}

/** What a box shows (REQ-2239, amended 2026-11-28, THE SHARED NODE STYLE IS COMPACT): its name, then
 * its body at ``detail``. No type row: the type is the band's colour and the legend's. */
export function nodeRows(node: GraphNodeSpec, detail: Detail): { name: string; rows: string[] } {
  const body = detail === "none" ? [] : (node.lines ?? []).filter((l) => detail === "all" || l.key);
  return { name: node.name, rows: body.map((l) => l.text) };
}

/** A box's height: its name's row, and its body's rows under it. */
export const nodeHeight = (rowCount: number) =>
  rowCount === 0
    ? SIZE.nodeHeight
    : 2 * SIZE.padY + SIZE.nameStep + SIZE.bodyGap + rowCount * SIZE.lineStep;

/** Where a box's text runs: from past its band to short of its right edge. */
export const TEXT_LEFT = SIZE.band + SIZE.padX;
export const TEXT_RIGHT = SIZE.padX;
/** The widest a box grows for its text; past it the text is ellipsised. */
export const NODE_MAX_WIDTH = 240;
/** A bold name is a little wider than ``textWidth``'s regular weight. */
const BOLD = 1.08;

/** A line's width drawn at ``size`` px, as near as can be told without a canvas: a little over the
 * system sans (or, ``mono``, the monospace) widths, so a node sized by it is wide enough. */
export function textWidth(line: string, size: number, mono = false): number {
  if (mono) return line.length * size * 0.62;
  let em = 0;
  for (const c of line) {
    if ("iljtfrI.,:;|!'()[] ".includes(c)) em += 0.34;
    else if ("mwMW@".includes(c)) em += 0.92;
    else if (c >= "A" && c <= "Z") em += 0.72;
    else if (c >= "0" && c <= "9") em += 0.6;
    else em += 0.6;
  }
  return em * size;
}

/** The width of a box's name (bold, SIZE.name). */
export const nameWidth = (name: string) => textWidth(name, SIZE.name) * BOLD;
/** The width of one of a box's body rows (monospace, SIZE.lineFont). */
export const rowWidth = (row: string) => textWidth(row, SIZE.lineFont, true);

/** ``text`` cut to ``room`` px as ``measure`` reads it, its end an ellipsis; whole when it fits. */
export function ellipsise(text: string, room: number, measure: (t: string) => number): string {
  if (measure(text) <= room) return text;
  let cut = text.length;
  while (cut > 0 && measure(`${text.slice(0, cut)}…`) > room) cut--;
  return `${text.slice(0, cut)}…`;
}

/** A box's width and its text's room: as wide as its name and body need, from SIZE.nodeWidth up
 * to NODE_MAX_WIDTH. */
export function boxWidth(name: string, rows: string[]): { w: number; text: number } {
  const need = Math.max(nameWidth(name), ...rows.map(rowWidth));
  const w = Math.min(
    NODE_MAX_WIDTH,
    Math.max(SIZE.nodeWidth, Math.ceil(TEXT_LEFT + need + TEXT_RIGHT)),
  );
  return { w, text: w - TEXT_LEFT - TEXT_RIGHT };
}

/** The room ELK keeps round a group's nodes at each side, and the padding its drawn box takes
 * there (stylesheet, flow.ts GROUP_OPTIONS, presets.ts): one value, so the two agree. */
export const GROUP_PAD = 16;
/** The title's text background padding each side (stylesheet ``text-background-padding``). */
const TITLE_BG_PAD = 3;
/** The room a group's title keeps from the box's rounded corners, each side. */
const TITLE_INSET = 8;

/** REQ-2239 (amended 2026-11-28): the least width of a group's box, padding included, so its title
 * (uppercase, bold, SIZE.groupLabel) lies inside its sides; never less than a one-node box's. */
export function groupMinWidth(title: string): number {
  const titled = textWidth(title, SIZE.groupLabel) * BOLD + 2 * TITLE_BG_PAD + 2 * TITLE_INSET;
  return Math.ceil(Math.max(SIZE.nodeWidth + 2 * GROUP_PAD, titled));
}

/** A group's ELK options for its least width (groupMinWidth): ELK keeps that room for the group,
 * its nodes centred across it, as the drawn box (min-width, centred on its nodes) takes it, so a
 * neighbour never comes nearer than the group's title. */
export function groupSizeOptions(minWidth: number): Record<string, string> {
  return {
    "elk.nodeSize.constraints": "[MINIMUM_SIZE]",
    "elk.nodeSize.minimum": `(${minWidth}, 0)`,
    "elk.contentAlignment": "[H_CENTER, V_TOP]",
  };
}

/** The elements of ``spec``. Group ids, node ids and edge ids are the view's; each element also
 * carries its view data, handed back on a click. */
export function graphElements(spec: GraphSpec, detail: Detail = "all"): Element[] {
  const groups: Element[] = (spec.groups ?? []).map((g) => ({
    group: "nodes",
    data: {
      ...g.data,
      id: g.id,
      kind: "group",
      title: g.label.toUpperCase(),
      // The least width of its box, padding included (groupMinWidth); its body's, inside it.
      minW: groupMinWidth(g.label.toUpperCase()),
      minInner: groupMinWidth(g.label.toUpperCase()) - 2 * GROUP_PAD,
      label: g.label,
      description: g.description ?? "",
      hue: groupHue(g.hue ?? g.id),
    },
    classes: "pg-group",
  }));
  const nodes: Element[] = spec.nodes.map((n) => {
    const kind = n.kind ?? "node";
    const shape = kind === "node" ? typeShape(n.type) : "box";
    // A diamond and a fold node carry their name only, centred; a box its name and body.
    const { name, rows } =
      kind === "node" && shape === "box" ? nodeRows(n, detail) : { name: n.name, rows: [] };
    const box =
      shape === "diamond"
        ? { w: SIZE.diamondWidth, text: SIZE.diamondWidth - 2 * SIZE.diamondInset }
        : boxWidth(name, rows);
    const title = ellipsise(name, box.text, nameWidth);
    const shown = rows.map((r) => ellipsise(r, box.text, rowWidth));
    // What was cut is shown whole on hover (the default card).
    const clipped = [...(title !== name ? [name] : []), ...rows.filter((r, i) => shown[i] !== r)];
    const card = kind === "node" && shape === "box";
    return {
      group: "nodes",
      data: {
        ...n.data,
        id: n.id,
        kind,
        type: n.type,
        name: n.name,
        label: [title, ...shown].join("\n"),
        // A box's text is drawn as its card (stylesheet cardImage); the rest as a centred label.
        caption: card ? "" : title,
        title,
        rows: shown,
        clipped,
        shape,
        h: shape === "diamond" ? SIZE.diamondHeight : nodeHeight(shown.length),
        w: box.w,
        textW: box.text,
        body: shown.length > 0 ? "yes" : "no",
        description: n.description ?? "",
        ...(n.parent !== undefined ? { parent: n.parent } : {}),
        ...(n.lane !== undefined ? { lane: n.lane } : {}),
        ...(n.status !== undefined ? { status: n.status } : {}),
        ...(n.withdrawn ? { withdrawn: "yes" } : {}),
      },
      classes: kind === "more" ? "pg-node pg-more" : "pg-node",
    };
  });
  // Which of the lines between one pair of nodes this is: their labels are stepped apart.
  const pairs = new Map<string, number>();
  const edges: Element[] = spec.edges
    .filter((e) => !e.overlay)
    .map((e) => {
      const pair = e.source < e.target ? `${e.source}|${e.target}` : `${e.target}|${e.source}`;
      const pathIndex = pairs.get(pair) ?? 0;
      pairs.set(pair, pathIndex + 1);
      return edgeElement(e, pathIndex);
    });
  return [...groups, ...nodes, ...edges];
}

export function edgeElement(e: GraphEdgeSpec, pathIndex = 0): Element {
  return {
    group: "edges",
    data: {
      ...e.data,
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label ?? "",
      arrow: e.arrow ?? "none",
      pathIndex,
      ...(e.dashed ? { dashed: true } : {}),
      ...(e.overlay ? { overlay: true } : {}),
    },
    classes: "pg-edge",
  };
}

/** The types ``spec`` draws, in NODE_TYPES' order: what the legend keys. */
export function typesDrawn(spec: GraphSpec): NodeType[] {
  const drawn = new Set(spec.nodes.filter((n) => n.kind !== "more").map((n) => n.type));
  return (Object.keys(NODE_TYPES) as NodeType[]).filter((t) => drawn.has(t));
}
