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
  /** The legs through an ERD junction. */
  tone?: "via";
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

const RULE = "─".repeat(18);

/** A node's label: its name and its type's name, then its body at ``detail``. */
export function nodeLabel(
  node: GraphNodeSpec,
  typeName: string,
  detail: Detail,
): { label: string; lineCount: number } {
  const head = [node.name, typeName.toUpperCase()];
  const body =
    detail === "none" ? [] : (node.lines ?? []).filter((l) => detail === "all" || l.key);
  const lines = body.length > 0 ? [...head, RULE, ...body.map((l) => l.text)] : head;
  return { label: lines.join("\n"), lineCount: lines.length };
}

/** A node's height: two lines, or its body's. */
export const nodeHeight = (lineCount: number) =>
  lineCount <= 2 ? SIZE.nodeHeight : lineCount * SIZE.line + 14;

/** Where a box's label may run: from past its band and icon to short of its right edge. */
export const TEXT_LEFT = SIZE.band + 8 + SIZE.icon + 8;
export const TEXT_RIGHT = 10;
/** The widest a box grows for its label; past it the label wraps. */
export const NODE_MAX_WIDTH = 360;

/** A line's width drawn at ``size`` px, as near as can be told without a canvas: a little over the
 * system sans (or, ``mono``, the monospace) widths, so a node sized by it is wide enough. A label
 * longer than its node still never runs past its room: it wraps (the stylesheet's
 * ``text-overflow-wrap``). */
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

/** A box's width and the label's room in it: wide enough for its name and type's lines (from
 * SIZE.nodeWidth to NODE_MAX_WIDTH); its body's lines wrap inside it. */
export function boxWidth(head: string[], mono: boolean): { w: number; text: number } {
  const size = mono ? 10 : SIZE.name;
  const need = Math.max(...head.map((l) => textWidth(l, size, mono)));
  const w = Math.min(NODE_MAX_WIDTH, Math.max(SIZE.nodeWidth, Math.ceil(TEXT_LEFT + need + TEXT_RIGHT)));
  return { w, text: w - TEXT_LEFT - TEXT_RIGHT };
}

/** How many lines ``lines`` take in ``room`` px, each wrapped where it runs past. */
const wrappedCount = (lines: string[], room: number, size: number, mono: boolean) =>
  lines.reduce((n, l) => n + Math.max(1, Math.ceil(textWidth(l, size, mono) / room)), 0);

/** The elements of ``spec``. Group ids, node ids and edge ids are the view's; each element also
 * carries its view data, handed back on a click. */
export function graphElements(
  spec: GraphSpec,
  typeName: (type: NodeType) => string,
  detail: Detail = "all",
): Element[] {
  const groups: Element[] = (spec.groups ?? []).map((g) => ({
    group: "nodes",
    data: {
      ...g.data,
      id: g.id,
      kind: "group",
      title: g.label.toUpperCase(),
      label: g.label,
      description: g.description ?? "",
      hue: groupHue(g.hue ?? g.id),
    },
    classes: "pg-group",
  }));
  const nodes: Element[] = spec.nodes.map((n) => {
    const kind = n.kind ?? "node";
    const shape = kind === "node" ? typeShape(n.type) : "box";
    const { label, lineCount } =
      kind === "more"
        ? { label: n.name, lineCount: 1 }
        : // A diamond carries its name and type only; it has no body.
          nodeLabel(n, typeName(n.type), shape === "diamond" ? "none" : detail);
    const mono = lineCount > 2;
    const lines = label.split("\n");
    const box =
      kind === "node" && shape === "box"
        ? boxWidth(lines.slice(0, 2), mono)
        : { w: SIZE.nodeWidth, text: SIZE.nodeWidth - 2 * TEXT_RIGHT };
    // The lines the label takes in its room, a long name wrapped.
    const drawnLines =
      kind === "node" && shape === "box"
        ? wrappedCount(lines, box.text, mono ? 10 : SIZE.name, mono)
        : lineCount;
    return {
      group: "nodes",
      data: {
        ...n.data,
        id: n.id,
        kind,
        type: n.type,
        name: n.name,
        label,
        lineCount,
        shape,
        h:
          shape === "diamond"
            ? SIZE.diamondHeight
            : kind === "more"
              ? SIZE.nodeHeight - 12
              : nodeHeight(drawnLines),
        w: shape === "diamond" ? SIZE.diamondWidth : box.w,
        textW: box.text,
        body: lineCount > 2 ? "yes" : "no",
        description: n.description ?? "",
        ...(n.parent !== undefined ? { parent: n.parent } : {}),
        ...(n.lane !== undefined ? { lane: n.lane } : {}),
        ...(n.status !== undefined ? { status: n.status } : {}),
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
      ...(e.tone ? { tone: e.tone } : {}),
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
