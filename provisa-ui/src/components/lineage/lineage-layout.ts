// Copyright (c) 2026 Kenneth Stott
// Canary: 3e7d9a52-1b64-4c08-8f2a-6d5c0e9b7a41
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1665: the lineage DAG as relation boxes with column rows, the shape the SQL canvas draws.
//
// A column-per-node layout has no notion of a relation as a box, so a federation graph sprawls: a
// layout engine spreads the columns of one table across the canvas and the reader loses which
// dataset a column belongs to. Here a relation is ONE box whose rows are its columns, edges run row
// to row, and only the boxes are laid out — rows are pinned inside their box in a stable order.
//
// Intermediate computations get boxes too: a CTE or subquery is a relation the lineage already
// names, and the statement's final projection becomes the `result` box (or the view it defines).
// Rows that no edge touches are not drawn; the box header counts them, so a wide table referenced
// by two columns stays two rows tall.
//
// Pure: no cytoscape here. The component turns these into elements and positions.

import type { LineageGraphData, LineageNative, LineageNode } from "../../api/lineage";
import { SOURCE_TYPES } from "../../pages/sources/constants";
import { shownSourceType, sourceTypeLabel } from "../../pages/sources/sourceHelpers";
import { sourceLogoImage } from "../sourceLogoMarks";

export type Role =
  | "source"
  | "intermediate"
  | "output"
  | "command"
  | "dataset"
  | "native"
  | "job"
  | "external";

/** How a field of each role is typeset — one definition for the DAG and its legend. */
export interface RoleStyle {
  color: string;
  fill: string;
  fontStyle: "normal" | "italic";
  fontWeight: "normal" | "bold";
}
export const ROLE_STYLE: Record<Role, RoleStyle> = {
  // a base column leaf
  source: { color: "#2f9e44", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // produced AND consumed: a hand-off between datasets
  intermediate: { color: "#0c8599", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // a terminal result column
  output: { color: "#1c7ed6", fill: "#ffffff", fontStyle: "italic", fontWeight: "normal" },
  // an opaque command boundary
  command: { color: "#9c36b5", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // a collapsed relation
  dataset: { color: "#5c7cfa", fill: "#ffffff", fontStyle: "normal", fontWeight: "bold" },
  // REQ-2116: a raw field as its source reads it
  native: { color: "#e8590c", fill: "#ffffff", fontStyle: "normal", fontWeight: "normal" },
  // REQ-2229: a job an OpenLineage event reported
  job: { color: "#b35c00", fill: "#fff4e6", fontStyle: "normal", fontWeight: "bold" },
  // REQ-2229: a dataset no registered table stands for
  external: { color: "#495057", fill: "#f1f3f5", fontStyle: "italic", fontWeight: "normal" },
};

/** REQ-2229: how an edge an OpenLineage event reported is drawn, and keyed in the legend. */
export const REPORTED_EDGE_COLOR = "#b35c00";

/** REQ-2229: the words for what OpenLineage reported, already translated. */
export interface ReportedWords {
  job: string;
  outside: string;
  reported: string;
}

/** REQ-2116: a native box's source, drawn in a head above its fields: the source's logo and type
 *  label (brand before carrier, as the Sources page shows it) and the address it is read from. */
export interface NativeHead {
  logo: string;
  typeLabel: string;
  lines: string[];
}

export interface LineageRow {
  id: string;
  label: string;
  role: Role;
  kind: LineageNode["kind"];
  output: boolean;
  materialized: boolean;
  cycle: string; // "no" | "feedback" | "error"
}

export interface LineageBox {
  id: string;
  /** The relation this box stands for; null for a synthetic box (result, a lone unresolved column). */
  relation: string | null;
  label: string;
  rows: LineageRow[];
  /** REQ-2116: set on a native box. */
  native: NativeHead | null;
  collapsed: boolean;
  /** Columns of the relation no edge touches — counted in the header, not drawn. */
  unused: number;
  /** REQ-1667, REQ-2174: the swimlane (lanesFrom): 0 the lane-0 relations, -1 their inputs, -1 - n
   *  n hops beyond the inputs, +1 a consumer. */
  lane: number | null;
  width: number;
  height: number;
}

export interface LineageDrawnEdge {
  id: string;
  source: string; // row id, or box id when that end is collapsed
  target: string;
  label: string;
  command: boolean;
  /** The command a drawn edge's transform names (a call or a statement command's write). */
  commandName: string | null;
  count: number;
  /** REQ-2174: the views this edge reads through (a view between an input and a member is folded
   *  into the edge, not drawn), in the order the column passes through them, named as a box is. */
  via: string[];
  /** REQ-2229: an OpenLineage event reported this edge; the model's definitions did not. */
  reported: boolean;
}

export interface LineageLayoutModel {
  boxes: LineageBox[];
  /** Row-to-row (or box) edges, the ones drawn. */
  edges: LineageDrawnEdge[];
}

export const RESULT_BOX = "result";
/** A native hop's node relation: this prefix, then the registered relation it lands in
 *  (provisa/lineage/native.py NATIVE_PREFIX). */
export const NATIVE_PREFIX = "native:";
export const boxId = (relation: string) => `rel:${relation}`;

// Geometry in canvas units. Monospace at 11px runs about 6.6px per glyph.
export const HEADER_H = 26;
export const ROW_H = 20;
export const NATIVE_LINE_H = 14;
const NATIVE_LOGO_W = 24;
export const FOOTER_H = 16;
export const BOX_MIN_W = 140;
export const BOX_MAX_W = 320;
const CHAR_W = 6.6;
const ROW_PAD_X = 12;

const textWidth = (s: string) => s.length * CHAR_W;

export interface LineageModelOptions {
  /** The box the statement's final projection lands in — the view or MV it defines, else "result". */
  resultLabel?: string;
  /** Draw every column as a row, not only the ones an edge touches (Complete Lineage: every column
   *  the role can see is in use, whether or not a registered view derives from it). */
  allColumns?: boolean;
  /** REQ-1667, REQ-2174: the relations of lane 0 (``members``) and their inputs (``inputs``, lane
   *  -1); every other box takes its lane from them (lanesFrom). */
  lanesAround?: { members: ReadonlySet<string>; inputs: ReadonlySet<string> };
  /** How a box names its registered relation — a page that knows the catalog names it as the
   *  catalog does. Unset, a box is named by the relation the server reports. */
  relationLabel?: (relation: string) => string;
  /** REQ-2229: how a reported job's box and a box outside the model say what they are. */
  reportedWords?: ReportedWords;
}

export function buildLineageModel(
  source: LineageGraphData,
  collapsedRelations: ReadonlySet<string> | undefined,
  options: LineageModelOptions = {},
): LineageLayoutModel {
  const graph = options.lanesAround
    ? foldViewsOntoEdges(source, options.lanesAround, options.relationLabel ?? ((r) => r))
    : source;
  const resultLabel = options.resultLabel ?? RESULT_BOX;
  const cycleClass: Record<string, string> = {};
  for (const c of graph.cycles ?? []) for (const n of c.nodes) cycleClass[n] = c.classification;
  const outputs = new Set(graph.outputs);
  const hasIn = new Set(graph.edges.map((e) => e.target));
  const hasOut = new Set(graph.edges.map((e) => e.source));
  // An output is always a row, edge or not: it is what the graph is FOR.
  const touched = new Set([...hasIn, ...hasOut, ...outputs]);

  const roleOf = (n: LineageNode): Role => {
    if (n.kind === "command") return "command";
    if (n.kind === "native") return "native";
    if (n.kind === "job") return "job";
    if (n.kind === "external") return "external";
    if (n.kind === "source") return "source";
    return hasIn.has(n.id) && hasOut.has(n.id) ? "intermediate" : "output";
  };

  // Which box each node lives in. A relation names its own box; the final projection's columns
  // share the result box; anything else unresolved stands alone as a one-row box.
  const natives = graph.natives ?? {};
  // REQ-2116: the table a native hop lands in says how it is served.
  const servedOf = new Map(Object.values(natives).map((nt) => [nt.relation, servedText(nt)]));
  const named = options.relationLabel ?? ((relation: string) => relation);
  const boxOf = (n: LineageNode): { id: string; relation: string | null; label: string } => {
    // REQ-2229: a reported job, or a dataset outside the model, goes by what the event called it.
    if (n.relation && (n.kind === "job" || n.kind === "external")) {
      const what = n.kind === "job" ? options.reportedWords?.job : options.reportedWords?.outside;
      const name = n.label ?? n.relation;
      return { id: boxId(n.relation), relation: n.relation, label: what ? `${name} · ${what}` : name };
    }
    const native = n.relation ? natives[n.relation] : undefined;
    if (n.relation && native)
      return { id: boxId(n.relation), relation: n.relation, label: nativeLabel(native) };
    // REQ-2116: natives (where a table is read from) go only to a user with source registration;
    // for anyone else a native box is named by its source and the names the source gives the
    // object (REQ-2156), never by the internal "native:" key.
    if (n.relation?.startsWith(NATIVE_PREFIX)) {
      return { id: boxId(n.relation), relation: n.relation, label: sourceNames(n) };
    }
    if (n.relation) {
      const served = servedOf.get(n.relation);
      const label = served ? `${named(n.relation)} · ${served}` : named(n.relation);
      return { id: boxId(n.relation), relation: n.relation, label };
    }
    if (outputs.has(n.id) || n.kind === "derived")
      return { id: boxId(resultLabel), relation: null, label: resultLabel };
    return { id: `lone:${n.id}`, relation: null, label: n.column };
  };

  const boxes = new Map<string, LineageBox>();
  const nodeBox = new Map<string, string>();
  for (const n of graph.nodes) {
    const b = boxOf(n);
    nodeBox.set(n.id, b.id);
    let box = boxes.get(b.id);
    if (!box) {
      box = {
        id: b.id,
        relation: b.relation,
        label: b.label,
        rows: [],
        native: b.relation && natives[b.relation] ? nativeHead(natives[b.relation]) : null,
        collapsed: !!b.relation && !!collapsedRelations?.has(b.relation),
        unused: 0,
        lane: null,
        width: 0,
        height: 0,
      };
      boxes.set(b.id, box);
    }
    // A lone unresolved column is its own box, so it is drawn even with no edge.
    if (!options.allColumns && !touched.has(n.id) && b.relation !== null) {
      box.unused += 1;
      continue;
    }
    box.rows.push({
      id: n.id,
      label: n.column,
      role: roleOf(n),
      kind: n.kind,
      output: outputs.has(n.id),
      materialized: n.materialized,
      cycle: cycleClass[n.id] ?? "no",
    });
  }

  for (const box of boxes.values()) {
    // Stable row order: the order the graph lists them, sources first inside a mixed box.
    const rank: Record<Role, number> = {
      native: 0,
      source: 0,
      intermediate: 1,
      output: 2,
      command: 3,
      dataset: 4,
      job: 5,
      external: 5,
    };
    box.rows.sort((a, b) => rank[a.role] - rank[b.role]);
    const header = box.collapsed
      ? `${box.label}  (${box.rows.length + box.unused} cols)`
      : box.label;
    const widest = Math.max(
      textWidth(header) + 16,
      ...box.rows.map((r) => textWidth(r.label) + ROW_PAD_X * 2),
      ...(box.native?.lines ?? []).map((l) => textWidth(l) + NATIVE_LOGO_W + ROW_PAD_X * 2),
    );
    box.width = Math.min(BOX_MAX_W, Math.max(BOX_MIN_W, Math.ceil(widest)));
    box.height = box.collapsed
      ? HEADER_H + 6
      : HEADER_H +
        (box.native ? nativeHeadHeight(box.native) : 0) +
        box.rows.length * ROW_H +
        (box.unused > 0 ? FOOTER_H : 0) +
        4;
  }

  // The end an edge is drawn to: the row, or the whole box when that relation is collapsed.
  const drawnEnd = (nodeId: string): string => {
    const id = nodeBox.get(nodeId) ?? nodeId;
    const box = boxes.get(id);
    return box && box.collapsed ? box.id : nodeId;
  };

  const rolled = new Map<string, LineageDrawnEdge>();
  for (const e of graph.edges) {
    const from = drawnEnd(e.source);
    const target = drawnEnd(e.target);
    if (from === target) continue; // internal to one collapsed relation
    const label = (e.ops ?? [])
      .filter((o) => o.kind !== "identity" && o.kind !== "constant")
      .map((o) => (o.args?.length ? `${o.name}(${o.args.join(", ")})` : o.name))
      .join(" ");
    const command = (e.ops ?? []).some((o) => o.kind === "command");
    const commandName =
      (e.ops ?? []).find((o) => o.kind === "command" || o.kind === "command_write")?.name ?? null;
    const key = `${from} ${target}`;
    const prior = rolled.get(key);
    const reported = e.origin === "openlineage";
    if (prior) {
      prior.count += 1;
      prior.reported = prior.reported || reported;
      prior.command = prior.command || command;
      prior.commandName = prior.commandName === commandName ? commandName : null;
      prior.via = [...new Set([...prior.via, ...(e.via ?? [])])];
      prior.label = ""; // several transforms behind one drawn edge: report the count, not one formula
    } else {
      rolled.set(key, {
        id: `e${rolled.size}`,
        source: from,
        target,
        label,
        command,
        commandName,
        count: 1,
        via: [...(e.via ?? [])],
        reported,
      });
    }
  }

  orderRowsByUpstream(boxes, nodeBox, graph);
  if (options.lanesAround) assignLanes(boxes, nodeBox, graph, options.lanesAround);

  return {
    boxes: Array.from(boxes.values()),
    edges: Array.from(rolled.values()),
  };
}

// Rows in a downstream box take the order of the rows that feed them (their mean upstream row
// index), so a column-for-column copy draws as parallel wires instead of a braid. Boxes are
// visited upstream-first; a row nothing feeds keeps its role rank, after the fed ones.
function orderRowsByUpstream(
  boxes: Map<string, LineageBox>,
  nodeBox: Map<string, string>,
  graph: LineageGraphData,
): void {
  const upstreamOf = new Map<string, string[]>();
  const feeders = new Map<string, Set<string>>(); // box → boxes feeding it
  for (const e of graph.edges) {
    const sb = nodeBox.get(e.source);
    const tb = nodeBox.get(e.target);
    if (!sb || !tb || sb === tb) continue;
    (upstreamOf.get(e.target) ?? upstreamOf.set(e.target, []).get(e.target)!).push(e.source);
    (feeders.get(tb) ?? feeders.set(tb, new Set()).get(tb)!).add(sb);
  }
  const rowIndex = new Map<string, number>();
  const done = new Set<string>();
  const visit = (id: string, trail: Set<string>) => {
    if (done.has(id) || trail.has(id)) return; // a cycle keeps whichever order it already has
    trail.add(id);
    for (const f of feeders.get(id) ?? []) visit(f, trail);
    trail.delete(id);
    const box = boxes.get(id);
    if (!box) return;
    const key = (r: LineageRow): number | null => {
      const ups = (upstreamOf.get(r.id) ?? [])
        .map((u) => rowIndex.get(u))
        .filter((i): i is number => i !== undefined);
      return ups.length ? ups.reduce((a, b) => a + b, 0) / ups.length : null;
    };
    const ranked = box.rows.map((r, i) => ({ r, i, k: key(r) }));
    ranked.sort((a, b) => {
      if (a.k === null && b.k === null) return a.i - b.i;
      if (a.k === null) return 1;
      if (b.k === null) return -1;
      return a.k - b.k || a.i - b.i;
    });
    box.rows = ranked.map((x) => x.r);
    box.rows.forEach((r, i) => rowIndex.set(r.id, i));
    done.add(id);
  };
  for (const id of boxes.keys()) visit(id, new Set());
}

/** Where row `index` of a box centred at (cx, cy) sits — the top-left origin is the box's own. */
export function rowPosition(
  box: { width: number; height: number },
  center: { x: number; y: number },
  index: number,
): { x: number; y: number } {
  const top = center.y - box.height / 2;
  return { x: center.x, y: top + HEADER_H + index * ROW_H + ROW_H / 2 };
}

export function rowWidth(box: { width: number }): number {
  return box.width - 8;
}

// REQ-1667, REQ-2174 (amended 2026-11-07): lanes around the lane-0 relations and their inputs
// (lanesFrom). A box the graph does not connect to lane 0 at all has no lane and is left to the
// layout.
function assignLanes(
  boxes: Map<string, LineageBox>,
  nodeBox: Map<string, string>,
  graph: LineageGraphData,
  around: { members: ReadonlySet<string>; inputs: ReadonlySet<string> },
): void {
  const links: [string, string][] = [];
  for (const e of graph.edges) {
    const sb = nodeBox.get(e.source);
    const tb = nodeBox.get(e.target);
    if (sb && tb) links.push([sb, tb]);
  }
  const of = (relations: ReadonlySet<string>) =>
    [...boxes.values()]
      .filter((b) => b.relation !== null && relations.has(b.relation))
      .map((b) => b.id);
  const lane = lanesFrom(links, of(around.members), of(around.inputs));
  for (const box of boxes.values()) box.lane = lane.get(box.id) ?? null;
}

/** REQ-1667, REQ-2128, REQ-2174 (amended 2026-11-07, 2026-11-08): the lane of each node of a
 * directed graph (``[from, to]`` links) upstream of ``zero`` (lane 0). Its ``inputs`` are lane -1
 * wherever they sit; a node upstream of an input is -1 - n, n its LONGEST path down to an input
 * (one that feeds an input both directly and through another sits a lane further left, so every
 * edge runs rightward across lanes). A node a lane-0 node feeds directly, not upstream, is +1.
 * A node not so connected, or upstream of lane 0 but of no input (a view between: viewsBetween,
 * which is folded into an edge, not drawn), has no lane. */
export function lanesFrom(
  links: [string, string][],
  zero: string[],
  inputs: string[],
): Map<string, number> {
  const { lane } = analyseLanes(links, zero, inputs);
  return lane;
}

/** REQ-2174 (amended 2026-11-08): the nodes upstream of lane 0 that are not an input and have no
 * input beyond them in the graph's direction of flow: what lane 0 reads its inputs through, or reads
 * from no input. They are not drawn; buildLineageModel folds each into the edges around it. */
export function viewsBetween(
  links: [string, string][],
  zero: string[],
  inputs: string[],
): Set<string> {
  return analyseLanes(links, zero, inputs).between;
}

function analyseLanes(
  links: [string, string][],
  zero: string[],
  inputs: string[],
): { lane: Map<string, number>; between: Set<string> } {
  const feeders = new Map<string, Set<string>>();
  const consumers = new Map<string, Set<string>>();
  for (const [sb, tb] of links) {
    if (sb === tb) continue;
    (feeders.get(tb) ?? feeders.set(tb, new Set()).get(tb)!).add(sb);
    (consumers.get(sb) ?? consumers.set(sb, new Set()).get(sb)!).add(tb);
  }
  const zeroSet = new Set(zero);
  const inputSet = new Set(inputs.filter((id) => !zeroSet.has(id)));
  // Longest path DOWN to an input from each box, memoized; a cycle contributes nothing.
  const depth = new Map<string, number>();
  const visiting = new Set<string>();
  const depthOf = (id: string): number => {
    if (inputSet.has(id)) return 0;
    const known = depth.get(id);
    if (known !== undefined) return known;
    if (visiting.has(id)) return -Infinity;
    visiting.add(id);
    let best = -Infinity;
    for (const c of consumers.get(id) ?? []) {
      const d = depthOf(c);
      if (d !== -Infinity) best = Math.max(best, d + 1);
    }
    visiting.delete(id);
    depth.set(id, best);
    return best;
  };
  const upstream = new Set<string>();
  const stack = [...zero];
  while (stack.length > 0) {
    const id = stack.pop()!;
    for (const f of feeders.get(id) ?? []) {
      if (upstream.has(f) || zeroSet.has(f)) continue;
      upstream.add(f);
      stack.push(f);
    }
  }
  const lane = new Map<string, number>();
  const between = new Set<string>();
  for (const id of zero) lane.set(id, 0);
  for (const id of upstream) {
    const d = depthOf(id);
    if (d === -Infinity) between.add(id);
    else lane.set(id, -1 - d);
  }
  for (const id of zero) {
    for (const c of consumers.get(id) ?? []) if (!lane.has(c) && !between.has(c)) lane.set(c, 1);
  }
  return { lane, between };
}

/** REQ-2174 (amended 2026-11-08): a view between an input and a member is not a node. Each column
 * path that enters a between relation and leaves it again becomes one edge from the column that
 * fed the first view to the column the last view feeds, carrying the transforms along the path and
 * the views' names (``via``). Every other node and edge is kept. */
function foldViewsOntoEdges(
  graph: LineageGraphData,
  around: { members: ReadonlySet<string>; inputs: ReadonlySet<string> },
  name: (relation: string) => string,
): LineageGraphData {
  const relationOf = new Map(graph.nodes.map((n) => [n.id, n.relation ?? `\0${n.id}`]));
  const links: [string, string][] = graph.edges.map((e) => [
    relationOf.get(e.source) ?? e.source,
    relationOf.get(e.target) ?? e.target,
  ]);
  const views = viewsBetween(links, [...around.members], [...around.inputs]);
  if (views.size === 0) return graph;
  const isView = (id: string) => views.has(relationOf.get(id) ?? id);

  const leaving = new Map<string, LineageGraphData["edges"]>();
  for (const e of graph.edges)
    (leaving.get(e.source) ?? leaving.set(e.source, []).get(e.source)!).push(e);

  const edges: LineageGraphData["edges"] = graph.edges.filter(
    (e) => !isView(e.source) && !isView(e.target),
  );
  const folded = new Map<string, number>();
  for (const first of graph.edges) {
    if (isView(first.source) || !isView(first.target)) continue;
    // Walk every column path through the views to the columns that leave them.
    const walk = (
      at: string,
      ops: LineageGraphData["edges"][number]["ops"],
      via: string[],
      trail: Set<string>,
    ) => {
      for (const next of leaving.get(at) ?? []) {
        if (trail.has(next.target)) continue; // a cycle among the views
        const allOps = [...ops, ...next.ops];
        if (isView(next.target)) {
          const view = name(relationOf.get(next.target) as string);
          walk(
            next.target,
            allOps,
            via.includes(view) ? via : [...via, view],
            new Set(trail).add(next.target),
          );
        } else {
          const key = `${first.source} ${next.target} ${via.join(",")}`;
          if (folded.has(key)) continue;
          folded.set(key, edges.length);
          edges.push({
            source: first.source,
            target: next.target,
            transform: "",
            ops: allOps,
            via,
          });
        }
      }
    };
    const view = name(relationOf.get(first.target) as string);
    walk(first.target, first.ops, [view], new Set([first.target]));
  }
  const kept = new Set(graph.nodes.filter((n) => !isView(n.id)).map((n) => n.id));
  return {
    ...graph,
    nodes: graph.nodes.filter((n) => kept.has(n.id)),
    edges,
    outputs: graph.outputs.filter((o) => kept.has(o)),
    ...(graph.cycles
      ? { cycles: graph.cycles.filter((c) => c.nodes.every((n) => kept.has(n))) }
      : {}),
  };
}

/** The text a drawn edge carries: its transform (or the count of columns behind it) and, when it
 * reads through views, their names; REQ-2229: and, when an OpenLineage event reported it, that. */
export function edgeText(
  e: Pick<LineageDrawnEdge, "label" | "count" | "via" | "reported">,
  reportedWord = "",
): string {
  const what = e.count > 1 ? `${e.count} columns` : e.label;
  const text = e.via.length > 0 ? [what, `via ${e.via.join(", ")}`].filter(Boolean).join(" ") : what;
  return e.reported ? [text, reportedWord].filter(Boolean).join(" · ") : text;
}

/** REQ-2116: the height of a native box's head -- one line per address part, the logo beside. */
export function nativeHeadHeight(head: NativeHead): number {
  return Math.max(NATIVE_LOGO_W, head.lines.length * NATIVE_LINE_H) + 6;
}

/** REQ-2156: a native object by its source's own names -- the source, then its schema and table
 *  where it has them. */
function sourceNames(n: LineageNode): string {
  const object = [n.source_schema, n.source_table].filter(Boolean).join(".");
  return object ? `${n.source_id} · ${object}` : (n.source_id as string);
}

function nativeHints(native: LineageNative): string {
  return JSON.stringify(native.hints);
}

function nativeLabel(native: LineageNative): string {
  return `${sourceTypeLabel(native.type, nativeHints(native))} · ${native.source}`;
}

function nativeHead(native: LineageNative): NativeHead {
  const shown = shownSourceType(native.type, nativeHints(native));
  const typeLabel = sourceTypeLabel(native.type, nativeHints(native));
  const category = SOURCE_TYPES.find((st) => st.value === shown)?.category ?? "Other";
  return {
    logo: sourceLogoImage(shown, typeLabel, category),
    typeLabel,
    lines: native.address.map((a) => `${a.what} ${a.value}`),
  };
}

/** How a table is served: virtual, or a replica with its freshness and last load. */
export function servedText(native: LineageNative): string {
  const s = native.served;
  if (s.mode === "virtual") return "virtual";
  const parts = ["replica"];
  if (s.cache_ttl !== null) parts.push(s.cache_ttl < 0 ? "never expires" : `TTL ${s.cache_ttl}s`);
  if (s.last_load) parts.push(`loaded ${new Date(s.last_load).toLocaleString()}`);
  return parts.join(", ");
}
