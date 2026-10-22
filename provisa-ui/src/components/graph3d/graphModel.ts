// Copyright (c) 2026 Kenneth Stott
// Canary: 3c1f6a2b-8d47-4e95-a0b3-7f2e9c5d1a84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's model. Its contents are layers, each a set of edge kinds the
// user turns on and off; a node is drawn when its own layer is on or a drawn edge reaches it.
// Filters by domain, tag, sensitivity and a focus node apply to every layer; a preset is a named
// set of layers with a layout. Pure data: the scene only draws what this decides.

import { lanesFrom } from "../lineage/lineage-layout";
import { forceLayout } from "../../pages/tables/profile3d/forceLayout";

export type NodeType =
  | "product"
  | "domain"
  | "table"
  | "column"
  | "command"
  | "checker"
  | "profile"
  | "tag"
  | "term"
  | "relation";

export type EdgeKind =
  | "in_domain"
  | "product_link"
  | "member"
  | "has_column"
  | "command"
  | "checks"
  | "profiled"
  | "tagged"
  | "term_edge"
  | "term_binding"
  | "relationship"
  | "relationship_outside"
  | "lineage"
  | "column_lineage"
  | "owns";

export type LayerId =
  | "products"
  | "tables"
  | "commands"
  | "quality"
  | "tags"
  | "ontology"
  | "relationships"
  | "outside"
  | "lineage"
  // REQ-2130: the glossary's own layers.
  | "domains"
  | "data";

/** Force; lineage lanes left to right (REQ-1667); or a top-down tree (REQ-2130's Taxonomy). */
export type LayoutKind = "force" | "lanes" | "tree";

/** REQ-2130: a glossary term's standing (REQ-1387's admission rule decides proposed). */
export type TermStatus = "live" | "proposed" | "deprecated" | "retired";

export interface GraphNode {
  id: string;
  type: NodeType;
  label: string;
  /** The domains the node is in; a node with none (a tag, a checker's run) is not filtered by it. */
  domains?: string[];
  tags?: string[];
  /** The sensitive tags on the node (or on any of a table's columns). */
  sensitivities?: string[];
  /** A column's table: the column is drawn inside it until the table is expanded. */
  parent?: string;
  /** REQ-1667: the lineage lane the node stands in, when the data says (upstream negative). */
  lane?: number;
  /** REQ-2130: a term's standing; deprecated is drawn dimmed. */
  status?: TermStatus;
  /** REQ-2130: drawn hollow (an abstract term). */
  hollow?: boolean;
}

export interface GraphEdge {
  id: string;
  /** For a directed kind (lineage, member, checks), ``a`` is the source. */
  a: string;
  b: string;
  kind: EdgeKind;
  label?: string;
  /** How many paths the edge stands for (REQ-2126's path count). */
  paths?: number;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface LayerDef {
  nodes: NodeType[];
  edges: EdgeKind[];
}

/** Each layer's node type and edge kinds; a node of the type is drawn whenever its layer is on. */
export const LAYERS = {
  products: { nodes: ["product", "domain"], edges: ["in_domain", "product_link"] },
  tables: { nodes: ["table"], edges: ["member", "has_column"] },
  commands: { nodes: ["command"], edges: ["command"] },
  quality: { nodes: ["checker", "profile"], edges: ["checks", "profiled"] },
  tags: { nodes: ["tag"], edges: ["tagged"] },
  ontology: { nodes: ["term"], edges: ["term_edge", "term_binding"] },
  relationships: { nodes: [], edges: ["relationship"] },
  outside: { nodes: [], edges: ["relationship_outside"] },
  lineage: { nodes: [], edges: ["lineage", "column_lineage"] },
} satisfies Partial<Record<LayerId, LayerDef>>;

export const LAYER_ORDER: LayerId[] = [
  "products",
  "tables",
  "commands",
  "quality",
  "tags",
  "ontology",
  "relationships",
  "outside",
  "lineage",
];

export type PresetId = "knowledge" | "ontology" | "productMap" | "lineage" | "taxonomy";

export interface Preset {
  layers: LayerId[];
  layout: LayoutKind;
  /** The term edge types it draws; every type when absent. */
  edgeTypes?: string[];
}

export const PRESETS = {
  // Every layer but relationships to tables outside the product.
  knowledge: { layers: LAYER_ORDER.filter((l) => l !== "outside"), layout: "force" },
  // Terms, their edges, the columns bound to them and the products that own those columns.
  ontology: { layers: ["ontology", "products"], layout: "force" },
  // Every product, grouped by domain, joined by data_product_edges.
  productMap: { layers: ["products"], layout: "force" },
  // Lineage edges alone, left to right in upstream lanes.
  lineage: { layers: ["lineage"], layout: "lanes" },
} satisfies Partial<Record<PresetId, Preset>>;

export const PRESET_ORDER: PresetId[] = ["knowledge", "ontology", "productMap", "lineage"];

/** What a graph is scoped to: its layers and presets (REQ-2128's product graph, REQ-2130's
 * glossary), on the one engine. */
export interface GraphScope {
  layers: Partial<Record<LayerId, LayerDef>>;
  layerOrder: LayerId[];
  presets: Partial<Record<PresetId, Preset>>;
  presetOrder: PresetId[];
  /** The statuses hidden until the user shows them. */
  hiddenStatuses: TermStatus[];
}

export const PRODUCT_SCOPE: GraphScope = {
  layers: LAYERS,
  layerOrder: LAYER_ORDER,
  presets: PRESETS,
  presetOrder: PRESET_ORDER,
  hiddenStatuses: [],
};

/** REQ-2130: the glossary's Ontology View. Terms and their edges; the domains each term is in
 * (an enterprise-wide term in every one, so it sits at the centre); off by default, the data the
 * terms are bound to -- columns folded into their tables -- and the products owning those tables. */
export const GLOSSARY_SCOPE: GraphScope = {
  layers: {
    ontology: { nodes: ["term"], edges: ["term_edge"] },
    domains: { nodes: ["domain"], edges: ["in_domain"] },
    data: { nodes: ["table", "product"], edges: ["term_binding", "has_column", "member"] },
  },
  layerOrder: ["ontology", "domains", "data"],
  presets: {
    // Every edge type, in a force layout.
    ontology: { layers: ["ontology", "domains"], layout: "force" },
    // The is-a and part-of hierarchy alone, as a tree from the top down.
    taxonomy: { layers: ["ontology"], layout: "tree", edgeTypes: ["KIND_OF", "PART_OF"] },
  },
  presetOrder: ["ontology", "taxonomy"],
  hiddenStatuses: ["retired"],
};

export interface GraphFilters {
  domains: string[];
  tags: string[];
  sensitivities: string[];
  /** A product or term: only what is reached from it is drawn. */
  focus: string | null;
  /** REQ-2130: the term edge types drawn; every type when empty. */
  edgeTypes: string[];
  /** REQ-2130: the term statuses not drawn. */
  hiddenStatuses: TermStatus[];
}

export const NO_FILTERS: GraphFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  focus: null,
  edgeTypes: [],
  hiddenStatuses: [],
};

function layerOfEdge(scope: GraphScope, kind: EdgeKind): LayerId | undefined {
  return scope.layerOrder.find((l) => scope.layers[l]?.edges.includes(kind));
}

function layerOfNode(scope: GraphScope, type: NodeType): LayerId | undefined {
  return scope.layerOrder.find((l) => scope.layers[l]?.nodes.includes(type));
}

/** The layers ``data`` has anything in; a layer the user's role cannot read has nothing. */
export function availableLayers(data: GraphData, scope: GraphScope): Set<LayerId> {
  const out = new Set<LayerId>();
  for (const e of data.edges) {
    const l = layerOfEdge(scope, e.kind);
    if (l) out.add(l);
  }
  for (const n of data.nodes) {
    const l = layerOfNode(scope, n.type);
    if (l) out.add(l);
  }
  return out;
}

/** What the filters can choose from: every domain, tag and sensitivity on a node. */
export function filterChoices(data: GraphData) {
  const domains = new Set<string>();
  const tags = new Set<string>();
  const sensitivities = new Set<string>();
  for (const n of data.nodes) {
    n.domains?.forEach((d) => domains.add(d));
    n.tags?.forEach((t) => tags.add(t));
    n.sensitivities?.forEach((x) => sensitivities.add(x));
  }
  const sorted = (s: Set<string>) => [...s].sort();
  return { domains: sorted(domains), tags: sorted(tags), sensitivities: sorted(sensitivities) };
}

function passes(n: GraphNode, f: GraphFilters): boolean {
  if (f.domains.length > 0 && n.domains && n.domains.length > 0) {
    if (!n.domains.some((d) => f.domains.includes(d))) return false;
  }
  if (n.type === "domain" && f.domains.length > 0 && !f.domains.includes(n.label)) return false;
  if (f.tags.length > 0) {
    if (n.type === "tag") return f.tags.includes(n.label);
    if (n.tags !== undefined && !n.tags.some((t) => f.tags.includes(t))) return false;
  }
  if (f.sensitivities.length > 0 && n.sensitivities !== undefined) {
    if (!n.sensitivities.some((x) => f.sensitivities.includes(x))) return false;
  }
  if (n.status !== undefined && f.hiddenStatuses.includes(n.status)) return false;
  return true;
}

/** The graph as drawn: layers on, columns of unexpanded tables folded into their table, filters
 * applied. */
export function visibleGraph(
  data: GraphData,
  layers: ReadonlySet<LayerId>,
  filters: GraphFilters,
  expanded: ReadonlySet<string>,
  scope: GraphScope,
): GraphData {
  const byId = new Map(data.nodes.map((n) => [n.id, n]));
  const isOn = (type: NodeType) => {
    const l = layerOfNode(scope, type);
    return l !== undefined && layers.has(l);
  };
  const tablesOn = isOn("table");
  // A column folds into its table while the table is drawn and not expanded.
  const fold = (id: string): string => {
    const n = byId.get(id);
    if (n?.type === "column" && n.parent && tablesOn && !expanded.has(n.parent)) return n.parent;
    return id;
  };
  // Who owns each table, for a bound column drawn without its table (the Ontology preset).
  const owners = new Map<string, string[]>();
  for (const e of data.edges) {
    if (e.kind === "member") owners.set(e.b, [...(owners.get(e.b) ?? []), e.a]);
  }

  const edges = new Map<string, GraphEdge>();
  const add = (e: GraphEdge) => {
    const a = fold(e.a);
    const b = fold(e.b);
    if (a === b) return;
    const id = `${e.kind}:${a}>${b}`;
    const seen = edges.get(id);
    if (seen) seen.paths = (seen.paths ?? 1) + (e.paths ?? 1);
    else edges.set(id, { ...e, id, a, b });
  };
  for (const e of data.edges) {
    const layer = layerOfEdge(scope, e.kind);
    if (!layer || !layers.has(layer)) continue;
    if (e.kind === "term_edge" && filters.edgeTypes.length > 0) {
      if (!filters.edgeTypes.includes(e.label ?? "")) continue;
    }
    // REQ-2129: a column's upstream columns are drawn once its table is expanded to its columns;
    // until then the product's relation-level lineage stands for them.
    if (e.kind === "column_lineage") {
      const parent = byId.get(e.b)?.parent;
      if (!parent || !expanded.has(parent)) continue;
    }
    add(e);
  }
  const touched = new Set<string>();
  for (const e of edges.values()) touched.add(e.a).add(e.b);
  if (!tablesOn && isOn("product")) {
    for (const id of touched) {
      const n = byId.get(id);
      if (n?.type !== "column" || !n.parent) continue;
      for (const p of owners.get(n.parent) ?? []) add({ id: "", a: p, b: id, kind: "owns" });
    }
  }

  const drawn = new Set<string>();
  for (const n of data.nodes) {
    if (fold(n.id) !== n.id) continue;
    const layer = layerOfNode(scope, n.type);
    if (layer && layers.has(layer)) drawn.add(n.id);
  }
  for (const e of edges.values()) drawn.add(e.a).add(e.b);
  for (const id of [...drawn]) {
    const n = byId.get(id);
    if (!n || !passes(n, filters)) drawn.delete(id);
  }
  let kept = [...edges.values()].filter((e) => drawn.has(e.a) && drawn.has(e.b));
  const anyFilter = filters.domains.length + filters.tags.length + filters.sensitivities.length > 0;
  if (anyFilter) {
    // A node the filters do not judge (a tag, a check) stays only while it is joined to one kept.
    const linked = new Set<string>();
    for (const e of kept) linked.add(e.a).add(e.b);
    for (const id of [...drawn]) {
      const n = byId.get(id) as GraphNode;
      if (!linked.has(id) && !judged(n)) drawn.delete(id);
    }
  }
  if (filters.focus !== null && drawn.has(filters.focus)) {
    const reached = reach(filters.focus, kept, byId);
    for (const id of [...drawn]) if (!reached.has(id)) drawn.delete(id);
    kept = kept.filter((e) => drawn.has(e.a) && drawn.has(e.b));
  }
  return {
    nodes: data.nodes.filter((n) => drawn.has(n.id)),
    edges: kept,
  };
}

const judged = (n: GraphNode) =>
  (n.domains !== undefined && n.domains.length > 0) ||
  n.tags !== undefined ||
  n.sensitivities !== undefined;

/** Everything reached from ``start`` without passing through another product or a domain. */
function reach(start: string, edges: GraphEdge[], byId: Map<string, GraphNode>): Set<string> {
  const next = new Map<string, string[]>();
  for (const e of edges) {
    next.set(e.a, [...(next.get(e.a) ?? []), e.b]);
    next.set(e.b, [...(next.get(e.b) ?? []), e.a]);
  }
  const seen = new Set([start]);
  const stack = [start];
  while (stack.length > 0) {
    const id = stack.pop() as string;
    if (id !== start) {
      const t = byId.get(id)?.type;
      if (t === "product" || t === "domain") continue;
    }
    for (const m of next.get(id) ?? []) {
      if (!seen.has(m)) {
        seen.add(m);
        stack.push(m);
      }
    }
  }
  return seen;
}

/** Spring strength by edge kind: a product holds its parts closer than a lineage hop. */
const PULL: Record<EdgeKind, number> = {
  in_domain: 0.9,
  product_link: 0.3,
  member: 0.8,
  has_column: 1,
  command: 0.7,
  checks: 0.7,
  profiled: 0.7,
  tagged: 0.4,
  term_edge: 0.6,
  term_binding: 0.5,
  owns: 0.6,
  relationship: 0.5,
  relationship_outside: 0.3,
  lineage: 0.4,
  column_lineage: 0.4,
};

const LANE_GAP = 4;

const TREE_GAP = 3;
/** Wider across than down: a level's names sit side by side. */
const TREE_SPREAD = 5;

/** REQ-2130: a top-down tree over the term edges (``a`` is a kind or part of ``b``): each term a
 * level below the deepest term it is a kind or part of; a term in no edge on a row of its own. */
function treeLayout(g: GraphData): Map<string, [number, number, number]> {
  const links = g.edges
    .filter((e) => e.kind === "term_edge")
    .map((e) => [e.a, e.b] as [string, string]);
  const parents = new Set(links.map(([, b]) => b));
  const children = new Set(links.map(([a]) => a));
  const roots = [...parents].filter((id) => !children.has(id));
  const depth = lanesFrom(links, roots);
  const levels = new Map<number, string[]>();
  let deepest = 0;
  for (const n of g.nodes) {
    const d = depth.get(n.id);
    if (d === undefined) continue;
    deepest = Math.min(deepest, d);
    levels.set(d, [...(levels.get(d) ?? []), n.id]);
  }
  const loose = g.nodes.filter((n) => !depth.has(n.id)).map((n) => n.id);
  if (loose.length > 0) levels.set(deepest - 1, loose);
  // Centred on its middle level, so the camera's view of the whole tree is centred too.
  const all = [...levels.keys()];
  const mid = (Math.min(...all) + Math.max(...all)) / 2;
  const out = new Map<string, [number, number, number]>();
  for (const [d, members] of levels) {
    members.sort();
    members.forEach((id, i) =>
      out.set(id, [(i - (members.length - 1) / 2) * TREE_SPREAD, (d - mid) * TREE_GAP, 0]),
    );
  }
  return out;
}

/** Each drawn node's place. Lanes: lineage left to right, members (``zero``) in lane 0. */
export function layoutGraph(
  g: GraphData,
  layout: LayoutKind,
  zero: string[],
): Map<string, [number, number, number]> {
  const ids = g.nodes.map((n) => n.id);
  const radius = Math.max(6, Math.cbrt(ids.length) * 3);
  if (layout === "tree") return treeLayout(g);
  if (layout === "force") {
    return forceLayout(
      ids,
      g.edges.map((e) => ({ a: e.a, b: e.b, strength: PULL[e.kind] })),
      radius,
    );
  }
  const lane = lanesFrom(
    g.edges.filter((e) => e.kind === "lineage").map((e) => [e.a, e.b] as [string, string]),
    zero.filter((z) => ids.includes(z)),
  );
  const given = new Map(g.nodes.filter((n) => n.lane !== undefined).map((n) => [n.id, n.lane]));
  const lanes = new Map<number, string[]>();
  for (const id of ids) {
    const l = given.get(id) ?? lane.get(id) ?? 0;
    lanes.set(l, [...(lanes.get(l) ?? []), id]);
  }
  const all = [...lanes.keys()];
  const mid = (Math.min(...all) + Math.max(...all)) / 2;
  const out = new Map<string, [number, number, number]>();
  for (const [l, members] of lanes) {
    members.sort();
    const rows = Math.ceil(Math.sqrt(members.length));
    members.forEach((id, i) => {
      const r = i % rows;
      const c = Math.floor(i / rows);
      out.set(id, [(l - mid) * LANE_GAP, (r - (rows - 1) / 2) * 2, c * 2]);
    });
  }
  return out;
}

/** A node's drawn edges. */
export function edgesOf(g: GraphData, id: string): GraphEdge[] {
  return g.edges.filter((e) => e.a === id || e.b === id);
}

/** The detail panel's simplified graph: the product, its member tables, its commands and the
 * glossary terms bound to its columns (folded into their tables), and nothing else. */
export function panelGraph(data: GraphData, product: string): GraphData {
  const g = visibleGraph(
    data,
    new Set<LayerId>(["tables", "commands", "ontology"]),
    { ...NO_FILTERS, focus: product },
    new Set(),
    PRODUCT_SCOPE,
  );
  const edges = g.edges.filter((e) => e.kind !== "term_edge");
  const kept = new Set([product]);
  for (const e of edges) kept.add(e.a).add(e.b);
  // A checker is a member too, but it is quality evidence: the quality layer's, not the core's.
  for (const n of g.nodes) if (n.type === "checker" || n.type === "relation") kept.delete(n.id);
  return {
    nodes: g.nodes.filter((n) => kept.has(n.id)),
    edges: edges.filter((e) => kept.has(e.a) && kept.has(e.b)),
  };
}
