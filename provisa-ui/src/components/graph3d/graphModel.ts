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

import type { TermRole } from "../../api/glossary";
import { ONTOLOGY_NEIGHBOURHOOD, PRODUCT_NEIGHBOURHOOD, type Neighbourhood } from "./neighbourhood";
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
  | "data"
  | "columns"
  // The tables of the columns drawn, with the relationships between them (with Columns).
  | "ownerTables";

/** Force; lineage lanes left to right (REQ-1667); or a top-down tree (REQ-2130's Taxonomy). */
export type LayoutKind = "force" | "lanes" | "tree";

/** REQ-2130: a glossary term's standing (REQ-1387's admission rule decides proposed). */
export type TermStatus = "live" | "proposed" | "deprecated" | "retired";

/** Every standing a term can have, in the order a reader picks them. */
export const TERM_STATUSES: TermStatus[] = ["live", "proposed", "deprecated", "retired"];

export interface GraphNode {
  id: string;
  type: NodeType;
  label: string;
  /** The domains the node is in; a node with none (a tag, a checker's run) is not filtered by it. */
  domains?: string[];
  tags?: string[];
  /** The sensitive tags on the node (or on any of a table's columns). */
  sensitivities?: string[];
  /** The table a column or a profile belongs to: a column is drawn inside it until the table is
   * expanded; a profile is reached on its page. */
  parent?: string;
  /** REQ-1667: the lineage lane the node stands in, when the data says (upstream negative). */
  lane?: number;
  /** REQ-2130: a term's standing; deprecated is drawn dimmed. */
  status?: TermStatus;
  /** REQ-2130: a term's role -- Class (tables), Property (columns), Concept (neither) -- coloured
   * apart (nodeLook); absent where the graph does not read it. */
  role?: TermRole;
  /** REQ-2130/2141: a class grounded in tables not all joined as the same entity (a curation
   * gap): its tables, marked on its name and named in the side panel. */
  unlinkedTables?: string[];
  /** A column a data product holds (coloured apart from one it does not, nodeLook). */
  inProduct?: boolean;
  /** A term's definition: read in the side panel, never drawn on the graph. */
  definition?: string;
  /** A table drawn as a stored table, a cylinder (the Ontology View's), not as a product's
   * member, a box (nodeLook). */
  stored?: boolean;
  /** The layers that draw this node, where its type's own layer is not the whole answer: in the
   * Ontology View a data product's column is the Data layer's as well as Columns'. Empty: drawn
   * only as the end of an edge drawn (a table reached through its displayed columns). */
  layers?: LayerId[];
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
  /** The layers that draw this edge, where its kind's own layer is not the whole answer (see
   * GraphNode.layers). */
  layers?: LayerId[];
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
  /** Whether terms left with no relationship can be hidden (the Ontology View's Orphans). */
  orphans: boolean;
  /** Whether a column folds into its table while the table is drawn and not expanded. */
  foldColumns: boolean;
  /** The edge kinds the edge-type filter chooses among, by their labels. */
  typedKinds: EdgeKind[];
  /** Edge kinds drawn only between nodes something else draws: they join what is shown and never
   * bring a node in (the Ontology View's relationships between the tables of its columns). */
  betweenDrawn: EdgeKind[];
  /** Layers always drawn and offered by no control: the graph itself (the Ontology View's terms
   * and their edges). A remembered setting without one is read with it. */
  implicitLayers: LayerId[];
  /** The layers, and the terms by status with the unconnected ones, are chosen in dropdowns
   * beside the other filters, not in rows of chips. */
  menus: boolean;
  /** What a focus draws around each match (neighbourhood.ts). */
  neighbourhood: Neighbourhood;
}

export const PRODUCT_SCOPE: GraphScope = {
  layers: LAYERS,
  layerOrder: LAYER_ORDER,
  presets: PRESETS,
  presetOrder: PRESET_ORDER,
  hiddenStatuses: [],
  orphans: false,
  foldColumns: true,
  typedKinds: ["term_edge"],
  betweenDrawn: [],
  implicitLayers: [],
  menus: false,
  neighbourhood: PRODUCT_NEIGHBOURHOOD,
};

/** REQ-2130: the glossary's Ontology View. Terms and their edges; the domains each term is in
 * (an enterprise-wide term in every one, so it sits at the centre); the columns a concrete term
 * IS (its physical refs, as the viewer may read them), shown by default and toggled beside the
 * filters; with Columns, the tables owning the columns drawn and the relationships declared
 * between them (Tables); off by default, the data products with their datasets and those
 * datasets' columns (Data). A column is drawn as itself, never folded into its table. */
export const GLOSSARY_SCOPE: GraphScope = {
  layers: {
    ontology: { nodes: ["term"], edges: ["term_edge"] },
    domains: { nodes: ["domain"], edges: ["in_domain"] },
    data: { nodes: ["table", "product"], edges: ["has_column", "member"] },
    // Toggled beside the filters (Columns, and Tables with it), not in the Layers row.
    columns: { nodes: ["column"], edges: ["term_binding"] },
    ownerTables: { nodes: [], edges: ["relationship"] },
  },
  layerOrder: ["ontology", "domains", "data", "columns", "ownerTables"],
  presets: {
    // Every edge type, in a force layout, with the columns each term is.
    ontology: { layers: ["ontology", "domains", "columns"], layout: "force" },
    // The is-a and part-of hierarchy alone, as a tree from the top down.
    taxonomy: { layers: ["ontology"], layout: "tree", edgeTypes: ["KIND_OF", "PART_OF"] },
  },
  presetOrder: ["ontology", "taxonomy"],
  hiddenStatuses: ["retired"],
  orphans: true,
  foldColumns: false,
  typedKinds: ["term_edge", "relationship"],
  betweenDrawn: ["relationship"],
  implicitLayers: ["ontology"],
  menus: true,
  neighbourhood: ONTOLOGY_NEIGHBOURHOOD,
};

export interface GraphFilters {
  domains: string[];
  tags: string[];
  sensitivities: string[];
  /** Products and terms: only what is reached from one of them is drawn; none, the whole graph. */
  focus: string[];
  /** REQ-2130: the term edge types drawn; every type when empty. */
  edgeTypes: string[];
  /** REQ-2130: the term statuses not drawn. */
  hiddenStatuses: TermStatus[];
}

/** REQ-2136: a view whose layers and filters run in its query, not after it: what the query reads.
 * No domains, or no edge types, is every one. */
export interface QueryScope {
  layers: ReadonlySet<LayerId>;
  domains: string[];
  edgeTypes: string[];
  hiddenStatuses: TermStatus[];
  /** The product graph's tag and sensitivity filters. */
  tags: string[];
  sensitivities: string[];
  /** Whether a term with no relationship drawn (no edge of the types kept to a term kept, and no
   * column it IS under Columns) is drawn: the Ontology View's Orphans. */
  orphans: boolean;
  /** The page's search, as searchItems splits it: the terms it matches and their neighbourhood
   * (neighbourhood.ts) are read; none, the whole graph. */
  search: string[];
}

export const NO_FILTERS: GraphFilters = {
  domains: [],
  tags: [],
  sensitivities: [],
  focus: [],
  edgeTypes: [],
  hiddenStatuses: [],
};

function layerOfEdge(scope: GraphScope, kind: EdgeKind): LayerId | undefined {
  return scope.layerOrder.find((l) => scope.layers[l]?.edges.includes(kind));
}

function layerOfNode(scope: GraphScope, type: NodeType): LayerId | undefined {
  return scope.layerOrder.find((l) => scope.layers[l]?.nodes.includes(type));
}

/** The layers that draw ``e``: its own, else its kind's. */
function edgeLayers(scope: GraphScope, e: GraphEdge): LayerId[] {
  if (e.layers) return e.layers;
  const l = layerOfEdge(scope, e.kind);
  return l ? [l] : [];
}

function nodeLayers(scope: GraphScope, n: GraphNode): LayerId[] {
  if (n.layers) return n.layers;
  const l = layerOfNode(scope, n.type);
  return l ? [l] : [];
}

/** The layers ``data`` has anything in; a layer the user's role cannot read has nothing. */
export function availableLayers(data: GraphData, scope: GraphScope): Set<LayerId> {
  const out = new Set<LayerId>();
  for (const e of data.edges) for (const l of edgeLayers(scope, e)) out.add(l);
  for (const n of data.nodes) for (const l of nodeLayers(scope, n)) out.add(l);
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
    if (
      scope.foldColumns &&
      n?.type === "column" &&
      n.parent &&
      tablesOn &&
      !expanded.has(n.parent)
    )
      return n.parent;
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
    if (!edgeLayers(scope, e).some((l) => layers.has(l))) continue;
    if (scope.betweenDrawn.includes(e.kind)) continue;
    if (scope.typedKinds.includes(e.kind) && filters.edgeTypes.length > 0) {
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
    if (nodeLayers(scope, n).some((l) => layers.has(l))) drawn.add(n.id);
  }
  for (const e of edges.values()) drawn.add(e.a).add(e.b);
  for (const id of [...drawn]) {
    const n = byId.get(id);
    if (!n || !passes(n, filters)) drawn.delete(id);
  }
  for (const e of data.edges) {
    if (!scope.betweenDrawn.includes(e.kind)) continue;
    if (!edgeLayers(scope, e).some((l) => layers.has(l))) continue;
    if (filters.edgeTypes.length > 0 && !filters.edgeTypes.includes(e.label ?? "")) continue;
    if (drawn.has(fold(e.a)) && drawn.has(fold(e.b))) add(e);
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
  const anchors = filters.focus.filter((f) => drawn.has(f));
  if (anchors.length > 0) {
    // The union of each focus's neighbourhood.
    const reached = new Set<string>();
    for (const a of anchors)
      for (const id of reach(a, kept, byId, scope.neighbourhood)) reached.add(id);
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

/** What is reached from ``start`` within ``rule.depth`` hops, passing through no product or domain
 * (``rule.stopAt``). A table is an end too -- drawn where a reached column, term or table touches
 * it, its other columns and relationships not followed -- unless it is the start or entered by a
 * member edge (a member of the start product). */
export function reach(
  start: string,
  edges: GraphEdge[],
  byId: Map<string, GraphNode>,
  rule: Neighbourhood,
): Set<string> {
  const next = new Map<string, [string, EdgeKind][]>();
  for (const e of edges) {
    next.set(e.a, [...(next.get(e.a) ?? []), [e.b, e.kind]]);
    next.set(e.b, [...(next.get(e.b) ?? []), [e.a, e.kind]]);
  }
  const seen = new Set([start]);
  // Breadth first, so each node is met at its fewest hops.
  let frontier: [string, EdgeKind | null][] = [[start, null]];
  for (let hop = 0; hop < rule.depth && frontier.length > 0; hop++) {
    const out: [string, EdgeKind | null][] = [];
    for (const [id, via] of frontier) {
      if (id !== start) {
        const t = byId.get(id)?.type;
        if (t !== undefined && rule.stopAt.includes(t)) continue;
        if (t === "table" && via !== rule.tableEnteredBy) continue;
      }
      for (const [m, kind] of next.get(id) ?? []) {
        if (!seen.has(m)) {
          seen.add(m);
          out.push([m, kind]);
        }
      }
    }
    frontier = out;
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

/** The term edges that set a tree's levels: ``a`` is a kind or a part of ``b``. */
const HIERARCHY = new Set(["KIND_OF", "PART_OF"]);

/** REQ-2130: a top-down tree over the term edges -- the general above the specific, the whole
 * above the part. Only KIND_OF and PART_OF set the levels: a term sits a level below the deepest
 * term it is a kind or part of, under the first of those; a term in no such edge is a root, the
 * roots side by side. A node joined to a term by any other edge (a column the term IS) hangs a
 * level below that term, under it; any other node is a root too. */
function treeLayout(g: GraphData): Map<string, [number, number, number]> {
  const terms = new Set(g.nodes.filter((n) => n.type === "term").map((n) => n.id));
  const up = new Map<string, string[]>();
  for (const e of g.edges)
    if (e.kind === "term_edge" && HIERARCHY.has(e.label ?? "") && e.a !== e.b)
      up.set(e.a, [...(up.get(e.a) ?? []), e.b].sort());
  const depth = new Map<string, number>();
  const visiting = new Set<string>();
  // The longest path up to a root; an edge closing a cycle counts for nothing.
  const depthOf = (id: string): number => {
    const known = depth.get(id);
    if (known !== undefined) return known;
    visiting.add(id);
    let d = 0;
    for (const p of up.get(id) ?? []) if (!visiting.has(p)) d = Math.max(d, depthOf(p) + 1);
    visiting.delete(id);
    depth.set(id, d);
    return d;
  };
  const parent = new Map<string, string>();
  for (const id of [...terms].sort()) {
    const d = depthOf(id);
    const under = (up.get(id) ?? []).find((p) => depthOf(p) === d - 1);
    if (under !== undefined) parent.set(id, under);
  }
  for (const e of [...g.edges].sort((x, y) => x.id.localeCompare(y.id))) {
    const [term, other] = terms.has(e.a) ? [e.a, e.b] : [e.b, e.a];
    if (!terms.has(term) || terms.has(other) || parent.has(other)) continue;
    parent.set(other, term);
    depth.set(other, depthOf(term) + 1);
  }
  const children = new Map<string, string[]>();
  for (const [child, p] of parent) children.set(p, [...(children.get(p) ?? []), child]);
  const x = new Map<string, number>();
  let next = 0;
  // Leaves side by side; a node centred over its children.
  const place = (id: string): number => {
    const kids = (children.get(id) ?? []).sort();
    const under = kids.map(place);
    const at = under.length === 0 ? next++ : (under[0] + under[under.length - 1]) / 2;
    x.set(id, at);
    return at;
  };
  const ids = g.nodes.map((n) => n.id).sort();
  for (const id of ids) if (!parent.has(id)) place(id);
  const deepest = Math.max(0, ...ids.map((id) => depth.get(id) ?? 0));
  // Centred, so the camera's view of the whole tree is centred too.
  const mid = (next - 1) / 2;
  return new Map(
    ids.map((id) => [
      id,
      [
        ((x.get(id) as number) - mid) * TREE_SPREAD,
        (deepest / 2 - (depth.get(id) ?? 0)) * TREE_GAP,
        0,
      ],
    ]),
  );
}

/** Each drawn node's place. Lanes: lineage left to right, members (``zero``) in lane 0. */
export function layoutGraph(
  g: GraphData,
  layout: LayoutKind,
  zero: string[],
  /** How close a force layout draws the nodes (1 the usual spacing); other layouts are fixed. */
  attraction = 1,
): Map<string, [number, number, number]> {
  const ids = g.nodes.map((n) => n.id);
  const radius = Math.max(6, Math.cbrt(ids.length) * 3);
  if (layout === "tree") return treeLayout(g);
  if (layout === "force") {
    return forceLayout(
      ids,
      g.edges.map((e) => ({ a: e.a, b: e.b, strength: PULL[e.kind] })),
      radius,
      attraction,
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
    { ...NO_FILTERS, focus: [product] },
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
