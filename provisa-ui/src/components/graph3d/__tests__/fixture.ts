// Copyright (c) 2026 Kenneth Stott
// Canary: 2e6b9d47-1c8a-4f3e-b5d2-7a0c4e9f1b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: a data product's graph, as the meta domain gives it, for the graph's tests.

import type { GraphData, GraphScope, LayerDef, LayerId, Preset } from "../graphModel";
import { NEIGHBOURHOOD } from "../neighbourhood";

// A product with member tables, a command, a checker, a profile run, a tag, glossary terms bound to
// member columns, a relationship inside and one outside, and upstream lineage.
export const DATA: GraphData = {
  nodes: [
    { id: "p:c360", type: "product", label: "customer_360", domains: ["sales"], tags: ["gold"] },
    { id: "p:billing", type: "product", label: "billing", domains: ["finance"], tags: [] },
    { id: "d:sales", type: "domain", label: "sales" },
    { id: "d:finance", type: "domain", label: "finance" },
    {
      id: "t:customers",
      type: "table",
      label: "sales.customers",
      domains: ["sales"],
      tags: ["gold"],
      sensitivities: ["pii"],
    },
    {
      id: "t:orders",
      type: "table",
      label: "sales.orders",
      domains: ["sales"],
      tags: [],
      sensitivities: [],
    },
    {
      id: "c:customers.email",
      type: "column",
      label: "email",
      parent: "t:customers",
      domains: ["sales"],
      sensitivities: ["pii"],
    },
    {
      id: "c:orders.total",
      type: "column",
      label: "total",
      parent: "t:orders",
      domains: ["sales"],
    },
    { id: "f:refresh", type: "command", label: "refresh_c360", domains: ["sales"] },
    { id: "k:orders_dq", type: "checker", label: "orders_dq" },
    { id: "r:orders_profile", type: "profile", label: "orders profile" },
    { id: "g:gold", type: "tag", label: "gold" },
    { id: "term:customer", type: "term", label: "Customer", domains: ["sales"] },
    { id: "term:party", type: "term", label: "Party", domains: [] },
    { id: "t:invoices", type: "relation", label: "finance.invoices", domains: ["finance"] },
    { id: "x:raw_orders", type: "relation", label: "raw.orders" },
  ],
  edges: [
    { id: "1", a: "p:c360", b: "d:sales", kind: "in_domain" },
    { id: "2", a: "p:billing", b: "d:finance", kind: "in_domain" },
    { id: "3", a: "p:c360", b: "p:billing", kind: "product_link", paths: 2 },
    { id: "4", a: "p:c360", b: "t:customers", kind: "member" },
    { id: "5", a: "p:c360", b: "t:orders", kind: "member" },
    { id: "6", a: "t:customers", b: "c:customers.email", kind: "has_column" },
    { id: "7", a: "t:orders", b: "c:orders.total", kind: "has_column" },
    { id: "8", a: "p:c360", b: "f:refresh", kind: "command" },
    { id: "9", a: "k:orders_dq", b: "t:orders", kind: "checks" },
    { id: "10", a: "r:orders_profile", b: "t:orders", kind: "profiled" },
    { id: "11", a: "g:gold", b: "p:c360", kind: "tagged" },
    { id: "12", a: "g:gold", b: "t:customers", kind: "tagged" },
    { id: "13", a: "term:customer", b: "c:customers.email", kind: "term_binding" },
    { id: "14", a: "term:customer", b: "term:party", kind: "term_edge", label: "KIND_OF" },
    { id: "15", a: "t:customers", b: "t:orders", kind: "relationship" },
    { id: "16", a: "t:orders", b: "t:invoices", kind: "relationship_outside" },
    { id: "17", a: "x:raw_orders", b: "t:orders", kind: "lineage" },
  ],
};

// REQ-2174: the same graph with what each node's hover card and detail panel say of it.
const INFO: Record<string, Partial<GraphData["nodes"][number]>> = {
  "term:customer": {
    definition: "A person or company that has bought from us.",
    info: { status: "live", stewards: ["ada"] },
  },
  "c:customers.email": { info: { dataType: "varchar", masked: true } },
  "t:customers": {
    info: { source: "sales_pg", description: "Every customer, one row each.", quality: "pass" },
  },
  "p:c360": { info: { status: "live", owner: "sales_steward" } },
};

export const FACTS: GraphData = {
  nodes: DATA.nodes.map((n) => ({ ...n, ...INFO[n.id] })),
  edges: DATA.edges,
};

// The engine (graphModel's visibleGraph) under a scope that uses every part of it: each kind of
// node and edge its own layer, columns folding into their tables. The ontology's scope
// (ONTOLOGY_SCOPE) is one such scope; this one exercises what that one leaves off.
const ENGINE_LAYERS = {
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

const ENGINE_LAYER_ORDER: LayerId[] = [
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

/** Named layer sets over the engine scope. */
export const ENGINE_PRESETS = {
  // Every layer but relationships to tables outside the product.
  all: { layers: ENGINE_LAYER_ORDER.filter((l) => l !== "outside") },
  // Terms, their edges, the columns bound to them and the products that own those columns.
  ontology: { layers: ["ontology", "products"] },
  // Every product, grouped by domain, joined by data_product_edges.
  products: { layers: ["products"] },
} satisfies Record<string, Preset>;

export const ENGINE_SCOPE: GraphScope = {
  layers: ENGINE_LAYERS,
  layerOrder: ENGINE_LAYER_ORDER,
  presets: { ontology: ENGINE_PRESETS.ontology },
  presetOrder: ["ontology"],
  hiddenStatuses: [],
  orphans: false,
  foldColumns: true,
  typedKinds: ["term_edge"],
  betweenDrawn: [],
  implicitLayers: [],
  menus: true,
  neighbourhood: NEIGHBOURHOOD,
};
