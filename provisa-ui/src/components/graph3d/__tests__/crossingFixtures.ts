// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7e4b91-6c3a-4f08-9e15-b8a0c47d3e62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: graphs for the crossing-minimisation tests -- the pet-store Ontology graph (17 terms, 32
// bound columns, 12 tables, one data product), the same shape as flows for the layered layout, and
// a small three-level graph whose best order is found by trying every one.

import type { GraphData, GraphEdge, GraphNode } from "../graphModel";

/** The pet-store tables and their columns, as a page reads them. */
const TABLES: Record<string, string[]> = {
  pets: ["id", "name", "status", "category_id", "photo_urls"],
  find_pets_by_status: ["id", "name", "status"],
  find_pets_by_tags: ["id", "name", "tags"],
  pet_photos: ["pet_id", "photo_urls"],
  categories: ["id", "name"],
  tags: ["id", "name"],
  store_order: ["id", "pet_id", "quantity", "status", "ship_date"],
  inventory: ["status", "quantity"],
  users: ["id", "username", "email"],
  user_login: ["username"],
  api_response: ["code", "message"],
  customers: ["id", "username"],
};

/** Each term and the columns (``table.column``) bound to it. */
const BINDINGS: Record<string, string[]> = {
  pet: ["pets.id", "find_pets_by_status.id", "find_pets_by_tags.id", "pet_photos.pet_id", "store_order.pet_id"],
  dog: ["find_pets_by_tags.tags"],
  cat: ["find_pets_by_tags.name"],
  bird: ["find_pets_by_status.name"],
  status: ["pets.status", "find_pets_by_status.status", "store_order.status", "inventory.status"],
  available: ["inventory.quantity"],
  pending: ["store_order.quantity"],
  sold: ["api_response.code"],
  category: ["pets.category_id", "categories.id", "categories.name"],
  tag: ["tags.id", "tags.name"],
  order: ["store_order.id", "store_order.ship_date"],
  user: ["users.id", "users.username", "user_login.username"],
  customer: ["customers.id", "customers.username"],
  identifier: ["users.email"],
  name: ["pets.name"],
  photo: ["pet_photos.photo_urls", "pets.photo_urls"],
  message: ["api_response.message"],
};

/** Narrower term, broader term, edge type. */
const RANKS: [string, string, string][] = [
  ["dog", "pet", "KIND_OF"],
  ["cat", "pet", "KIND_OF"],
  ["bird", "pet", "KIND_OF"],
  ["available", "status", "VALID_VALUE_OF"],
  ["pending", "status", "VALID_VALUE_OF"],
  ["sold", "status", "VALID_VALUE_OF"],
  ["customer", "user", "KIND_OF"],
];

const PRODUCT_TABLES = ["pets", "find_pets_by_status", "store_order", "inventory", "categories"];

const termId = (t: string) => `term:${t}`;
const tableId = (t: string) => `t:${t}`;
const columnId = (c: string) => `c:${c}`;

/** The pet-store Ontology graph: what the 2D lane diagram draws. */
export function petStoreOntology(): GraphData {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  for (const t of Object.keys(BINDINGS)) nodes.push({ id: termId(t), type: "term", label: t });
  for (const [t, cols] of Object.entries(TABLES)) {
    nodes.push({ id: tableId(t), type: "table", label: t });
    for (const c of cols)
      nodes.push({ id: columnId(`${t}.${c}`), type: "column", label: `${t}.${c}`, parent: tableId(t) });
  }
  nodes.push({ id: "p:pet_store", type: "product", label: "pet_store" });
  for (const [a, b, label] of RANKS)
    edges.push({ id: `term_edge:${a}>${b}`, a: termId(a), b: termId(b), kind: "term_edge", label });
  for (const [t, cols] of Object.entries(BINDINGS))
    for (const c of cols)
      edges.push({ id: `term_binding:${t}>${c}`, a: termId(t), b: columnId(c), kind: "term_binding" });
  for (const t of PRODUCT_TABLES)
    edges.push({ id: `member:${t}`, a: "p:pet_store", b: tableId(t), kind: "member" });
  return { nodes, edges };
}

/** The pet-store graph's lanes as flows (term, column, table, product): the layered layout puts
 * them on four levels, a term bound straight to nothing but columns. */
export function petStoreFlows(): GraphData {
  const g = petStoreOntology();
  const nodes = g.nodes.map((n) => ({ ...n, parent: undefined }));
  const edges: GraphEdge[] = [];
  const flow = (a: string, b: string) =>
    edges.push({ id: `lineage:${a}>${b}`, a, b, kind: "lineage" });
  for (const e of g.edges) {
    if (e.kind === "term_binding") flow(e.a, e.b);
    if (e.kind === "member") flow(e.b, e.a);
  }
  for (const n of g.nodes) if (n.type === "column") flow(n.id, n.parent as string);
  return { nodes, edges };
}

/** A three-level graph (4, 5 and 4 nodes) of flows: every order of it is tried in the tests. */
export function threeLevels(): GraphData {
  const pairs: [string, string][] = [
    ["a2", "b0"], ["a1", "b1"], ["a2", "b2"], ["a3", "b3"], ["a2", "b4"], ["b2", "c0"],
    ["b4", "c1"], ["b4", "c2"], ["b0", "c3"], ["b1", "c0"], ["b0", "c1"], ["a0", "b3"],
    ["a3", "b0"], ["b0", "c0"], ["a2", "c2"],
  ];
  const ids = [...new Set(pairs.flat())].sort();
  return {
    nodes: ids.map((id) => ({ id, type: "term", label: id })),
    edges: pairs.map(([a, b]) => ({ id: `lineage:${a}>${b}`, a, b, kind: "lineage" })),
  };
}
