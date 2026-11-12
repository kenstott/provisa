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
// a small three-level graph whose best order is found by trying every one -- and the lane diagram
// placed as the lanes preset places it.

import type { GraphData, GraphEdge, GraphNode } from "../graphModel";
import { LANES, type At, type LaneDiagram } from "../laneDiagram";
import { LANE_GAP, placeLanes, sortLanes, type LaneLine } from "../../provisa-graph/lanes";
import { SIZE } from "../../provisa-graph/tokens";

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
  pet: [
    "pets.id",
    "find_pets_by_status.id",
    "find_pets_by_tags.id",
    "pet_photos.pet_id",
    "store_order.pet_id",
  ],
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
      nodes.push({
        id: columnId(`${t}.${c}`),
        type: "column",
        label: `${t}.${c}`,
        parent: tableId(t),
      });
  }
  nodes.push({ id: "p:pet_store", type: "product", label: "pet_store" });
  for (const [a, b, label] of RANKS)
    edges.push({ id: `term_edge:${a}>${b}`, a: termId(a), b: termId(b), kind: "term_edge", label });
  for (const [t, cols] of Object.entries(BINDINGS))
    for (const c of cols)
      edges.push({
        id: `term_binding:${t}>${c}`,
        a: termId(t),
        b: columnId(c),
        kind: "term_binding",
      });
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
    ["a2", "b0"],
    ["a1", "b1"],
    ["a2", "b2"],
    ["a3", "b3"],
    ["a2", "b4"],
    ["b2", "c0"],
    ["b4", "c1"],
    ["b4", "c2"],
    ["b0", "c3"],
    ["b1", "c0"],
    ["b0", "c1"],
    ["a0", "b3"],
    ["a3", "b0"],
    ["b0", "c0"],
    ["a2", "c2"],
  ];
  const ids = [...new Set(pairs.flat())].sort();
  return {
    nodes: ids.map((id) => ({ id, type: "term", label: id })),
    edges: pairs.map(([a, b]) => ({ id: `lineage:${a}>${b}`, a, b, kind: "lineage" })),
  };
}

/** Where the lanes preset draws ``d`` (sortLanes, then placeLanes), every node ``w`` wide and as tall
 * as the graph draws a node: the lines of the term lane laid over it (its arcs) place nothing. */
export function placedDiagram(d: LaneDiagram, w = 160): (id: string) => At {
  const lines: LaneLine[] = [...d.links, ...d.arcs].map((k) => ({
    id: k.id,
    source: k.from,
    target: k.to,
  }));
  const ids = [...d.graph.nodes.map((n) => n.id), ...d.more.map((m) => m.id)];
  const laneOf = (id: string) => (d.more.some((m) => m.id === id) ? "column" : d.lane.get(id));
  const lanes = LANES.map((l) => ({
    id: l as string,
    items: ids.filter((id) => laneOf(id) === l).map((id) => ({ id, w, h: SIZE.nodeHeight })),
  }));
  const overlay = new Set(d.arcs.map((k) => k.id));
  const where = placeLanes(
    sortLanes(lanes, lines, "term", LANE_GAP, overlay),
    lines,
    "term",
    LANE_GAP,
    overlay,
  );
  return (id: string) => where.get(id) as At;
}

/** J1206: a Pet Health ontology as the maintainer's 2026-11-11 screenshots drew it -- hub terms
 * bound across many tables (Pet, Employee, Name), a table whose name has no space in it
 * (shelter.assignments_by_employee), three products sharing tables. */
const PH_TABLES: Record<string, string[]> = {
  "shelter.animals": ["id", "species", "name", "shelter_id", "intake_date", "weight_kg"],
  "shelter.assignments_by_employee": ["employee_id", "animal_id", "shelter_id", "assigned_on"],
  "shelter.shelters": ["id", "name", "city", "capacity"],
  "hr.employees": ["id", "name", "role", "hired_on"],
  "clinic.visits": ["id", "animal_id", "vet_id", "visited_on", "diagnosis_code"],
  "clinic.vaccinations": ["id", "animal_id", "vaccine", "given_on"],
  "clinic.prescriptions": ["id", "visit_id", "medication", "dose"],
  "clinic.diagnoses": ["code", "description"],
  "adoption.adoptions": ["id", "animal_id", "owner_id", "adopted_on", "fee"],
  "adoption.owners": ["id", "name", "email", "phone"],
};

const PH_BINDINGS: Record<string, string[]> = {
  pet: [
    "shelter.animals.id",
    "shelter.assignments_by_employee.animal_id",
    "clinic.visits.animal_id",
    "clinic.vaccinations.animal_id",
    "adoption.adoptions.animal_id",
  ],
  dog: ["shelter.animals.species"],
  cat: ["shelter.animals.species"],
  owner: ["adoption.owners.id", "adoption.adoptions.owner_id"],
  employee: [
    "hr.employees.id",
    "shelter.assignments_by_employee.employee_id",
    "clinic.visits.vet_id",
  ],
  veterinarian: ["clinic.visits.vet_id"],
  shelter: [
    "shelter.shelters.id",
    "shelter.animals.shelter_id",
    "shelter.assignments_by_employee.shelter_id",
  ],
  visit: ["clinic.visits.id", "clinic.prescriptions.visit_id"],
  vaccination: ["clinic.vaccinations.id", "clinic.vaccinations.vaccine"],
  diagnosis: ["clinic.diagnoses.code", "clinic.visits.diagnosis_code"],
  medication: ["clinic.prescriptions.medication"],
  adoption: ["adoption.adoptions.id", "adoption.adoptions.fee"],
  assignment: ["shelter.assignments_by_employee.assigned_on"],
  name: [
    "shelter.animals.name",
    "hr.employees.name",
    "adoption.owners.name",
    "shelter.shelters.name",
  ],
  contact: ["adoption.owners.email", "adoption.owners.phone"],
};

const PH_RANKS: [string, string, string][] = [
  ["dog", "pet", "KIND_OF"],
  ["cat", "pet", "KIND_OF"],
  ["veterinarian", "employee", "KIND_OF"],
  ["vaccination", "visit", "KIND_OF"],
  ["diagnosis", "visit", "PART_OF"],
  ["medication", "visit", "PART_OF"],
];

const PH_PRODUCTS: Record<string, string[]> = {
  pet_health: [
    "shelter.animals",
    "clinic.visits",
    "clinic.vaccinations",
    "clinic.prescriptions",
    "clinic.diagnoses",
  ],
  shelter_ops: [
    "shelter.shelters",
    "shelter.animals",
    "shelter.assignments_by_employee",
    "hr.employees",
  ],
  adoptions: ["adoption.adoptions", "adoption.owners", "shelter.animals"],
};

/** The Pet Health ontology, every term with its tables and columns (the term graph at 3 hops). */
export function petHealthOntology(): GraphData {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  for (const t of Object.keys(PH_BINDINGS)) nodes.push({ id: termId(t), type: "term", label: t });
  for (const [t, cols] of Object.entries(PH_TABLES)) {
    nodes.push({ id: tableId(t), type: "table", label: t });
    for (const c of cols)
      nodes.push({
        id: columnId(`${t}.${c}`),
        type: "column",
        label: `${t}.${c}`,
        parent: tableId(t),
      });
  }
  for (const p of Object.keys(PH_PRODUCTS)) nodes.push({ id: `p:${p}`, type: "product", label: p });
  for (const [a, b, label] of PH_RANKS)
    edges.push({ id: `term_edge:${a}>${b}`, a: termId(a), b: termId(b), kind: "term_edge", label });
  for (const [t, cols] of Object.entries(PH_BINDINGS))
    for (const c of cols)
      edges.push({
        id: `term_binding:${t}>${c}`,
        a: termId(t),
        b: columnId(c),
        kind: "term_binding",
      });
  for (const [p, tables] of Object.entries(PH_PRODUCTS))
    for (const t of tables)
      edges.push({ id: `member:${p}>${t}`, a: `p:${p}`, b: tableId(t), kind: "member" });
  return { nodes, edges };
}
