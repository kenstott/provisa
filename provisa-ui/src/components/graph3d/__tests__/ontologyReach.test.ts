// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2e8b17-3a94-4c6f-9e01-b7c4a2f8d953
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130 (amended 2026-10-29, THE ONTOLOGY VIEW DRAWS ONLY WHAT A TERM REACHES; AN ONTOLOGY VIEW,
// NOT A DATA-FLOW VIEW): the view's query, answered by a server holding a pet-store domain like the
// maintainer's -- terms bound to some columns of a few tables, data-quality checker tables in the
// same domain and product with no term bound -- and drawn. Only what a shown term reaches is drawn,
// and no edge between tables.

import { describe, expect, it } from "vitest";
import {
  RoleSchema,
  rootKey,
  type GraphqlType,
  type Introspection,
  type MetaReader,
} from "../../../api/metaGraph";
import { ontologyGraph, ontologyQuery, type OntologyRows } from "../glossaryGraphData";
import {
  GLOSSARY_SCOPE,
  NO_FILTERS,
  visibleGraph,
  type GraphData,
  type LayerId,
  type QueryScope,
} from "../graphModel";

const scalar: GraphqlType = { kind: "SCALAR", name: "String" };
const list = (name: string): GraphqlType => ({
  kind: "LIST",
  name: null,
  ofType: { kind: "NON_NULL", name: null, ofType: { kind: "OBJECT", name } },
});
const one = (name: string): GraphqlType => ({ kind: "OBJECT", name });
const type = (name: string, fields: Record<string, GraphqlType>) => ({
  name,
  fields: Object.entries(fields).map(([n, t]) => ({ name: n, type: t })),
});
const scalars = (...names: string[]) => Object.fromEntries(names.map((n) => [n, scalar]));

function reader(): MetaReader {
  const raw: Introspection = {
    __schema: {
      queryType: { name: "Query" },
      types: [
        type("Query", {
          on_terms: list("ON_Terms"),
          m__dataProducts: list("M__DataProducts"),
          m__registeredTables: list("M__RegisteredTables"),
        }),
        type("ON_Terms", {
          ...scalars("id", "name", "definition", "refCount", "deprecated", "retired", "status"),
          ...scalars("grounding", "termDomains", "identityGap"),
          domains: list("ON_Domains"),
          refs: list("ON_TermRefs"),
          edgesOut: list("ON_TermEdges"),
          tables: list("M__RegisteredTables"),
        }),
        type("ON_Domains", scalars("domainId")),
        type("ON_TermRefs", {
          ...scalars("columnKey", "columnName", "tableId", "tableName", "domainId"),
          registeredTable: one("M__RegisteredTables"),
        }),
        type("M__RegisteredTables", {
          ...scalars("id", "domainId", "tableName"),
          product: one("M__DataProducts"),
          tableColumns: list("M__TableColumns"),
          sourceRelationships: list("M__Relationships"),
        }),
        type("M__TableColumns", {
          ...scalars("columnName", "columnKey"),
          glossaryRef: list("ON_TermRefs"),
        }),
        type("M__Relationships", scalars("id", "cypherType", "targetTableId")),
        type("ON_TermEdges", {
          ...scalars("id", "fromTermId", "toTermId", "relType"),
          targetTerm: one("ON_Terms"),
        }),
        type("M__DataProducts", {
          ...scalars("id", "name", "domainId"),
          memberTables: list("M__RegisteredTables"),
        }),
      ],
    },
  };
  return {
    schema: new RoleSchema(raw),
    roots: new Map([
      [rootKey("ontology", "_terms"), "on_terms"],
      [rootKey("meta", "data_products"), "m__dataProducts"],
      [rootKey("meta", "registered_tables"), "m__registeredTables"],
    ]),
  };
}

// The pet-store domain: its tables with every column, and the product each is in.
const TABLES: Record<number, { name: string; columns: string[]; product: string }> = {
  10: { name: "pets", columns: ["id", "name", "status", "photo_urls"], product: "pet_store" },
  11: { name: "store_order", columns: ["id", "pet_id", "status"], product: "pet_store" },
  12: { name: "categories", columns: ["id", "name"], product: "catalog" },
  // Data-quality checker tables: in the domain and its products, no term bound to them.
  90: { name: "pets_quality_dq_checker_dq", columns: ["outcome", "run_at"], product: "pet_store" },
  91: { name: "vets_quality_dq_soda_dq", columns: ["outcome"], product: "vet_clinic" },
};
// Relationships declared between the tables: the ERD's, never the Ontology View's.
const RELATIONSHIPS: Record<number, { id: string; cypherType: string; targetTableId: number }[]> =
  {
    11: [{ id: "order_pet", cypherType: "PLACED_FOR", targetTableId: 10 }],
    90: [{ id: "dq_pets", cypherType: "CHECKS", targetTableId: 10 }],
  };
// Pet IS pets.id and store_order.pet_id; Status IS both status columns; Category IS the categories
// table (a class term); Companion grounds nothing, RELATED_TO Pet.
const TERMS = [
  { id: 1, name: "Pet", columns: ["10:id", "11:pet_id"], tables: [] as number[] },
  { id: 2, name: "Status", columns: ["10:status", "11:status"], tables: [] },
  { id: 3, name: "Category", columns: [], tables: [12] },
  { id: 4, name: "Companion", columns: [], tables: [] },
];
const EDGES = [{ id: 1, fromTermId: 4, toTermId: 1, relType: "RELATED_TO" }];

const product = (id: string, named: boolean) =>
  named ? { id, name: id, domainId: "sales" } : { id };

/** The server: answers ``query`` with what it selects and nothing else. */
function answer(query: string): OntologyRows & { data_products?: unknown[] } {
  const q = query.replace(/\s+/g, " ");
  const selected = (field: string) => new RegExp(`\\b${field}[ (]`).test(q);
  const named = q.includes("product { id name domainId");
  const rels = q.includes("sourceRelationships");
  const table = (id: number) => ({
    id,
    product: product(TABLES[id].product, named),
    ...(rels ? { sourceRelationships: RELATIONSHIPS[id] ?? [] } : {}),
  });
  const out: OntologyRows & { data_products?: unknown[] } = {
    _terms: TERMS.map((t) => ({
      id: t.id,
      name: t.name,
      definition: null,
      refCount: t.columns.length,
      deprecated: false,
      retired: false,
      status: "approved" as const,
      grounding: "grounded" as const,
      domains: selected("domains") ? [{ domainId: "sales" }] : undefined,
      edgesOut: selected("edgesOut")
        ? EDGES.filter((e) => e.fromTermId === t.id).map((e) => ({
            ...e,
            targetTerm: { id: e.toTermId },
          }))
        : undefined,
      tables: selected("tables")
        ? t.tables.map((id) => ({
            ...table(id),
            tableName: TABLES[id].name,
            domainId: "sales",
          }))
        : undefined,
      refs: selected("refs")
        ? t.columns.map((key) => {
            const [tid, column] = key.split(":");
            const id = Number(tid);
            return {
              columnKey: key,
              columnName: column,
              tableId: id,
              tableName: TABLES[id].name,
              domainId: "sales",
              registeredTable: table(id),
            };
          })
        : undefined,
    })),
  };
  if (q.includes("data_products"))
    out.data_products = [...new Set(Object.values(TABLES).map((t) => t.product))].map((p) => ({
      id: p,
      name: p,
      domainId: "sales",
      memberTables: Object.entries(TABLES)
        .filter(([, t]) => t.product === p)
        .map(([id, t]) => ({
          id: Number(id),
          domainId: "sales",
          tableName: t.name,
          tableColumns: t.columns.map((c) => ({
            columnName: c,
            columnKey: `${id}:${c}`,
            glossaryRef: TERMS.filter((x) => x.columns.includes(`${id}:${c}`)).map((x) => ({
              termId: x.id,
            })),
          })),
          ...(rels ? { sourceRelationships: RELATIONSHIPS[Number(id)] ?? [] } : {}),
        })),
    }));
  return out;
}

/** The view as drawn with ``layers`` on: its query run, its answer drawn. */
function drawn(layers: LayerId[]): GraphData {
  const scope: QueryScope = {
    layers: new Set(layers),
    domains: [],
    edgeTypes: [],
    hiddenStatuses: [],
    orphans: true,
    tags: [],
    sensitivities: [],
    search: [],
  };
  const query = ontologyQuery(reader(), scope);
  expect(query).not.toBeNull();
  return visibleGraph(
    ontologyGraph(answer(query!)),
    scope.layers,
    NO_FILTERS,
    new Set(),
    GLOSSARY_SCOPE,
  );
}

// Every layer the view has: whichever are on, nothing unreached is drawn.
const EVERY: LayerId[] = GLOSSARY_SCOPE.layerOrder;
const ids = (g: GraphData) => g.nodes.map((n) => n.id);

describe("the Ontology View draws only what a shown term reaches", () => {
  it("draws no data-quality checker table and no column a shown term does not ground", () => {
    const g = drawn(EVERY);
    for (const id of ["t:90", "t:91", "c:90:outcome", "c:90:run_at", "c:91:outcome"])
      expect(ids(g)).not.toContain(id);
    for (const id of ["c:10:name", "c:10:photo_urls", "c:11:id", "c:12:id", "c:12:name"])
      expect(ids(g)).not.toContain(id);
    // What the terms reach: the columns they are, each with its table, and the class term's table.
    expect(ids(g)).toEqual(
      expect.arrayContaining(["c:10:id", "c:11:pet_id", "c:10:status", "c:11:status"]),
    );
    expect(ids(g)).toEqual(expect.arrayContaining(["t:10", "t:11", "t:12"]));
    expect(g.edges).toContainEqual(
      expect.objectContaining({ kind: "has_column", a: "c:10:id", b: "t:10" }),
    );
  });

  it("draws a term linked only by RELATED_TO to a grounded term, with its edge", () => {
    const g = drawn(EVERY);
    expect(ids(g)).toContain("term:4");
    expect(g.edges).toContainEqual(
      expect.objectContaining({ kind: "term_edge", a: "term:4", b: "term:1", label: "RELATED_TO" }),
    );
  });

  it("draws no relationship edge between tables", () => {
    const g = drawn(EVERY);
    const tables = new Set(g.nodes.filter((n) => n.type === "table").map((n) => n.id));
    expect(g.edges.filter((e) => tables.has(e.a) && tables.has(e.b))).toEqual([]);
    expect(g.edges.filter((e) => e.kind === "relationship")).toEqual([]);
  });

  it("adds with Data only the reached tables' products, each joined to its table", () => {
    expect(drawn(["ontology", "domains", "columns"]).nodes.some((n) => n.type === "product")).toBe(
      false,
    );
    const g = drawn(EVERY);
    expect(
      g.nodes
        .filter((n) => n.type === "product")
        .map((n) => n.id)
        .sort(),
    ).toEqual(["p:catalog", "p:pet_store"]);
    expect(g.edges).toContainEqual(
      expect.objectContaining({ kind: "member", a: "p:pet_store", b: "t:10" }),
    );
    // A class term's table's product is reached through the table it grounds.
    expect(g.edges).toContainEqual(
      expect.objectContaining({ kind: "member", a: "p:catalog", b: "t:12" }),
    );
  });

  it("draws no product none of whose tables is reached", () => {
    expect(ids(drawn(EVERY))).not.toContain("p:vet_clinic");
  });

  it("reads everything nested under the terms: no data product read apart", () => {
    const q = ontologyQuery(reader(), {
      layers: new Set(EVERY),
      domains: [],
      edgeTypes: [],
      hiddenStatuses: [],
      orphans: true,
      tags: [],
      sensitivities: [],
      search: [],
    });
    expect(q).not.toContain("data_products");
    expect(q).not.toContain("sourceRelationships");
  });
});
