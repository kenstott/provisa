// Copyright (c) 2026 Kenneth Stott
// Canary: 2680046b-6ba9-4efe-adb0-e38e587c2fc9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136: the Ontology View's one query. Its layers are what it selects and its domain, edge type
// and status filters are its where, so the query run returns what the view draws.

import { describe, expect, it } from "vitest";
import {
  RoleSchema,
  rootKey,
  type GraphqlType,
  type Introspection,
  type MetaReader,
} from "../../../api/metaGraph";
import { ontologyQuery } from "../glossaryGraphData";
import type { LayerId, QueryScope } from "../graphModel";

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

/** The glossary's meta views as the compiler names them: domain alias "m", camelCase columns. */
function reader(): MetaReader {
  const raw: Introspection = {
    __schema: {
      queryType: { name: "Query" },
      types: [
        type("Query", {
          m__glossaryTerms: list("M__GlossaryTerms"),
          m__glossaryTermEdges: list("M__GlossaryTermEdges"),
          m__dataProducts: list("M__DataProducts"),
          m__registeredTables: list("M__RegisteredTables"),
          m__tableColumns: list("M__TableColumns"),
        }),
        type("M__GlossaryTerms", {
          id: scalar,
          name: scalar,
          definition: scalar,
          isAbstract: scalar,
          deprecated: scalar,
          retired: scalar,
          live: scalar,
          termDomains: scalar,
          domains: list("M__GlossaryTermDomains"),
          refs: list("M__GlossaryTermRefs"),
        }),
        type("M__GlossaryTermDomains", { domainId: scalar }),
        type("M__GlossaryTermRefs", {
          termId: scalar,
          columnKey: scalar,
          columnName: scalar,
          tableId: scalar,
          tableName: scalar,
          domainId: scalar,
          registeredTable: one("M__RegisteredTables"),
        }),
        type("M__RegisteredTables", {
          id: scalar,
          domainId: scalar,
          tableName: scalar,
          product: one("M__DataProducts"),
          tableColumns: list("M__TableColumns"),
          sourceRelationships: list("M__Relationships"),
        }),
        type("M__Relationships", { id: scalar, alias: scalar, targetTableId: scalar }),
        type("M__TableColumns", {
          columnName: scalar,
          columnKey: scalar,
          glossaryRef: list("M__GlossaryTermRefs"),
        }),
        type("M__GlossaryTermEdges", {
          id: scalar,
          fromTermId: scalar,
          toTermId: scalar,
          relType: scalar,
        }),
        type("M__DataProducts", {
          id: scalar,
          name: scalar,
          domainId: scalar,
          memberTables: list("M__RegisteredTables"),
        }),
      ],
    },
  };
  return {
    schema: new RoleSchema(raw),
    roots: new Map([
      [rootKey("meta", "glossary_terms"), "m__glossaryTerms"],
      [rootKey("meta", "glossary_term_edges"), "m__glossaryTermEdges"],
      [rootKey("meta", "data_products"), "m__dataProducts"],
      [rootKey("meta", "registered_tables"), "m__registeredTables"],
      [rootKey("meta", "table_columns"), "m__tableColumns"],
    ]),
  };
}

const scope = (layers: LayerId[], over: Partial<QueryScope> = {}): QueryScope => ({
  layers: new Set(layers),
  domains: [],
  edgeTypes: [],
  hiddenStatuses: [],
  ...over,
});
const squash = (q: string | null) => (q ?? "").replace(/\s+/g, " ");

describe("the Ontology View's query", () => {
  it("is one query reading the terms and, under Ontology, the edges between them", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology", "domains", "columns"])));
    expect(q.startsWith("query Ontology {")).toBe(true);
    expect(q).toContain("glossary_terms: m__glossaryTerms {");
    expect(q).toContain(
      "glossary_term_edges: m__glossaryTermEdges { id: id from_term_id: fromTermId",
    );
    expect(q).toContain("domains: domains { domain_id: domainId }");
    expect(q).toContain("refs: refs {");
    // Data off: a column's product is read for its look alone, and no products are read.
    expect(q).toContain("product: product { id: id }");
    expect(q).not.toContain("data_products");
  });

  it("selects only what its layers draw", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"])));
    expect(q).not.toContain("domains:");
    expect(q).not.toContain("refs:");
    const data = squash(ontologyQuery(reader(), scope(["ontology", "data", "columns"])));
    expect(data).toContain("product: product { id: id name: name domain_id: domainId }");
    expect(data).toContain("data_products: m__dataProducts {");
    expect(data).toContain("glossary_ref: glossaryRef { term_id: termId }");
    // Tables, with Columns: the relationships declared from the tables drawn.
    const tables = squash(
      ontologyQuery(reader(), scope(["ontology", "data", "columns", "ownerTables"])),
    );
    expect(tables).toContain("source_relationships: sourceRelationships {");
    expect(data).not.toContain("source_relationships");
  });

  it("leaves the hidden statuses out in its where", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"], { hiddenStatuses: ["retired"] })));
    expect(q).toContain("m__glossaryTerms(where: { retired: { eq: false } })");
    const two = squash(
      ontologyQuery(reader(), scope(["ontology"], { hiddenStatuses: ["retired", "proposed"] })),
    );
    expect(two).toContain(
      "m__glossaryTerms(where: { _and: [{ retired: { eq: false } }, { _or: [{ retired: { eq: true } }, { deprecated: { eq: true } }, { live: { eq: true } }] }] })",
    );
  });

  it("keeps the chosen domains, and the enterprise-wide terms, in its where", () => {
    const q = squash(
      ontologyQuery(reader(), scope(["ontology", "domains", "columns"], { domains: ["sales"] })),
    );
    expect(q).toContain(
      'm__glossaryTerms(where: { _or: [{ termDomains: { like: "%|sales|%" } }, { termDomains: { like: "%|*|%" } }] })',
    );
    expect(q).toContain('domains: domains(where: { domainId: { in: ["sales","*"] } })');
    expect(q).toContain('refs: refs(where: { domainId: { in: ["sales"] } })');
  });

  it("keeps the chosen edge types in its where, of the term edges and the relationships", () => {
    const q = squash(
      ontologyQuery(
        reader(),
        scope(["ontology", "columns", "ownerTables"], { edgeTypes: ["KIND_OF", "PLACED_BY"] }),
      ),
    );
    expect(q).toContain(
      'm__glossaryTermEdges(where: { relType: { in: ["KIND_OF","PLACED_BY"] } })',
    );
    expect(q).toContain(
      'sourceRelationships(where: { _or: [{ alias: { in: ["KIND_OF","PLACED_BY"] } }, { _and: [{ alias: { is_null: true } }, { id: { in: ["KIND_OF","PLACED_BY"] } }] }] })',
    );
  });

  it("is null when the role cannot read the glossary", () => {
    const m = reader();
    m.roots.delete(rootKey("meta", "glossary_terms"));
    expect(ontologyQuery(m, scope(["ontology"]))).toBeNull();
  });
});
