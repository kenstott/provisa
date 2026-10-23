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
import {
  flatTerms,
  ontologyCountQuery,
  ontologyQuery,
  termEdges,
  type TermRow,
} from "../glossaryGraphData";
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
          identityGap: scalar,
          domains: list("M__GlossaryTermDomains"),
          refs: list("M__GlossaryTermRefs"),
          edgesOut: list("M__GlossaryTermEdges"),
          edgesIn: list("M__GlossaryTermEdges"),
          tables: list("M__GlossaryTermTableRefs"),
        }),
        type("M__GlossaryTermTableRefs", { tableId: scalar, tableName: scalar, domainId: scalar }),
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
          sourceTerm: one("M__GlossaryTerms"),
          targetTerm: one("M__GlossaryTerms"),
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
  orphans: true,
  search: [],
  tags: [],
  sensitivities: [],
  ...over,
});
const squash = (q: string | null) => (q ?? "").replace(/\s+/g, " ");

describe("the Ontology View's query", () => {
  it("is one query with one root: the terms, each with the edges leaving it under Ontology", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology", "domains", "columns"])));
    expect(q.startsWith("query Ontology {")).toBe(true);
    expect(q).toContain("glossary_terms: m__glossaryTerms {");
    expect(q).toContain(
      "edges_out: edgesOut { id: id from_term_id: fromTermId to_term_id: toTermId rel_type: relType target_term: targetTerm { id: id } }",
    );
    // One root: the edges are the terms', not a root of their own.
    expect(q).not.toContain("m__glossaryTermEdges");
    expect(q.match(/m__\w+/g)).toEqual(["m__glossaryTerms"]);
    expect(squash(ontologyQuery(reader(), scope(["columns"])))).not.toContain("edgesOut {");
    expect(q).toContain("domains: domains { domain_id: domainId }");
    expect(q).toContain("refs: refs {");
    // Data off: a column's product is read for its look alone, and no products are read.
    expect(q).toContain("product: product { id: id }");
    expect(q).not.toContain("data_products");
  });

  it("reads whether each term's tables are all declared one entity", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"])));
    expect(q).toContain("identity_gap: identityGap");
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
      ontologyQuery(reader(), scope(["ontology"], { hiddenStatuses: ["retired", "draft"] })),
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
    expect(q).toContain('edges_out: edgesOut(where: { relType: { in: ["KIND_OF","PLACED_BY"] } })');
    expect(q).toContain(
      'sourceRelationships(where: { _or: [{ alias: { in: ["KIND_OF","PLACED_BY"] } }, { _and: [{ alias: { is_null: true } }, { id: { in: ["KIND_OF","PLACED_BY"] } }] }] })',
    );
  });

  it("keeps, with Orphans off, the terms with an edge kept to a term kept or a column they are", () => {
    const q = squash(
      ontologyQuery(
        reader(),
        scope(["ontology", "columns"], {
          orphans: false,
          edgeTypes: ["KIND_OF"],
          hiddenStatuses: ["retired"],
        }),
      ),
    );
    const kept = "{ retired: { eq: false } }";
    expect(q).toContain(
      `m__glossaryTerms(where: { _and: [${kept}, { _or: [` +
        `{ edgesOut: { _and: [{ relType: { in: ["KIND_OF"] } }, { targetTerm: ${kept} }] } }, ` +
        `{ edgesIn: { _and: [{ relType: { in: ["KIND_OF"] } }, { sourceTerm: ${kept} }] } }, ` +
        `{ tables: { } }, { refs: { } }] }] })`,
    );
    // With no layer drawing a relationship every term is an orphan: none is kept.
    const none = squash(ontologyQuery(reader(), scope(["domains"], { orphans: false })));
    expect(none).toContain("m__glossaryTerms(where: { _or: [] })");
  });

  it("reads the tables a class term grounds with the ontology, in the domains kept", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"], { domains: ["sales"] })));
    expect(q).toContain(
      'tables: tables(where: { domainId: { in: ["sales"] } }) { table_id: tableId table_name: tableName domain_id: domainId }',
    );
    expect(squash(ontologyQuery(reader(), scope(["domains"])))).not.toContain("tables:");
    // A class term is no orphan: its table is a relationship drawn.
    const orphans = squash(ontologyQuery(reader(), scope(["ontology"], { orphans: false })));
    expect(orphans).toContain("{ tables: { } }");
  });

  it("is null when the role cannot read the glossary", () => {
    const m = reader();
    m.roots.delete(rootKey("meta", "glossary_terms"));
    expect(ontologyQuery(m, scope(["ontology"]))).toBeNull();
  });
});

describe("REQ-2143: the Ontology View's query under a search", () => {
  const searched = (items: string[], over: Partial<QueryScope> = {}) =>
    squash(
      ontologyQuery(
        reader(),
        scope(["ontology", "domains", "columns"], { search: items, ...over }),
      ),
    );

  it("reads at its root the terms the search matches, by name or definition, any case", () => {
    const q = searched(["cust", "lost order"]);
    expect(q).toContain(
      "m__glossaryTerms(where: { _or: [" +
        '{ name: { ilike: "%cust%" } }, { definition: { ilike: "%cust%" } }, ' +
        '{ name: { ilike: "%lost order%" } }, { definition: { ilike: "%lost order%" } }] })',
    );
    // A pattern's own wildcards are the text's: 50% matches "50%", not "50" and anything.
    expect(searched(["50%_x"])).toContain('{ name: { ilike: "%50\\\\%\\\\_x%" } }');
  });

  it("reads each match's neighbourhood both ways, two hops deep and no further", () => {
    const q = searched(["cust"]);
    // A match and the terms a hop from it (by an edge leaving it or one reaching it) are read with
    // what they touch; the terms two hops off with their edges' ends alone.
    expect(q.match(/refs: refs/g)).toHaveLength(3);
    expect(q.match(/domains: domains/g)).toHaveLength(3);
    expect(q).toContain("edges_in: edgesIn");
    expect(q).toContain("source_term: sourceTerm");
    // The deepest level: an edge's end by its id.
    expect(q).toContain("target_term: targetTerm { id: id }");
    // Without a search: one level, the edges' ends by id, and no edges read the other way.
    const flat = squash(ontologyQuery(reader(), scope(["ontology", "domains", "columns"])));
    expect(flat.match(/refs: refs/g)).toHaveLength(1);
    expect(flat).not.toContain("edges_in: edgesIn");
  });

  it("counts what its root would read, by id with each term's domains", () => {
    const q = squash(ontologyCountQuery(reader(), scope(["ontology"], { search: ["cust"] })));
    expect(q).toContain("query OntologyCount");
    expect(q).toContain('{ name: { ilike: "%cust%" } }');
    expect(q).toContain("{ id: id domains: domains { domain_id: domainId } }");
  });
});

describe("REQ-2143: a nested read, flattened", () => {
  const term = (id: number, over: Partial<TermRow> = {}): TermRow => ({
    id,
    name: `t${id}`,
    definition: null,
    ref_count: 0,
    deprecated: false,
    retired: false,
    live: true,
    ...over,
  });

  it("holds each term once, at its fullest, and each edge once", () => {
    // 1 matched; 2 a hop off (read in full); 3 two hops off (its edge's end alone at 2's level).
    const two = term(2, {
      refs: [],
      edges_out: [
        { id: 9, from_term_id: 2, to_term_id: 3, rel_type: "KIND_OF", target_term: term(3) },
      ],
    });
    const rows = [
      term(1, {
        refs: [],
        edges_out: [
          { id: 8, from_term_id: 1, to_term_id: 2, rel_type: "PART_OF", target_term: two },
        ],
        edges_in: [
          { id: 8, from_term_id: 1, to_term_id: 2, rel_type: "PART_OF", source_term: { id: 1 } },
        ],
      }),
      // 2 read again at the root (a match too), with less.
      term(2),
    ];
    const terms = flatTerms(rows);
    expect(terms.map((t) => t.id).sort()).toEqual([1, 2, 3]);
    expect(terms.find((t) => t.id === 2)?.refs).toEqual([]);
    expect(
      termEdges(terms)
        .map((e) => e.id)
        .sort(),
    ).toEqual([8, 9]);
  });
});
