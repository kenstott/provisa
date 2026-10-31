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
  ontologyGraph,
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
          on_terms: list("ON_Terms"),
          on_termEdges: list("ON_TermEdges"),
          m__dataProducts: list("M__DataProducts"),
          m__registeredTables: list("M__RegisteredTables"),
          m__tableColumns: list("M__TableColumns"),
        }),
        type("ON_Terms", {
          id: scalar,
          name: scalar,
          definition: scalar,
          isAbstract: scalar,
          deprecated: scalar,
          retired: scalar,
          status: scalar,
          grounding: scalar,
          termDomains: scalar,
          identityGap: scalar,
          domains: list("ON_Domains"),
          refs: list("ON_TermRefs"),
          edgesOut: list("ON_TermEdges"),
          edgesIn: list("ON_TermEdges"),
          tables: list("M__RegisteredTables"),
        }),
        type("ON_Domains", { domainId: scalar }),
        type("ON_TermRefs", {
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
        type("M__Relationships", { id: scalar, cypherType: scalar, targetTableId: scalar }),
        type("M__TableColumns", {
          columnName: scalar,
          columnKey: scalar,
          glossaryRef: list("ON_TermRefs"),
        }),
        type("ON_TermEdges", {
          id: scalar,
          fromTermId: scalar,
          toTermId: scalar,
          relType: scalar,
          sourceTerm: one("ON_Terms"),
          targetTerm: one("ON_Terms"),
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
      [rootKey("ontology", "_terms"), "on_terms"],
      [rootKey("ontology", "_term_edges"), "on_termEdges"],
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
    expect(q).toContain("_terms: on_terms {");
    expect(q).toContain(
      "edgesOut { id fromTermId toTermId relType targetTerm { id } }",
    );
    // One root: the edges are the terms', not a root of their own.
    expect(q).not.toContain("on_termEdges");
    expect(q.match(/\b(?:m__|on_)\w+/g)).toEqual(["on_terms"]);
    expect(squash(ontologyQuery(reader(), scope(["columns"])))).not.toContain("edgesOut {");
    expect(q).toContain("domains { domainId }");
    expect(q).toContain("refs {");
    // Data off: a column's product is read for its look alone, and no products are read.
    expect(q).toContain("product { id }");
    expect(q).not.toContain("data_products");
  });

  it("reads whether each term's tables are all declared one entity", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"])));
    expect(q).toContain("identityGap");
  });

  it("selects only what its layers draw", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"])));
    expect(q).not.toContain("domains:");
    expect(q).not.toContain("refs:");
    // REQ-2130 (amended 2026-10-29): Data names the product of each table a term reaches -- a
    // column's, and a class term's -- and reads no product, table or column apart.
    const data = squash(ontologyQuery(reader(), scope(["ontology", "data", "columns"])));
    expect(data.match(/product \{ id name domainId \}/g)).toHaveLength(2);
    expect(data).not.toContain("data_products");
    expect(data).not.toContain("glossary_ref");
    // No relationship between tables is read: those are the ERD's.
    expect(data).not.toContain("source_relationships");
  });

  it("leaves the hidden statuses out in its where", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"], { hiddenStatuses: ["retired"] })));
    // REQ-1387: by the status a person set.
    expect(q).toContain(
      'on_terms(where: { status: { in: ["approved","draft","deprecated"] } })',
    );
    const two = squash(
      ontologyQuery(reader(), scope(["ontology"], { hiddenStatuses: ["retired", "draft"] })),
    );
    expect(two).toContain(
      'on_terms(where: { _and: [{ status: { in: ["approved","draft","deprecated"] } }, { status: { in: ["approved","deprecated","retired"] } }] })',
    );
  });

  it("keeps the chosen domains, and the enterprise-wide terms, in its where", () => {
    const q = squash(
      ontologyQuery(reader(), scope(["ontology", "domains", "columns"], { domains: ["sales"] })),
    );
    expect(q).toContain(
      'on_terms(where: { _or: [{ termDomains: { like: "%|sales|%" } }, { termDomains: { like: "%|*|%" } }] })',
    );
    expect(q).toContain('domains(where: { domainId: { in: ["sales","*"] } })');
    expect(q).toContain('refs(where: { domainId: { in: ["sales"] } })');
  });

  it("keeps the chosen edge types in its where, of the term edges", () => {
    const q = squash(
      ontologyQuery(reader(), scope(["ontology", "columns"], { edgeTypes: ["KIND_OF", "PART_OF"] })),
    );
    expect(q).toContain('edgesOut(where: { relType: { in: ["KIND_OF","PART_OF"] } })');
    expect(q).not.toContain("sourceRelationships");
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
    const kept = '{ status: { in: ["approved","draft","deprecated"] } }';
    expect(q).toContain(
      `on_terms(where: { _and: [${kept}, { _or: [` +
        `{ edgesOut: { _and: [{ relType: { in: ["KIND_OF"] } }, { targetTerm: ${kept} }] } }, ` +
        `{ edgesIn: { _and: [{ relType: { in: ["KIND_OF"] } }, { sourceTerm: ${kept} }] } }, ` +
        `{ tables: { } }, { refs: { } }] }] })`,
    );
    // With no layer drawing a relationship every term is an orphan: none is kept.
    const none = squash(ontologyQuery(reader(), scope(["data"], { orphans: false })));
    expect(none).toContain("on_terms(where: { _or: [] })");
  });

  it("keeps, with Orphans off and Domains drawn, a term in a domain kept, as it is drawn linked to it", () => {
    // A draft named for a column is in its table's domain (term_domains), whatever else is drawn.
    const q = squash(ontologyQuery(reader(), scope(["ontology", "domains"], { orphans: false })));
    expect(q).toContain("{ domains: { } }");
    const narrowed = squash(
      ontologyQuery(reader(), scope(["ontology", "domains"], { orphans: false, domains: ["x"] })),
    );
    expect(narrowed).toContain('{ domains: { domainId: { in: ["x","*"] } } }');
  });

  it("reads the tables a class term grounds with the ontology, in the domains kept", () => {
    const q = squash(ontologyQuery(reader(), scope(["ontology"], { domains: ["sales"] })));
    expect(q).toContain(
      'tables(where: { domainId: { in: ["sales"] } }) { id tableName domainId }',
    );
    expect(squash(ontologyQuery(reader(), scope(["domains"])))).not.toContain("tables:");
    // A class term is no orphan: its table is a relationship drawn.
    const orphans = squash(ontologyQuery(reader(), scope(["ontology"], { orphans: false })));
    expect(orphans).toContain("{ tables: { } }");
  });

  it("is null when the role cannot read the glossary", () => {
    const m = reader();
    m.roots.delete(rootKey("ontology", "_terms"));
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
      "on_terms(where: { _or: [" +
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
    expect(q.match(/refs \{/g)).toHaveLength(3);
    expect(q.match(/domains \{/g)).toHaveLength(3);
    expect(q).toContain("edgesIn");
    expect(q).toContain("sourceTerm");
    // The deepest level: an edge's end by its id.
    expect(q).toContain("targetTerm { id }");
    // Without a search: one level, the edges' ends by id, and no edges read the other way.
    const flat = squash(ontologyQuery(reader(), scope(["ontology", "domains", "columns"])));
    expect(flat.match(/refs \{/g)).toHaveLength(1);
    expect(flat).not.toContain("edgesIn");
  });

  it("counts what its root would read, by id with each term's domains", () => {
    const q = squash(ontologyCountQuery(reader(), scope(["ontology"], { search: ["cust"] })));
    expect(q).toContain("query OntologyCount");
    expect(q).toContain('{ name: { ilike: "%cust%" } }');
    expect(q).toContain("{ id domains { domainId } }");
  });
});

describe("REQ-416: the query's answer keeps GraphQL's names", () => {
  it("selects each field under the schema's own name, and the graph reads its answer by it", () => {
    const q = ontologyQuery(reader(), scope(["ontology", "domains", "columns"])) as string;
    const selections = q.split("\n").slice(1, -1);
    // Only the root field is answered under a name of its own: its name carries its domain's alias.
    expect(selections.filter((l) => /^\s*\w+: \w+/.test(l))).toEqual(["  _terms: on_terms {"]);
    expect(q).toContain("identityGap");
    expect(q).toContain("fromTermId");
    expect(q).not.toMatch(/\b(identity_gap|from_term_id): /);

    const answer = (id: number, over: Partial<TermRow>): TermRow => ({
      id,
      name: `t${id}`,
      definition: null,
      refCount: 0,
      deprecated: false,
      retired: false,
      status: "approved",
      grounding: "grounded",
      ...over,
    });
    const g = ontologyGraph({
      _terms: [
        answer(1, {
          domains: [{ domainId: "sales" }],
          edgesOut: [{ id: 5, fromTermId: 1, toTermId: 2, relType: "KIND_OF", targetTerm: { id: 2 } }],
        }),
        answer(2, {}),
      ],
    });
    expect(g.nodes.map((n) => n.id)).toEqual(["d:sales", "term:1", "term:2"]);
    expect(g.edges.map((e) => e.id)).toContain("term_edge:term:1>term:2:KIND_OF");
  });
});

describe("REQ-2143: a nested read, flattened", () => {
  const term = (id: number, over: Partial<TermRow> = {}): TermRow => ({
    id,
    name: `t${id}`,
    definition: null,
    refCount: 0,
    deprecated: false,
    retired: false,
    status: "approved",
    grounding: "grounded",
    ...over,
  });

  it("holds each term once, at its fullest, and each edge once", () => {
    // 1 matched; 2 a hop off (read in full); 3 two hops off (its edge's end alone at 2's level).
    const two = term(2, {
      refs: [],
      edgesOut: [
        { id: 9, fromTermId: 2, toTermId: 3, relType: "KIND_OF", targetTerm: term(3) },
      ],
    });
    const rows = [
      term(1, {
        refs: [],
        edgesOut: [
          { id: 8, fromTermId: 1, toTermId: 2, relType: "PART_OF", targetTerm: two },
        ],
        edgesIn: [
          { id: 8, fromTermId: 1, toTermId: 2, relType: "PART_OF", sourceTerm: { id: 1 } },
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
