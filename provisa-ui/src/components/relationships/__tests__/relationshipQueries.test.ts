// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1a8e27-b5c4-4d93-a072-9e3c51d8b4f6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: a relationship's query on each surface, by the names the role is served, with a row
// limit; a surface that does not serve it to the role has none.

import { describe, expect, it } from "vitest";
import {
  relationshipCypher,
  relationshipGraphql,
  relationshipJsonApi,
  relationshipQueryLinks,
  relationshipQueryRef,
  relationshipSparql,
  relationshipSql,
  type RelationshipQueryRef,
} from "../relationshipQueries";
import { GRAPH, TABLES, VOCABULARY, placedBy, reader, tagged } from "./relationshipQueryFixtures";

const ref = (r = placedBy()): RelationshipQueryRef => {
  const built = relationshipQueryRef(r, TABLES);
  if (built === null) throw new Error("no reference");
  return built;
};

describe("relationshipQueryRef", () => {
  it("names both ends from the listed tables, with their aliases", () => {
    const r = ref();
    expect(r.source).toEqual({ id: 1, domainId: "sales", tableName: "orders", alias: null });
    expect(r.target).toEqual({
      id: 2,
      domainId: "sales",
      tableName: "customers",
      alias: "clients",
    });
    expect(r.via).toBeNull();
    expect(r.graphqlField).toBe("client");
  });

  it("is none for a relationship reaching a function", () => {
    expect(
      relationshipQueryRef(
        placedBy({ targetTableId: null, targetColumn: null, targetFunctionName: "f" }),
        TABLES,
      ),
    ).toBeNull();
  });

  it("is none while a table it joins is not listed", () => {
    expect(relationshipQueryRef(placedBy(), [])).toBeNull();
  });

  it("carries a junction and no GraphQL field for a junction-backed edge", () => {
    const r = ref(tagged());
    expect(r.via?.table.tableName).toBe("order_tags");
    expect(r.graphqlField).toBeNull();
  });
});

describe("Cypher", () => {
  it("matches its type between its two node labels, limited", () => {
    expect(relationshipCypher(ref(), GRAPH)).toBe(
      "MATCH (a:Orders)-[r:PLACED_BY]->(b:Clients) RETURN a, r, b LIMIT 25",
    );
  });

  it("is none for a relationship excluded from Cypher", () => {
    expect(relationshipCypher(ref(placedBy({ disableCypher: true })), GRAPH)).toBeNull();
  });

  it("is none when the role's graph does not serve the edge", () => {
    expect(relationshipCypher(ref(), { ...GRAPH, relationship_types: [] })).toBeNull();
  });
});

describe("SQL", () => {
  it("joins the source to the target on its columns, by the tables' SQL names, limited", () => {
    expect(relationshipSql(ref())).toBe(
      'select * from "sales"."orders" as s join "sales"."clients" as t on s."customer_id" = t."id" limit 25',
    );
  });

  it("pairs composite keys in order", () => {
    expect(relationshipSql(ref(placedBy({ sourceColumn: "a, b", targetColumn: "x,y" })))).toContain(
      's."a" = t."x" and s."b" = t."y"',
    );
  });

  it("goes through the junction, its type fixed, for a junction-backed edge", () => {
    expect(relationshipSql(ref(tagged()))).toBe(
      'select * from "sales"."orders" as s' +
        ' join "sales"."order_tags" as j on s."id" = j."order_id" and j."kind" = \'label\'' +
        ' join "sales"."tags" as t on j."tag_id" = t."id" limit 25',
    );
  });

  it("is none when its key lists do not pair", () => {
    expect(relationshipSql(ref(placedBy({ sourceColumn: "a,b", targetColumn: "x" })))).toBeNull();
  });
});

describe("SPARQL", () => {
  it("selects subject and object over its predicate, each typed by its end's class", () => {
    const q = relationshipSparql(ref(), GRAPH, VOCABULARY);
    expect(q).toContain("PREFIX sales: <https://provisa.example/sales/>");
    expect(q).toContain("PREFIX rel: <https://provisa.example/rel/>");
    expect(q).toContain("?s a sales:orders . ?s rel:PLACED_BY ?o . ?o a sales:clients .");
    expect(q).toMatch(/LIMIT 25$/);
  });

  it("is none for a relationship excluded from Cypher (no edge is served)", () => {
    expect(
      relationshipSparql(ref(placedBy({ disableCypher: true })), GRAPH, VOCABULARY),
    ).toBeNull();
  });

  it("is none when the vocabulary has no predicate for the edge", () => {
    expect(relationshipSparql(ref(), GRAPH, { ...VOCABULARY, edges: [] })).toBeNull();
  });
});

describe("GraphQL", () => {
  it("reads the source's columns with the relationship's field nested, limited", () => {
    expect(relationshipGraphql(ref(), reader())).toBe(
      "{ sales__orders(limit: 25) { id customerId client { id name } } }",
    );
  });

  it("is none for a field the role's schema does not serve", () => {
    expect(relationshipGraphql(ref(placedBy({ graphqlAlias: "owner" })), reader())).toBeNull();
  });

  it("is none for a junction-backed edge", () => {
    expect(relationshipGraphql(ref(tagged()), reader())).toBeNull();
  });
});

describe("JSON:API", () => {
  it("reads the source's resources with the relationship included, by its SQL-plane name", () => {
    expect(relationshipJsonApi(ref(), reader(), GRAPH)).toBe(
      "/data/jsonapi/sales/orders?include=client&page%5Bsize%5D=25",
    );
  });

  it("is none without a SQL-plane name", () => {
    expect(relationshipJsonApi(ref(placedBy({ physicalName: null })), reader(), GRAPH)).toBeNull();
  });

  it("is none when the target's resources have no id", () => {
    const keyless = {
      ...GRAPH,
      node_labels: GRAPH.node_labels.map((n) => (n.table_id === 2 ? { ...n, pk_columns: [] } : n)),
    };
    // Neither a key nor an id column: its resources have no id, and JSON:API refuses the include.
    expect(relationshipJsonApi(ref(), reader(["name"]), keyless)).toBeNull();
  });
});

describe("relationshipQueryLinks", () => {
  const all = { reader: reader(), graph: GRAPH, vocabulary: VOCABULARY };

  it("links every surface that serves it, in order, each run at once", () => {
    const links = relationshipQueryLinks(ref(), all);
    expect(links.map((l) => l.surface)).toEqual(["cypher", "sql", "sparql", "graphql", "jsonapi"]);
    expect(links.map((l) => l.link.to)).toEqual([
      "/graph",
      "/sql",
      "/sparql",
      "/query",
      "/jsonapi",
    ]);
    expect(links.every((l) => l.link.state.autoRun === true)).toBe(true);
    expect(links[1].link.state.sql).toContain("join");
    expect(links[4].link.state.jsonapiUrl).toContain("include=client");
  });

  it("leaves out Cypher and SPARQL for a relationship excluded from Cypher", () => {
    const links = relationshipQueryLinks(ref(placedBy({ disableCypher: true })), all);
    expect(links.map((l) => l.surface)).toEqual(["sql", "graphql", "jsonapi"]);
  });

  it("links a junction-backed edge in Cypher, SQL and SPARQL only", () => {
    const links = relationshipQueryLinks(ref(tagged()), all);
    expect(links.map((l) => l.surface)).toEqual(["cypher", "sql", "sparql"]);
  });

  it("leaves out SQL when the role reads neither table", () => {
    const r = reader();
    const links = relationshipQueryLinks(ref(), {
      ...all,
      reader: { ...r, roots: new Map() },
    });
    expect(links.map((l) => l.surface)).toEqual(["cypher", "sparql"]);
  });

  it("links nothing while nothing is read", () => {
    expect(relationshipQueryLinks(ref(), { reader: null, graph: null, vocabulary: null })).toEqual(
      [],
    );
  });
});
