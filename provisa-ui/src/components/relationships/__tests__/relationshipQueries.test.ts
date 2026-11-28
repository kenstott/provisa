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

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { RoleSchema, type GraphqlType } from "../../../api/metaGraph";
import {
  relationshipCypher,
  relationshipGraphql,
  relationshipJsonApi,
  relationshipQueryLinks,
  protoRelationFieldName,
  relationshipGrpc,
  relationshipQueryRef,
  relationshipRest,
  relationshipSparql,
  relationshipSql,
  type RelationshipQueryRef,
} from "../relationshipQueries";
import {
  GRAPH,
  TABLES,
  VOCABULARY,
  placedBy,
  reader,
  tagged,
  toDimension,
} from "./relationshipQueryFixtures";

/** The role's schema with a second relationship from orders to customers (a billing client). */
function twoToClients(): RoleSchema {
  return schemaWith((fields) => [
    ...fields,
    { name: "billedTo", type: { kind: "OBJECT", name: "SalesClients" } },
  ]);
}

/** The role's schema with a second relationship to customers and a column of orders its gRPC
 * field name translates to. */
function billedToColumnOnOrders(): RoleSchema {
  return schemaWith((fields) => [
    ...fields,
    { name: "billedTo", type: { kind: "OBJECT", name: "SalesClients" } },
    { name: "billed_to", type: { kind: "SCALAR", name: "String" } },
  ]);
}

function schemaWith(
  orders: (fields: { name: string; type: GraphqlType }[]) => { name: string; type: GraphqlType }[],
): RoleSchema {
  const scalar = (name: string): GraphqlType => ({ kind: "SCALAR", name });
  const object = (name: string): GraphqlType => ({ kind: "OBJECT", name });
  const list = (name: string): GraphqlType => ({ kind: "LIST", name: null, ofType: object(name) });
  return new RoleSchema({
    __schema: {
      queryType: { name: "Query" },
      types: [
        {
          name: "Query",
          fields: [
            { name: "sales__orders", type: list("SalesOrders") },
            { name: "sales__clients", type: list("SalesClients") },
          ],
        },
        {
          name: "SalesOrders",
          fields: orders([
            { name: "id", type: scalar("Int") },
            { name: "customerId", type: scalar("Int") },
            { name: "client", type: object("SalesClients") },
          ]),
        },
        { name: "SalesClients", fields: [{ name: "id", type: scalar("Int") }] },
      ],
    },
  });
}

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

  it("carries a junction and its stored GraphQL field for a junction-backed edge", () => {
    const r = ref(tagged());
    expect(r.via?.table.tableName).toBe("order_tags");
    expect(r.graphqlField).toBe("tagged");
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

  it("finds the served edge by its ends and join, under the type it is served as", () => {
    // A discovery-made shared dimension: no alias, so the admin list's stored type
    // (BELONGS_TO_NEIGHBORHOOD) is not the one the graph schema serves it under.
    expect(relationshipCypher(ref(toDimension()), GRAPH)).toBe(
      "MATCH (a:Shelter:BuildingPermits)-[r:IS_PERMIT_NEIGHBORHOOD_DIMENSION]->" +
        "(b:Shelter:PermitNeighborhood) RETURN a, r, b LIMIT 25",
    );
  });

  it("is none when no served edge between its labels joins its columns", () => {
    expect(
      relationshipCypher(
        ref(placedBy({ sourceColumn: "billing_id", cypherType: "BILLED" })),
        GRAPH,
      ),
    ).toBeNull();
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

  it("selects a shared dimension's edge by the predicate it is served under", () => {
    expect(relationshipSparql(ref(toDimension()), GRAPH, VOCABULARY)).toContain(
      "?s a shelter:building_permits . ?s rel:IS_PERMIT_NEIGHBORHOOD_DIMENSION ?o ." +
        " ?o a shelter:permit_neighborhood .",
    );
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

  it("nests a junction-backed edge's field, as the server names it", () => {
    expect(relationshipGraphql(ref(tagged()), reader())).toBe(
      "{ sales__orders(limit: 25) { id customerId tagged { id } } }",
    );
  });

  it("is none for a junction-backed edge whose field the role's schema does not serve", () => {
    expect(relationshipGraphql(ref({ ...tagged(), graphqlAlias: "labelled" }), reader())).toBeNull();
  });
});

describe("REST", () => {
  it("reads the source's columns with the relationship nested, by its GraphQL name, limited", () => {
    expect(relationshipRest(ref(), reader())).toBe(
      "GET /data/rest/sales/orders?fields=id,customerId,client&limit=25",
    );
  });

  it("is none for a field the role's schema does not serve", () => {
    expect(relationshipRest(ref(placedBy({ graphqlAlias: "owner" })), reader())).toBeNull();
  });

  it("nests a junction-backed edge by its GraphQL field", () => {
    expect(relationshipRest(ref(tagged()), reader())).toBe(
      "GET /data/rest/sales/orders?fields=id,customerId,tagged&limit=25",
    );
  });
});

describe("gRPC", () => {
  it("calls the source's Query method, masking its join column and the relationship field's", () => {
    expect(relationshipGrpc(ref(), reader())).toEqual({
      method: "QuerySalesOrders()",
      body: { read_mask: { paths: ["customer_id", "client.id"] }, limit: 25 },
    });
  });

  it("gives each of two relationships to one table its own field in the read_mask", () => {
    const r = { ...reader(), schema: twoToClients() };
    expect(relationshipGrpc(ref(), r)?.body.read_mask).toEqual({
      paths: ["customer_id", "client.id"],
    });
    expect(
      relationshipGrpc(ref(placedBy({ graphqlAlias: "billedTo" })), r)?.body.read_mask,
    ).toEqual({ paths: ["customer_id", "billed_to.id"] });
  });

  it("is none when a column of the source takes the relationship's field name", () => {
    const r = reader();
    const billed = ref(placedBy({ graphqlAlias: "billedTo" }));
    expect(relationshipGrpc(billed, { ...r, schema: billedToColumnOnOrders() })).toBeNull();
  });

  it("is none for a field the role's schema does not serve", () => {
    expect(relationshipGrpc(ref(placedBy({ graphqlAlias: "owner" })), reader())).toBeNull();
  });

  it("masks a junction-backed edge by its GraphQL field", () => {
    expect(relationshipGrpc(ref(tagged()), reader())).toEqual({
      method: "QuerySalesOrders()",
      body: { read_mask: { paths: ["id", "tagged.id"] }, limit: 25 },
    });
  });
});

describe("protoRelationFieldName", () => {
  // The examples tests/unit/test_proto_relation_field_name_examples.py pins proto_gen's
  // proto_relation_field_name to: one translation on both sides.
  const fixture = resolve(
    __dirname,
    "../../../../../tests/fixtures/proto_relation_field_names.json",
  );
  const { examples } = JSON.parse(readFileSync(fixture, "utf8")) as {
    examples: [string, string][];
  };

  it.each(examples)("translates %s to %s as proto_gen does", (graphqlField, protoField) => {
    expect(protoRelationFieldName(graphqlField)).toBe(protoField);
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
    expect(links.map((l) => l.surface)).toEqual([
      "cypher",
      "sql",
      "sparql",
      "graphql",
      "rest",
      "grpc",
      "jsonapi",
    ]);
    expect(links.map((l) => l.link.to)).toEqual([
      "/graph",
      "/sql",
      "/sparql",
      "/query",
      "/openapi",
      "/grpc",
      "/jsonapi",
    ]);
    // The Graph page (Cypher, SPARQL) runs every query it is handed; the others are asked to.
    const graph = (to: string) => to === "/graph" || to === "/sparql";
    expect(links.every((l) => !graph(l.link.to) || !("autoRun" in l.link.state))).toBe(true);
    expect(links.every((l) => graph(l.link.to) || l.link.state.autoRun === true)).toBe(true);
    expect(links[1].link.state.sql).toContain("join");
    expect(links[4].link.state.openApiUrl).toContain("fields=id,customerId,client");
    expect(links[5].link.state).toEqual({
      grpcMethod: "QuerySalesOrders()",
      grpcBody: { read_mask: { paths: ["customer_id", "client.id"] }, limit: 25 },
      autoRun: true,
    });
    expect(links[6].link.state.jsonapiUrl).toContain("include=client");
  });

  it("links a shared dimension on every surface, Cypher and SPARQL included", () => {
    const links = relationshipQueryLinks(ref(toDimension()), all);
    expect(links.map((l) => l.surface)).toEqual([
      "cypher",
      "sql",
      "sparql",
      "graphql",
      "rest",
      "grpc",
      "jsonapi",
    ]);
  });

  it("leaves out Cypher and SPARQL for a relationship excluded from Cypher", () => {
    const links = relationshipQueryLinks(ref(placedBy({ disableCypher: true })), all);
    expect(links.map((l) => l.surface)).toEqual(["sql", "graphql", "rest", "grpc", "jsonapi"]);
  });

  it("links a junction-backed edge in every surface that serves it, GraphQL included", () => {
    const links = relationshipQueryLinks(ref(tagged()), all);
    expect(links.map((l) => l.surface)).toEqual(["cypher", "sql", "sparql", "graphql", "rest", "grpc"]);
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
