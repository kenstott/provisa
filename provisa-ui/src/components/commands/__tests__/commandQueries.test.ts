// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2a9d14-8b37-4f05-a1c6-3d0f7e9b2c58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): a command's call on each surface it is served on, as an Explore
// hand-off, its arguments filled in: a scalar as that surface writes a literal, a relation argument
// as the one value it takes (a table reference, a query, or rows). A write opens without running.

import { describe, expect, it } from "vitest";
import { RoleSchema, type MetaReader } from "../../../api/metaGraph";
import type { ActionArg } from "../../../api/actions";
import {
  commandQueryLink,
  commandSurfaces,
  sqlTypedLiteral,
  type ArgValue,
  type CommandCall,
  type ServedCommand,
} from "../commandQueries";

const scalar = (name: string) => ({ kind: "SCALAR", name });
const list = (name: string) => ({
  kind: "LIST",
  name: null,
  ofType: { kind: "OBJECT", name },
});

const reader: MetaReader = {
  schema: new RoleSchema({
    __schema: {
      queryType: { name: "Query" },
      types: [
        { name: "Query", fields: [{ name: "derived__enrichGrpcSet", type: list("EnrichOut") }] },
        { name: "Mutation", fields: [{ name: "sales__refundOrder", type: scalar("Int") }] },
        {
          name: "EnrichOut",
          fields: [
            { name: "id", type: scalar("Int") },
            { name: "score", type: scalar("Float") },
          ],
        },
      ],
    },
  }),
  roots: new Map(),
};

const SERVED: ServedCommand = {
  command: "enrich_grpc_set",
  domain_id: "derived",
  graphql_root: "Query",
  graphql_field: "derived__enrichGrpcSet",
  names: { sql: "enrich_grpc_set", cypher: "enrich_grpc_set", rest: "enrich_grpc_set" },
};

const input: ActionArg = { name: "input", type: "String", argKind: "result_set" };
const factor: ActionArg = { name: "factor", type: "Float" };
const label: ActionArg = { name: "label", type: "String" };

const ENRICH: CommandCall = { name: "enrich_grpc_set", kind: "query", arguments: [input, factor] };

const QUERY = `select * from (select "id" from "sales"."orders") as given limit 100`;
const ROWS = [{ id: 1 }, { id: 2 }];
const asQuery: ArgValue = { kind: "relation", text: QUERY, rows: null };
const asRows: ArgValue = { kind: "relation", text: JSON.stringify(ROWS), rows: ROWS };
const values = (rel: ArgValue): Record<string, ArgValue> => ({
  input: rel,
  factor: { kind: "scalar", value: 1.5 },
});

describe("the surfaces a command is offered on", () => {
  it("is every surface that names it, in the order a table's links are drawn", () => {
    expect(commandSurfaces(SERVED, reader)).toEqual([
      "cypher",
      "sql",
      "graphql",
      "rest",
      "grpc",
      "jsonapi",
    ]);
  });

  it("leaves out a surface the command is excluded from", () => {
    const served = { ...SERVED, names: { ...SERVED.names, cypher: null, rest: null } };
    expect(commandSurfaces(served, reader)).toEqual(["sql", "graphql", "grpc"]);
  });

  it("leaves out JSON:API where the command has no REST name, the name its route binds", () => {
    const served = { ...SERVED, names: { ...SERVED.names, rest: null } };
    expect(commandSurfaces(served, reader)).not.toContain("jsonapi");
  });

  it("is none when the role is not served the command", () => {
    expect(commandSurfaces(null, reader)).toEqual([]);
  });

  it("does not offer SPARQL, which has no call of a command", () => {
    const offered: string[] = commandSurfaces(SERVED, reader);
    expect(offered).not.toContain("sparql");
  });
});

describe("a command's call on each surface", () => {
  it("SQL: the command's SQL name in FROM, its arguments as literals in order", () => {
    expect(commandQueryLink("sql", ENRICH, SERVED, reader, values(asQuery))).toEqual({
      to: "/sql",
      state: {
        sql: `select * from enrich_grpc_set('${QUERY.replace(/'/g, "''")}', 1.5)`,
        autoRun: true,
      },
    });
  });

  it("SQL: rows are passed as their JSON text, a quote in them doubled", () => {
    const rows = [{ name: "o'neil" }];
    const link = commandQueryLink(
      "sql",
      ENRICH,
      SERVED,
      reader,
      values({ kind: "relation", text: JSON.stringify(rows), rows }),
    );
    expect(link.state.sql).toBe(`select * from enrich_grpc_set('[{"name":"o''neil"}]', 1.5)`);
  });

  it("GraphQL: the served field with its arguments and the columns it answers", () => {
    expect(commandQueryLink("graphql", ENRICH, SERVED, reader, values(asQuery))).toEqual({
      to: "/query",
      state: {
        query: `query { derived__enrichGrpcSet(input: ${JSON.stringify(QUERY)}, factor: 1.5) { id score } }`,
        autoRun: true,
      },
    });
  });

  it("Cypher: a CALL of its Cypher name, a quote in a string escaped", () => {
    const call: CommandCall = { ...ENRICH, arguments: [input, label] };
    const link = commandQueryLink("cypher", call, SERVED, reader, {
      input: asRows,
      label: { kind: "scalar", value: "it's" },
    });
    expect(link).toEqual({
      to: "/graph",
      state: { query: `CALL enrich_grpc_set('[{"id":1},{"id":2}]', 'it\\'s')`, autoRun: true },
    });
  });

  it("REST: a read is a GET with each argument a query parameter", () => {
    const link = commandQueryLink("rest", ENRICH, SERVED, reader, values(asQuery));
    const params = new URLSearchParams({ input: QUERY, factor: "1.5" });
    expect(link).toEqual({
      to: "/openapi",
      state: {
        openApiUrl: `GET /data/rest/derived/commands/enrich_grpc_set?${params.toString()}`,
        autoRun: true,
      },
    });
  });

  it("JSON:API: a read hands its REST-named command with each argument as written", () => {
    expect(commandQueryLink("jsonapi", ENRICH, SERVED, reader, values(asQuery))).toEqual({
      to: "/jsonapi",
      state: {
        jsonapiCommand: "enrich_grpc_set",
        jsonapiDomain: "derived",
        jsonapiWrite: false,
        jsonapiArgs: { input: QUERY, factor: 1.5 },
        autoRun: true,
      },
    });
  });

  it("gRPC: the command's call with its arguments, rows as a JSON array", () => {
    expect(commandQueryLink("grpc", ENRICH, SERVED, reader, values(asRows))).toEqual({
      to: "/grpc",
      state: {
        grpcCommand: "enrich_grpc_set",
        grpcBody: { name: "enrich_grpc_set", args: { input: ROWS, factor: 1.5 } },
        autoRun: true,
      },
    });
  });
});

describe("a command that writes", () => {
  const REFUND: CommandCall = {
    name: "refund_order",
    kind: "mutation",
    arguments: [{ name: "order_id", type: "Int" }],
  };
  const served: ServedCommand = {
    command: "refund_order",
    domain_id: "sales",
    graphql_root: "Mutation",
    graphql_field: "sales__refundOrder",
    names: { sql: "refund_order", cypher: "refund_order", rest: "refund_order" },
  };
  const given = { order_id: { kind: "scalar", value: 7 } } as Record<string, ArgValue>;

  it("opens on every surface without running", () => {
    for (const surface of commandSurfaces(served, reader)) {
      expect(commandQueryLink(surface, REFUND, served, reader, given).state.autoRun).toBe(false);
    }
  });

  it("GraphQL: a mutation, with no selection for a scalar answer", () => {
    expect(commandQueryLink("graphql", REFUND, served, reader, given).state.query).toBe(
      "mutation { sales__refundOrder(order_id: 7) }",
    );
  });

  it("REST: a POST with its arguments as the body", () => {
    expect(commandQueryLink("rest", REFUND, served, reader, given)).toEqual({
      to: "/openapi",
      state: {
        openApiUrl: "POST /data/rest/sales/commands/refund_order",
        openApiBody: { order_id: 7 },
        autoRun: false,
      },
    });
  });

  it("JSON:API: a POST with its arguments, left for Run", () => {
    expect(commandQueryLink("jsonapi", REFUND, served, reader, given)).toEqual({
      to: "/jsonapi",
      state: {
        jsonapiCommand: "refund_order",
        jsonapiDomain: "sales",
        jsonapiWrite: true,
        jsonapiArgs: { order_id: 7 },
        autoRun: false,
      },
    });
  });
});

describe("SQL: each scalar argument a literal of its declared type", () => {
  it.each([
    ["double", "1", "1"],
    ["double", 2.5, "2.5"],
    ["Float", -0.5, "-0.5"],
    ["integer", 42, "42"],
    ["Int", 7, "7"],
    ["bigint", "9223372036854775807", "9223372036854775807"],
    ["numeric", "1.50", "1.50"],
    ["double", Infinity, "'Infinity'"],
    ["boolean", true, "true"],
    ["Boolean", false, "false"],
    ["text", "o'neil", "'o''neil'"],
    ["String", "1", "'1'"],
    ["date", "2024-01-02", "DATE '2024-01-02'"],
    ["timestamp", "2024-01-02 03:04:05", "TIMESTAMP '2024-01-02 03:04:05'"],
    ["timestamptz", "2024-01-02 03:04:05+00", "TIMESTAMPTZ '2024-01-02 03:04:05+00'"],
    ["time", "03:04", "TIME '03:04'"],
    ["uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "UUID 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"],
    ["double", null, "null"],
  ])("%s %j is written %s", (type, value, written) => {
    expect(sqlTypedLiteral(type, value)).toBe(written);
  });

  it("the maintainer's report: four doubles are written as numbers, not quoted text", () => {
    const greet: CommandCall = {
      name: "greet",
      kind: "query",
      arguments: ["lat1", "lon1", "lat2", "lon2"].map((name) => ({ name, type: "double" })),
    };
    const served: ServedCommand = {
      command: "greet",
      domain_id: "geo",
      graphql_root: "Query",
      graphql_field: "geo__greet",
      names: { sql: "greet", cypher: "greet", rest: "greet" },
    };
    const given: Record<string, ArgValue> = {
      lat1: { kind: "scalar", value: 1 },
      lon1: { kind: "scalar", value: "2" },
      lat2: { kind: "scalar", value: 3 },
      lon2: { kind: "scalar", value: 4 },
    };
    expect(commandQueryLink("sql", greet, served, reader, given).state.sql).toBe(
      "select * from greet(1, 2, 3, 4)",
    );
  });
});
