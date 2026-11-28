// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4c6a20-7e1b-4b53-a8f2-61c0e5d93b47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252 (Amended 2026-11-22): a table whose aggregates are enabled shows, on its API section, a
// "Query it" row drawn as a relationship's links are: a link per surface that serves its aggregates
// to the acting role, each opening that surface's Explore editor with an aggregate query run at
// once -- its count, and with group-by enabled its count per value of one of its columns. A
// surface that does not serve the table's aggregates has no link; with aggregates off there is no
// row.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement } from "react";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { RegisteredTable } from "../../../types/admin";
import { RoleSchema, metaReader, type MetaReader } from "../../../api/metaGraph";
import type { SparqlVocabulary } from "../../../components/graph/sparql";
import {
  GRAPH,
  VOCABULARY,
} from "../../../components/relationships/__tests__/relationshipQueryFixtures";
import { QueryRead } from "../TableSectionsRead";

vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: vi.fn(),
}));

/** sales.customers, served to the role as sales__clients and the Clients node label. */
function customers(enableAggregates: boolean, enableGroupBy: boolean): RegisteredTable {
  return {
    id: 2,
    sourceId: "pg",
    schemaName: "public",
    tableName: "customers",
    domainId: "sales",
    alias: "clients",
    enableAggregates,
    enableGroupBy,
    writeOps: [],
    watermarkColumn: null,
    viewSql: null,
    columns: [
      { columnName: "id", computedSqlAlias: "id", isPrimaryKey: true },
      { columnName: "full_name", computedSqlAlias: "name", isPrimaryKey: false },
    ],
  } as unknown as RegisteredTable;
}

const scalar = (name: string) => ({ kind: "SCALAR", name });
const object = (name: string) => ({ kind: "OBJECT", name });
const list = (name: string) => ({ kind: "LIST", name: null, ofType: object(name) });

/** The role's schema: customers' root, and its aggregate and group-by roots where it has them. */
function reader(roots: { aggregate: boolean; groupBy: boolean }): MetaReader {
  const schema = new RoleSchema({
    __schema: {
      queryType: { name: "Query" },
      types: [
        {
          name: "Query",
          fields: [
            { name: "sales__clients", type: list("SalesClients") },
            ...(roots.aggregate
              ? [{ name: "sales__clients_aggregate", type: object("SalesClientsAggregate") }]
              : []),
            ...(roots.groupBy
              ? [{ name: "sales__clients_group_by", type: list("SalesClientsGroupByRow") }]
              : []),
          ],
        },
        {
          name: "SalesClients",
          fields: [
            { name: "id", type: scalar("Int") },
            { name: "name", type: scalar("String") },
          ],
        },
        { name: "SalesClientsAggregate", fields: [{ name: "count", type: scalar("Int") }] },
        { name: "SalesClientsGroupByRow", fields: [{ name: "groupKey", type: scalar("JSON") }] },
      ],
    },
  });
  return { schema, roots: new Map([["sales.customers", "sales__clients"]]) };
}

const VOCAB: SparqlVocabulary = {
  ...VOCABULARY,
  predicates: [
    {
      iri: "https://provisa.example/sales/clients_name",
      kind: "column",
      label: "Clients",
      property: "name",
      datatype: "string",
    },
  ],
};

function Opened() {
  const location = useLocation();
  return (
    <pre data-testid="opened">
      {JSON.stringify({ to: location.pathname, state: location.state })}
    </pre>
  );
}

function routed(ui: ReactElement) {
  return render(
    <Routes>
      <Route path="/" element={ui} />
      <Route path="*" element={<Opened />} />
    </Routes>,
  );
}

const opened = () => JSON.parse(screen.getByTestId("opened").textContent ?? "null");

beforeEach(() => {
  vi.mocked(metaReader).mockClear();
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) =>
      Promise.resolve(
        new Response(JSON.stringify(url === "/data/graph-schema" ? GRAPH : VOCAB), {
          status: 200,
        }),
      ),
    ),
  );
});
afterEach(() => vi.unstubAllGlobals());

let n = 0;
/** A fresh role a test: what a role is served is read once until the model changes. */
const freshRole = () => `aggregator-${++n}`;

const SURFACES: [string, string][] = [
  ["cypher", "Cypher"],
  ["sql", "SQL"],
  ["sparql", "SPARQL"],
  ["graphql", "GraphQL"],
  ["rest", "REST"],
  ["grpc", "gRPC"],
  ["jsonapi", "JSON:API"],
];

const COUNTS: Record<string, { to: string; state: Record<string, unknown> }> = {
  cypher: {
    to: "/graph",
    state: { query: "MATCH (n:Clients) RETURN count(n) AS count" },
  },
  sql: { to: "/sql", state: { sql: 'select count(*) from "sales"."clients"', autoRun: true } },
  sparql: {
    to: "/sparql",
    state: {
      query:
        "PREFIX sales: <https://provisa.example/sales/>\n" +
        "SELECT (COUNT(?s) AS ?count) WHERE {\n  ?s a sales:clients .\n}",
    },
  },
  graphql: {
    to: "/query",
    state: { query: "{ sales__clients_aggregate { aggregate { count } } }", autoRun: true },
  },
  rest: {
    to: "/openapi",
    state: { openApiUrl: "GET /data/rest/sales/customers?aggregate=count", autoRun: true },
  },
  grpc: {
    to: "/grpc",
    state: {
      grpcMethod: "QuerySalesClientsAggregate()",
      grpcBody: { funcs: ["count"] },
      autoRun: true,
    },
  },
  jsonapi: {
    to: "/jsonapi",
    state: { jsonapiUrl: "/data/jsonapi/sales/customers?aggregate=count", autoRun: true },
  },
};

const GROUPED: Record<string, { to: string; state: Record<string, unknown> }> = {
  cypher: {
    to: "/graph",
    state: { query: "MATCH (n:Clients) RETURN n.name AS name, count(n) AS count" },
  },
  sql: {
    to: "/sql",
    state: {
      sql: 'select "name", count(*) from "sales"."clients" group by "name"',
      autoRun: true,
    },
  },
  sparql: {
    to: "/sparql",
    state: {
      query:
        "PREFIX sales: <https://provisa.example/sales/>\n" +
        "SELECT ?v (COUNT(?s) AS ?count) WHERE {\n" +
        "  ?s a sales:clients . ?s sales:clients_name ?v .\n}\nGROUP BY ?v",
    },
  },
  graphql: {
    to: "/query",
    state: {
      query: "{ sales__clients_group_by(by: [name]) { groupKey aggregate { count } } }",
      autoRun: true,
    },
  },
  rest: {
    to: "/openapi",
    state: {
      openApiUrl: "GET /data/rest/sales/customers?aggregate=count&groupBy=full_name",
      autoRun: true,
    },
  },
  grpc: {
    to: "/grpc",
    state: {
      grpcMethod: "QuerySalesClientsGroupBy()",
      grpcBody: { by: ["full_name"], funcs: ["count"] },
      autoRun: true,
    },
  },
  jsonapi: {
    to: "/jsonapi",
    state: {
      jsonapiUrl: "/data/jsonapi/sales/customers?aggregate=count&groupBy=full_name",
      autoRun: true,
    },
  },
};

describe("a table's API section aggregate links", () => {
  it("names each surface that serves the table's aggregates under Query it", async () => {
    vi.mocked(metaReader).mockResolvedValue(reader({ aggregate: true, groupBy: false }));
    routed(<QueryRead table={customers(true, false)} actingRole={freshRole()} />);
    for (const [surface, text] of SURFACES) {
      const link = await screen.findByTestId(`table-query-aggregate-query-${surface}`);
      expect(link).toHaveAccessibleName(`Count customers in ${text}`);
      expect(link).toHaveTextContent(text);
    }
    expect(screen.getByTestId("table-query-aggregate-query-links")).toHaveTextContent(/^Query it/);
  });

  for (const [surface] of SURFACES) {
    it(`opens ${surface}'s Explore editor with the table's count, run at once`, async () => {
      vi.mocked(metaReader).mockResolvedValue(reader({ aggregate: true, groupBy: false }));
      routed(<QueryRead table={customers(true, false)} actingRole={freshRole()} />);
      fireEvent.click(await screen.findByTestId(`table-query-aggregate-query-${surface}`));
      expect(opened()).toEqual(COUNTS[surface]);
    });

    it(`opens ${surface}'s Explore editor with a group-by when group-by is enabled`, async () => {
      vi.mocked(metaReader).mockResolvedValue(reader({ aggregate: true, groupBy: true }));
      routed(<QueryRead table={customers(true, true)} actingRole={freshRole()} />);
      fireEvent.click(await screen.findByTestId(`table-query-aggregate-query-${surface}`));
      expect(opened()).toEqual(GROUPED[surface]);
    });
  }

  it("has no GraphQL, REST, JSON:API or gRPC link where the role is not served the aggregates", async () => {
    vi.mocked(metaReader).mockResolvedValue(reader({ aggregate: false, groupBy: false }));
    routed(<QueryRead table={customers(true, false)} actingRole={freshRole()} />);
    expect(await screen.findByTestId("table-query-aggregate-query-sql")).toBeInTheDocument();
    expect(await screen.findByTestId("table-query-aggregate-query-cypher")).toBeInTheDocument();
    for (const surface of ["graphql", "rest", "jsonapi", "grpc"])
      expect(screen.queryByTestId(`table-query-aggregate-query-${surface}`)).toBeNull();
  });

  it("has no row when aggregates are off", async () => {
    vi.mocked(metaReader).mockResolvedValue(reader({ aggregate: true, groupBy: true }));
    routed(<QueryRead table={customers(false, true)} actingRole={freshRole()} />);
    await waitFor(() => expect(screen.getByTestId("table-query")).toBeInTheDocument());
    expect(screen.queryByTestId("table-query-aggregate-query-links")).toBeNull();
    expect(vi.mocked(metaReader)).not.toHaveBeenCalled();
  });
});
