// Copyright (c) 2026 Kenneth Stott
// Canary: e4a81c37-90d2-4b6f-8c15-2f7d93a6b048
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252 (Amended 2026-11-18, 2026-11-22): a table's Identity section has one link per query
// surface the table is exposed on, drawn as a relationship's detail panel draws its links ("Query
// it", then each surface's glyph with its name beside it); each opens that surface's Explore editor
// with a query that reads the table, run at once, by the names the role sees and with a row limit.
// A surface the table is not exposed on shows no link.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement } from "react";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { RegisteredTable, Source } from "../../../types/admin";
import { metaReader } from "../../../api/metaGraph";
import type { GraphSchemaDoc } from "../../../components/relationships/relationshipQueries";
import {
  GRAPH,
  VOCABULARY,
  reader,
} from "../../../components/relationships/__tests__/relationshipQueryFixtures";
import { IdentityRead } from "../TableSectionsRead";

vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: vi.fn(),
}));

/** sales.customers, served to the role as sales__clients and the Clients node label. */
const CUSTOMERS = {
  id: 2,
  sourceId: "pg",
  schemaName: "public",
  tableName: "customers",
  domainId: "sales",
  alias: "clients",
  gqlNamingConvention: null,
  description: null,
  draft: false,
  region: null,
  apiEndpoint: null,
} as unknown as RegisteredTable;

const PG = { id: "pg", type: "postgresql" } as Source;

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

function serve(graph: GraphSchemaDoc) {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) =>
      Promise.resolve(
        new Response(JSON.stringify(url === "/data/graph-schema" ? graph : VOCABULARY), {
          status: 200,
        }),
      ),
    ),
  );
}

let n = 0;
let role = "";
beforeEach(() => {
  vi.mocked(metaReader).mockClear();
  vi.mocked(metaReader).mockImplementation(() => Promise.resolve(reader()));
  // A fresh role a test: the surfaces are read once per role for the page's life.
  n += 1;
  role = `analyst-${n}`;
});
afterEach(() => vi.unstubAllGlobals());

const SURFACES: [string, string][] = [
  ["cypher", "Cypher"],
  ["sql", "SQL"],
  ["sparql", "SPARQL"],
  ["graphql", "GraphQL"],
  ["rest", "REST"],
  ["grpc", "gRPC"],
  ["jsonapi", "JSON:API"],
];

const EXPECTED: Record<string, { to: string; state: Record<string, unknown> }> = {
  cypher: { to: "/graph", state: { query: "MATCH (n:Clients) RETURN n LIMIT 25", autoRun: true } },
  sql: { to: "/sql", state: { sql: 'select * from "sales"."clients" limit 25', autoRun: true } },
  sparql: {
    to: "/sparql",
    state: {
      query:
        "PREFIX sales: <https://provisa.example/sales/>\n\n" +
        "SELECT * WHERE {\n  ?s a sales:clients . ?s ?p ?o .\n}\nLIMIT 25",
      autoRun: true,
    },
  },
  graphql: {
    to: "/query",
    state: { query: "{ sales__clients(limit: 25) { id name } }", autoRun: true },
  },
  rest: {
    to: "/openapi",
    state: { openApiUrl: "GET /data/rest/sales/customers?limit=25", autoRun: true },
  },
  grpc: {
    to: "/grpc",
    state: { grpcMethod: "QuerySalesClients()", grpcBody: { limit: 25 }, autoRun: true },
  },
  jsonapi: {
    to: "/jsonapi",
    state: { jsonapiUrl: "/data/jsonapi/sales/customers?page%5Bsize%5D=25", autoRun: true },
  },
};

describe("a table's Identity section query links", () => {
  it("links the table on every surface it is exposed on, each named and tipped by its surface", async () => {
    serve(GRAPH);
    routed(<IdentityRead table={CUSTOMERS} source={PG} actingRole={role} />);
    for (const [surface, text] of SURFACES) {
      const link = await screen.findByTestId(`table-identity-query-${surface}`);
      expect(link).toHaveAccessibleName(`Read customers in ${text}`);
      // Drawn as a relationship's: the surface's name beside its glyph, not an icon alone.
      expect(link).toHaveTextContent(text);
    }
    expect(screen.getByTestId("table-identity-query-links")).toHaveTextContent(/^Query it/);
  });

  for (const [surface] of SURFACES) {
    it(`opens ${surface}'s Explore editor with a limited read of the table, run at once`, async () => {
      serve(GRAPH);
      routed(<IdentityRead table={CUSTOMERS} source={PG} actingRole={role} />);
      fireEvent.click(await screen.findByTestId(`table-identity-query-${surface}`));
      const got = opened();
      if (surface === "sparql") {
        expect(got.to).toBe(EXPECTED.sparql.to);
        expect(got.state.autoRun).toBe(true);
        expect(got.state.query).toContain("?s a sales:clients . ?s ?p ?o .");
        expect(got.state.query).toContain("LIMIT 25");
      } else {
        expect(got).toEqual(EXPECTED[surface]);
      }
    });
  }

  it("has no Cypher or SPARQL link for a table excluded from Cypher", async () => {
    serve({ ...GRAPH, node_labels: GRAPH.node_labels.filter((l) => l.table_id !== 2) });
    routed(<IdentityRead table={CUSTOMERS} source={PG} actingRole={role} />);
    expect(await screen.findByTestId("table-identity-query-jsonapi")).toBeInTheDocument();
    for (const surface of ["sql", "graphql", "rest", "grpc"])
      expect(screen.getByTestId(`table-identity-query-${surface}`)).toBeInTheDocument();
    expect(screen.queryByTestId("table-identity-query-cypher")).toBeNull();
    expect(screen.queryByTestId("table-identity-query-sparql")).toBeNull();
  });

  it("has no links, and no row for them, for a table the role is not served", async () => {
    serve({ ...GRAPH, node_labels: [] });
    const unserved = { ...CUSTOMERS, id: 99, tableName: "ledger" } as RegisteredTable;
    routed(<IdentityRead table={unserved} source={PG} actingRole={role} />);
    await waitFor(() => expect(vi.mocked(metaReader)).toHaveBeenCalled());
    await waitFor(() => expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2));
    expect(screen.queryByTestId("table-identity-query-links")).toBeNull();
    expect(screen.queryByText("Query it")).toBeNull();
  });

  it("has no links where no acting role is given (the edit form)", () => {
    serve(GRAPH);
    routed(<IdentityRead table={CUSTOMERS} source={PG} />);
    expect(screen.queryByTestId("table-identity-query-links")).toBeNull();
    expect(vi.mocked(metaReader)).not.toHaveBeenCalled();
  });
});
