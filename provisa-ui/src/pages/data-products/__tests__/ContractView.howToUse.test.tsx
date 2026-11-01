// Copyright (c) 2026 Kenneth Stott
// Canary: 7a0e3d52-b1c6-4f84-9d27-58c2e6f1a430
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a contract's How to use, for one table: each surface's most specific address, how it is
// used and a link to its example; that it works only once access is granted and who grants it; and
// who to ask in place of the link, for a role that cannot read the table.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useLocation } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { ContractView } from "../ContractView";
import { contractGrant, surfaceAddress } from "../howToUse";
import { sparqlQueryOf } from "../useSparqlExample";
import { graphqlExample, grpcExample, restExample, sqlExample } from "../tableExamples";
import type { MetaReader } from "../../../api/metaGraph";
import type { OdcsContract } from "../../../api/dataProductContracts";
import type { SparqlVocabulary } from "../../../components/graph/sparql";

const HOST = { host: "data.example.com" };
const SQL = { database: "provisa", schema: "sales" };
const api = (server: string, location: string) => ({
  server,
  type: "api",
  description: "",
  location,
});
const WEB = "https://data.example.com/data";

const CONTRACT = {
  id: "c1",
  version: "1.0.0",
  status: "active",
  name: "orders",
  roles: [
    { role: "analyst", access: "read", firstLevelApprovers: "owner@example.com, lead@example.com" },
    { role: "finance", access: "write", firstLevelApprovers: "owner@example.com" },
  ],
  servers: [
    { server: "pgwire", type: "postgres", ...HOST, port: 5439, ...SQL },
    { server: "flight-sql", type: "custom", ...HOST, port: 8815, ...SQL },
    { server: "grpc", type: "custom", ...HOST, port: 50051, serviceName: "provisa.Query" },
    api("graphql", `${WEB}/graphql`),
    api("rest", `${WEB}/rest/sales/orders`),
    api("openapi", `${WEB}/rest/openapi.json?domains=sales&tables=orders`),
    api("jsonapi", `${WEB}/jsonapi/sales/orders`),
    api("sparql", `${WEB}/sparql`),
  ],
} as unknown as OdcsContract;

const TABLE = { domainId: "sales", tableName: "orders", alias: null };
const READER = {
  roots: new Map([["sales.orders", "sales__orders"]]),
  schema: { rootType: () => "SalesOrders", scalars: () => ["id", "amount"] },
} as unknown as MetaReader;

const VOCABULARY: SparqlVocabulary = {
  base: "https://example.com/",
  prefixes: { sales: "https://example.com/sales/" },
  classes: [
    {
      iri: "https://example.com/sales/orders",
      kind: "table",
      labels: ["Sales:Orders"],
      parameterized: false,
      parameterized_labels: [],
      superclasses: [],
    },
  ],
  predicates: [],
  edges: [],
  concepts: [],
};
const GRAPH = { node_labels: [{ label: "Sales:Orders", table_id: 7 }] };

function Landed() {
  const { pathname, state } = useLocation();
  return <output data-testid="landed">{JSON.stringify({ pathname, state })}</output>;
}

function show(
  held: string[],
  over: Partial<OdcsContract> = {},
  reader: MetaReader | null = READER,
) {
  render(
    <>
      <ContractView
        contract={{ ...CONTRACT, ...over }}
        versions={[]}
        tableId={7}
        promises={[]}
        table={TABLE}
        reader={reader}
        viewer={{ held: new Set(held), acting: held.join(",") || "guest" }}
      />
      <Landed />
    </>,
  );
}

const row = (server: string) => screen.getByTestId(`data-contract-how-to-use-7-${server}`);
const cells = (server: string) =>
  Array.from(row(server).querySelectorAll("td")).map((cell) => cell.textContent);
const link = (server: string) => screen.queryByTestId(`data-contract-how-to-use-7-${server}-link`);

function landing(server: string) {
  fireEvent.click(link(server) as HTMLElement);
  return JSON.parse(screen.getByTestId("landed").textContent as string);
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) =>
      Promise.resolve(
        new Response(JSON.stringify(url === "/data/graph-schema" ? GRAPH : VOCABULARY)),
      ),
    ),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("How to use", () => {
  it("replaces Servers, with a surface, address, sentence and example column", () => {
    show(["analyst"]);
    expect(screen.queryByTestId("data-contract-7-servers")).toBeNull();
    expect(screen.getByTestId("data-contract-7-howToUse")).toHaveTextContent("How to use");
    const table = screen.getByTestId("data-contract-how-to-use-7");
    expect(Array.from(table.querySelectorAll("th")).map((h) => h.textContent)).toEqual([
      "Surface",
      "Address",
      "How it is used",
      "Example",
    ]);
    expect(table.querySelectorAll("tbody tr")).toHaveLength(8);
  });

  it("shows the most specific address each surface has for the table", () => {
    show(["analyst"]);
    const address = (server: string) => cells(server)[1];
    expect(address("rest")).toBe(`${WEB}/rest/sales/orders`);
    expect(address("jsonapi")).toBe(`${WEB}/jsonapi/sales/orders`);
    expect(address("openapi")).toBe(`${WEB}/rest/openapi.json?domains=sales&tables=orders`);
    expect(address("pgwire")).toBe(
      "postgresql://data.example.com:5439/provisa?options=-csearch_path%3Dsales",
    );
    expect(address("flight-sql")).toBe(
      "grpc://data.example.com:8815?database=provisa&schema=sales",
    );
    expect(address("graphql")).toBe(`${WEB}/graphql`);
    expect(address("sparql")).toBe(`${WEB}/sparql`);
    expect(address("grpc")).toBe("data.example.com:50051");
  });

  it("says how a surface is used, naming the database and schema", () => {
    show(["analyst"]);
    expect(cells("pgwire")[2]).toContain(
      "Any PostgreSQL client, such as psql or DBeaver. Database provisa, schema sales.",
    );
    expect(cells("grpc")[2]).toContain("service provisa.Query");
  });

  it("shows a token only as a placeholder", () => {
    show(["analyst"]);
    expect(cells("rest")[2]).toContain("Bearer <token>");
    const text = screen.getByTestId("data-contract-7-howToUse").textContent as string;
    expect(text.replaceAll("Bearer <token>", "")).not.toMatch(/Bearer|provisa_pat_/);
  });

  it.each([["analyst"], ["guest"]])("says who grants access on every surface, for %s", (held) => {
    show([held]);
    for (const server of (CONTRACT.servers ?? []).map((s) => String(s.server))) {
      expect(screen.getByTestId(`data-contract-how-to-use-7-${server}-access`)).toHaveTextContent(
        "Works only once access is granted, by owner@example.com, lead@example.com.",
      );
    }
  });
});

describe("a role that can read the table", () => {
  it("opens each example on its Explore page, as Access Patterns does", async () => {
    show(["analyst"]);
    expect(landing("pgwire")).toEqual({
      pathname: "/sql",
      state: { sql: sqlExample(TABLE).state.sql, autoRun: true },
    });
    expect(landing("flight-sql").pathname).toBe("/sql");
    expect(landing("flight-sql").state).toEqual(sqlExample(TABLE).state);
    expect(landing("graphql")).toEqual({
      pathname: "/query",
      state: { query: "{ sales__orders { id amount } }", autoRun: true },
    });
    expect(landing("rest")).toEqual({
      pathname: "/openapi",
      state: { openApiUrl: "GET /data/rest/sales__orders", autoRun: true },
    });
    expect(landing("openapi").pathname).toBe("/openapi");
    expect(landing("jsonapi")).toEqual({
      pathname: "/jsonapi",
      state: { jsonapiUrl: "/data/jsonapi/sales/orders", autoRun: true },
    });
    expect(landing("grpc")).toEqual({
      pathname: "/grpc",
      state: { grpcMethod: "QuerySalesOrders()", autoRun: true },
    });
    await waitFor(() => expect(link("sparql")).toBeInTheDocument());
    const sparql = landing("sparql");
    expect(sparql.pathname).toBe("/sparql");
    expect(sparql.state.autoRun).toBe(true);
    expect(sparql.state.query).toContain("?s a sales:orders");
  });

  it("asks as the acting role for the SPARQL example", async () => {
    show(["analyst"]);
    await waitFor(() => expect(link("sparql")).toBeInTheDocument());
    expect(fetch).toHaveBeenCalledWith("/data/sparql/vocabulary", {
      headers: { "X-Provisa-Role": "analyst" },
    });
  });

  it("gives a surface with no example for this table no link", () => {
    show(["analyst"], {}, null);
    for (const server of ["graphql", "rest", "openapi", "grpc"]) {
      expect(link(server)).toBeNull();
      expect(cells(server)[3]).toBe("—");
    }
    expect(link("pgwire")).toBeInTheDocument();
    expect(link("jsonapi")).toBeInTheDocument();
  });

  it("reads through any role the contract names", () => {
    show(["finance"]);
    expect(link("pgwire")).toBeInTheDocument();
  });

  it("leaves SPARQL without a link when the role's vocabulary has no class for the table", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(new Response("{}", { status: 403 }))),
    );
    show(["analyst"]);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    expect(link("sparql")).toBeNull();
  });
});

describe("a role that cannot read the table", () => {
  it("names who to ask in place of every link", () => {
    show(["guest"]);
    for (const server of (CONTRACT.servers ?? []).map((s) => String(s.server))) {
      expect(link(server)).toBeNull();
      expect(screen.getByTestId(`data-contract-how-to-use-7-${server}-ask`)).toHaveTextContent(
        "Ask owner@example.com, lead@example.com for access",
      );
    }
  });

  it("reads nothing to build an example it would not show", () => {
    show(["guest"]);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("names a role's holders where the contract names no approver", () => {
    show(["guest"], { roles: [{ role: "analyst", access: "read" }] });
    expect(screen.getByTestId("data-contract-how-to-use-7-pgwire-ask")).toHaveTextContent(
      "Ask a holder of analyst for access",
    );
    expect(screen.getByTestId("data-contract-how-to-use-7-pgwire-access")).toHaveTextContent(
      "Works only once access is granted, by a holder of analyst. This contract names no approver.",
    );
  });

  it("says so where the contract names no one", () => {
    show(["guest"], { roles: [] });
    expect(screen.getByTestId("data-contract-how-to-use-7-pgwire-ask")).toHaveTextContent(
      "This contract names no one to ask",
    );
    expect(screen.getByTestId("data-contract-how-to-use-7-pgwire-access")).toHaveTextContent(
      "This contract names no role or approver.",
    );
  });
});

describe("what How to use reads from a contract", () => {
  it("escapes the schema into a connection URI", () => {
    const server = { server: "pgwire", ...HOST, port: 5439, database: "provisa", schema: "a b" };
    expect(surfaceAddress(server)).toBe(
      "postgresql://data.example.com:5439/provisa?options=-csearch_path%3Da%20b",
    );
  });

  it("names each approver once", () => {
    expect(contractGrant(CONTRACT).approvers).toEqual(["owner@example.com", "lead@example.com"]);
  });

  it("builds a table's SPARQL example only from a class of its own", () => {
    expect(sparqlQueryOf(7, GRAPH, VOCABULARY)).toContain("?s a sales:orders");
    expect(sparqlQueryOf(8, GRAPH, VOCABULARY)).toBeNull();
    const classes = VOCABULARY.classes.map((c) => ({ ...c, parameterized: true }));
    expect(sparqlQueryOf(7, GRAPH, { ...VOCABULARY, classes })).toBeNull();
  });

  it("has no example on a surface the role's schema does not name", () => {
    expect(graphqlExample(null, TABLE)).toBeNull();
    expect(restExample(null, TABLE)).toBeNull();
    expect(grpcExample(null, TABLE)).toBeNull();
  });
});
