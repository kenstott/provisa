// Copyright (c) 2026 Kenneth Stott
// Canary: 5b9e2d71-c4a3-4f86-9e10-3d7a8c62f1b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252 (Amended 2026-11-22): a table's query links appear as soon as it is registered. The
// server rebuilds every role's schemas before it answers the registration; the page's reads of
// what the role is served (its GraphQL schema, graph schema and SPARQL vocabulary) are read again
// on that write, not kept for the page's life, so a table registered after they were first read
// shows its links without a reload -- and loses them once it is deleted.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { DeleteTable, RegisterTable } from "../../../hooks/admin.graphql";
import { useDeleteTable, useRegisterTable } from "../../../hooks/useTableQueries";
import type { RegisteredTable, Source } from "../../../types/admin";
import { IdentityRead } from "../TableSectionsRead";

/** sales.tickets: registered while the page is open. */
const TICKETS = {
  id: 7,
  sourceId: "pg",
  schemaName: "public",
  tableName: "tickets",
  domainId: "sales",
  alias: null,
  gqlNamingConvention: null,
  description: null,
  draft: false,
  region: null,
  apiEndpoint: null,
} as unknown as RegisteredTable;

const PG = { id: "pg", type: "postgresql" } as Source;
const INPUT = { sourceId: "pg", schemaName: "public", tableName: "tickets", domainId: "sales" };
const OK = { success: true, message: "ok", code: null, params: null };

/** What the server serves the role: tickets only while it is registered. */
let registered = false;

const scalar = (name: string) => ({ kind: "SCALAR", name });
const list = (name: string) => ({ kind: "LIST", name: null, ofType: { kind: "OBJECT", name } });

function introspection() {
  return {
    data: {
      __schema: {
        queryType: { name: "Query" },
        types: [
          {
            name: "Query",
            fields: registered ? [{ name: "sales__tickets", type: list("SalesTickets") }] : [],
          },
          { name: "SalesTickets", fields: [{ name: "id", type: scalar("Int") }] },
        ],
      },
    },
  };
}

function graphSchema() {
  return {
    node_labels: registered
      ? [{ label: "Tickets", table_id: 7, pk_columns: ["id"], properties: ["id"] }]
      : [],
    relationship_types: [],
    root_fields: registered
      ? [{ domain_id: "sales", table_name: "tickets", graphql_field: "sales__tickets" }]
      : [],
  };
}

const VOCABULARY = {
  base: "https://provisa.example/",
  prefixes: {},
  classes: [],
  predicates: [],
  edges: [],
  concepts: [],
};

function serve() {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) => {
      const body =
        url === "/data/graphql"
          ? introspection()
          : url === "/data/graph-schema"
            ? graphSchema()
            : VOCABULARY;
      return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
    }),
  );
}

function Writes() {
  const { registerTable } = useRegisterTable();
  const { deleteTable } = useDeleteTable();
  return (
    <>
      <button
        type="button"
        onClick={() => {
          registered = true;
          void registerTable(INPUT);
        }}
      >
        register
      </button>
      <button
        type="button"
        onClick={() => {
          registered = false;
          void deleteTable(7);
        }}
      >
        delete
      </button>
    </>
  );
}

const MOCKS = [
  {
    request: { query: RegisterTable, variables: { input: INPUT } },
    result: { data: { registerTable: { __typename: "MutationResult", ...OK } } },
  },
  {
    request: { query: DeleteTable, variables: { id: 7 } },
    result: { data: { deleteTable: { __typename: "MutationResult", ...OK } } },
  },
];

afterEach(() => {
  vi.unstubAllGlobals();
  registered = false;
});

describe("a just-registered table's Identity query links", () => {
  it("appear once the table is registered, and go once it is deleted, without a reload", async () => {
    serve();
    render(
      <>
        <IdentityRead table={TICKETS} source={PG} actingRole="steward-new-table" />
        <Writes />
      </>,
      { mocks: MOCKS },
    );
    // Read before the registration (its schema, its root fields, its graph schema and its
    // vocabulary): the role is served no tickets, so there are no links.
    await waitFor(() => expect(vi.mocked(fetch)).toHaveBeenCalledTimes(4));
    expect(screen.queryByTestId("table-identity-query-links")).toBeNull();

    fireEvent.click(screen.getByText("register"));
    expect(await screen.findByTestId("table-identity-query-sql")).toBeInTheDocument();
    expect(screen.getByTestId("table-identity-query-cypher")).toBeInTheDocument();
    expect(screen.getByTestId("table-identity-query-graphql")).toBeInTheDocument();

    fireEvent.click(screen.getByText("delete"));
    await waitFor(() => expect(screen.queryByTestId("table-identity-query-links")).toBeNull());
  });
});
