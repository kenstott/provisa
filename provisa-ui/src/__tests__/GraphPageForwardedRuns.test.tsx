// Copyright (c) 2026 Kenneth Stott
// Canary: cb6f9568-ce06-47b6-a799-80e6dda90c28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-28): every query forwarded into the Cypher explorer runs at once, with
// no flag asking it to, exactly once per navigation: not again on a re-render, nor on back and
// forward. A held one (a command that writes, or parameter placeholders to fill) opens unrun.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, Route, Routes, useNavigate } from "react-router-dom";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "org_admin" },
    selectedRoles: [{ id: "org_admin" }],
    actingRoles: "org_admin",
    loading: false,
  }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useTables: () => ({ tables: [], loading: false }),
  useCompileQuery: () => ({ compileQuery: vi.fn() }),
}));
vi.mock("../components/graph/GraphFrame", () => ({ GraphFrame: () => null }));
vi.mock("../components/graph/GraphSidebar", () => ({ Sidebar: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";
import { OpenAsQuery } from "../components/graph3d/OpenAsQuery";
import { ChatArtifact } from "../components/ChatArtifact";
import {
  commandQueryLink,
  type CommandCall,
  type ServedCommand,
} from "../components/commands/commandQueries";
import { explorerHandoff } from "../lib/explorerHandoff";

const QUERY = "MATCH (t:Ontology:Terms)-[r:KIND_OF]->(b:Ontology:Terms) RETURN t, r, b";

const cypherCalls: string[] = [];
const runsOf = (query: string) => cypherCalls.filter((b) => b.includes(query)).length;

/** Back and forward through the history, as the browser's buttons do. */
function History() {
  const navigate = useNavigate();
  return (
    <>
      <button onClick={() => navigate(-1)}>back</button>
      <button onClick={() => navigate(1)}>forward</button>
    </>
  );
}

function app(from: React.ReactElement) {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/from"]}>
        <History />
        <Routes>
          <Route path="/from" element={from} />
          <Route path="/graph" element={<GraphPage />} />
        </Routes>
      </MemoryRouter>
    </MantineProvider>,
  );
}

/** A button that hands ``state`` to the Graph page. */
function HandOff({ state }: { state: Record<string, unknown> }) {
  const navigate = useNavigate();
  return <button onClick={() => navigate("/graph", { state })}>hand off</button>;
}

const SERVED: ServedCommand = {
  command: "refund_order",
  domain_id: "sales",
  graphql_root: "Mutation",
  graphql_field: "sales__refundOrder",
  names: { sql: "refund_order", cypher: "refund_order", rest: "refund_order" },
};
const callOf = (kind: CommandCall["kind"]): CommandCall => ({
  name: "refund_order",
  kind,
  arguments: [{ name: "order_id", type: "Int" }],
});
const commandState = (kind: CommandCall["kind"]) =>
  commandQueryLink("cypher", callOf(kind), SERVED, null, {
    order_id: { kind: "scalar", value: 7 },
  }).state;

/** Settle, then say whether ``query`` is in the editor and never ran. */
async function openedUnrun(query: string) {
  await waitFor(() =>
    expect(screen.getByTestId("graph-query-block")).toHaveTextContent(query),
  );
  await act(() => new Promise((r) => setTimeout(r, 50)));
  expect(runsOf(query)).toBe(0);
}

/** Settle, then say whether ``query`` ran exactly once. */
async function ranOnce(query: string) {
  await waitFor(() => expect(runsOf(query)).toBe(1));
  await act(() => new Promise((r) => setTimeout(r, 50)));
  expect(runsOf(query)).toBe(1);
}

describe("GraphPage — a forwarded query runs once, unasked", () => {
  beforeEach(() => {
    localStorage.clear();
    cypherCalls.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (String(url).includes("/data/cypher")) cypherCalls.push(String(init?.body));
        return new Response(
          JSON.stringify({
            results: [
              {
                statement: "MATCH",
                status: 200,
                body: { columns: [], data: [], nodes: [], relationships: [] },
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }),
    );
  });

  it("runs the 3D graph's Open as query → Cypher", async () => {
    app(<OpenAsQuery query={QUERY} language="cypher" focused={false} testId="x" />);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    fireEvent.click(await screen.findByTestId("x-open-as-cypher"));
    await ranOnce(QUERY);
  });

  it("runs the chat's Open on the graph page", async () => {
    const cypher = "MATCH (n:Shop:Customers) RETURN n LIMIT 25";
    app(
      <ChatArtifact
        artifact={{ kind: "graph", png: "AAAA", cypher, nodes: 1, relationships: 0 }}
      />,
    );
    fireEvent.click(screen.getByTestId("chat-graph-open"));
    await ranOnce(cypher);
  });

  it("does not run it again on back and forward", async () => {
    app(<OpenAsQuery query={QUERY} language="cypher" focused={false} testId="x" />);
    fireEvent.click(screen.getByTestId("x-open-as-query"));
    fireEvent.click(await screen.findByTestId("x-open-as-cypher"));
    await ranOnce(QUERY);
    act(() => screen.getByText("back").click());
    await screen.findByTestId("x-open-as-query");
    act(() => screen.getByText("forward").click());
    await ranOnce(QUERY);
  });

  it("runs a command's read CALL once", async () => {
    app(<HandOff state={commandState("query")} />);
    fireEvent.click(screen.getByText("hand off"));
    await ranOnce("CALL refund_order(7)");
  });

  it("opens a command's write CALL unrun, waiting for Run", async () => {
    app(<HandOff state={commandState("mutation")} />);
    fireEvent.click(screen.getByText("hand off"));
    await openedUnrun("CALL refund_order(7)");
  });

  it("opens a query held for its parameter placeholders unrun, waiting for Run", async () => {
    const query = "MATCH (n:Clients) WHERE n._nf_region = '<region>' RETURN n LIMIT 25";
    app(<HandOff state={explorerHandoff("/graph", { query }, "parameters")} />);
    fireEvent.click(screen.getByText("hand off"));
    await openedUnrun(query);
  });
});
