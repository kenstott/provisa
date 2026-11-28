// Copyright (c) 2026 Kenneth Stott
// Canary: cb6f9568-ce06-47b6-a799-80e6dda90c28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every query forwarded into the Cypher explorer runs at once, with no flag
// asking it to, exactly once per navigation: not again on a re-render, nor on back and forward.

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
});
