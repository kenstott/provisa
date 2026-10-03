// Copyright (c) 2026 Kenneth Stott
// Canary: 0b2237c2-928a-40da-b60f-9ba19cabb5cc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Cypher query handed to the graph explorer with autoRun (NL "Open in Cypher", Polly) runs,
// whether the page was just opened or was already open.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "org_admin" },
    selectedRoles: [{ id: "org_admin" }],
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
}));
vi.mock("../components/graph/GraphFrame", () => ({ GraphFrame: () => null }));
vi.mock("../components/graph/GraphSidebar", () => ({ Sidebar: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const cypherCalls: string[] = [];

function Polly() {
  const navigate = useNavigate();
  return (
    <button
      onClick={() =>
        navigate("/graph", { state: { query: "MATCH (n) RETURN n LIMIT 1", autoRun: true } })
      }
    >
      polly
    </button>
  );
}

describe("GraphPage — a hand-off while the page is open", () => {
  beforeEach(() => {
    localStorage.clear();
    cypherCalls.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (String(url).includes("/data/cypher")) cypherCalls.push(String(init?.body));
        return new Response(
          JSON.stringify({ columns: [], data: [], nodes: [], relationships: [] }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      }),
    );
  });

  it("runs each handed query", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Polly />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    await new Promise((r) => setTimeout(r, 50));
    expect(cypherCalls).toHaveLength(0);

    act(() => screen.getByText("polly").click());
    await waitFor(() =>
      expect(cypherCalls.some((b) => b.includes("MATCH (n) RETURN n LIMIT 1"))).toBe(true),
    );
    const after = cypherCalls.length;

    act(() => screen.getByText("polly").click());
    await waitFor(() => expect(cypherCalls.length).toBeGreaterThan(after));
  });
});
