// Copyright (c) 2026 Kenneth Stott
// Canary: c8f03b72-5e19-4d46-a7b0-92d1e6f4a385
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2176: the Cypher page lists the user's graph session in its sidebar, and reads it again only
// when a statement answers that it changed the session. The session endpoint is stubbed.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import "../i18n";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }],
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
}));
vi.mock("../components/graph/GraphFrame", () => ({ GraphFrame: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const sent: { url: string; method: string; body?: string }[] = [];
let sessionChanged = true;
let session = {
  labels: [{ label: "Draft", nodes: 2, tagged_model_nodes: 0 }],
  types: [{ type: "LINKS", relationships: 1 }],
  empty: false,
};

function Run({ query }: { query: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query, autoRun: true } })}>{query}</button>
  );
}

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

beforeEach(() => {
  localStorage.clear();
  sent.length = 0;
  sessionChanged = true;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const u = String(url);
      sent.push({ url: u, method: init?.method ?? "GET", body: init?.body as string | undefined });
      if (u.endsWith("/data/cypher/session")) return json(session);
      if (u.endsWith("/data/cypher"))
        return json({
          results: [{ statement: "q", status: 200, body: { rows: [], columns: [] } }],
          ...(sessionChanged ? { session_changed: true } : {}),
        });
      if (u.includes("/data/graph-schema"))
        return json({ node_labels: [], relationship_types: [] });
      return json({});
    }),
  );
});

const reads = () =>
  sent.filter((s) => s.url.endsWith("/data/cypher/session") && s.method === "GET").length;

describe("the Cypher page's graph session", () => {
  it("lists the session in the sidebar, beside the model's labels", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    expect(await screen.findByTestId("graph-session-label-Draft")).toBeInTheDocument();
    expect(screen.getByTestId("graph-session-type-LINKS")).toBeInTheDocument();
  });

  it("runs the same MATCH as a model pill when a session pill is clicked", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(await screen.findByTestId("graph-session-label-Draft"));
    await waitFor(() =>
      expect(
        sent.some(
          (s) =>
            s.url.endsWith("/data/cypher") && s.body?.includes("MATCH (n:Draft) RETURN n LIMIT 25"),
        ),
      ).toBe(true),
    );
  });

  it("reads the session again only after a statement that changed it", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run query="CREATE (:Draft)" />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    await screen.findByTestId("graph-session-label-Draft");
    expect(reads()).toBe(1);
    session = { ...session, labels: [{ label: "Fresh", nodes: 1, tagged_model_nodes: 0 }] };
    act(() => screen.getByText("CREATE (:Draft)").click());
    await screen.findByTestId("graph-session-label-Fresh");
    expect(reads()).toBe(2);
  });

  it("does not read it again after a statement that did not change it", async () => {
    sessionChanged = false;
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run query="MATCH (n) RETURN n" />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    await screen.findByTestId("graph-session-label-Fresh");
    const before = reads();
    act(() => screen.getByText("MATCH (n) RETURN n").click());
    await waitFor(() =>
      expect(sent.some((s) => s.url.endsWith("/data/cypher") && s.method === "POST")).toBe(true),
    );
    await new Promise((r) => setTimeout(r, 100));
    expect(reads()).toBe(before);
  });
});
