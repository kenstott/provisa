// Copyright (c) 2026 Kenneth Stott
// Canary: ae9be7f1-4c2b-429e-aed3-c1d94df0e0e9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The saved frames are read back from IndexedDB once, after the page has rendered, and on a busy
// browser that read can land at any moment. What the page did meanwhile stands: a frame run while
// the read was out keeps its answer when the read lands, and the saved frames join it. The
// maintainer's report (2026-11-12) was a frame left on "Running…" after its answer had come: the
// read landing put back the frame as it was when it started running.

import { describe, it, expect, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import "../i18n";

const GRAPH_STATE = "provisa.graph.state";
let releaseRead: (saved: unknown) => void = () => {};
const savedRead = new Promise<unknown>((resolve) => (releaseRead = resolve));

vi.mock("../lib/bulkStorage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/bulkStorage")>()),
  // The page's saved frames come back only when the test lets them; every other list at once.
  readBulk: (key: string) => (key === GRAPH_STATE ? savedRead : Promise.resolve(null)),
  writeBulk: () => Promise.resolve(),
}));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "all" }, selectedRoles: [{ id: "all" }], loading: false }),
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
vi.mock("../components/graph/GraphCanvas", () => ({ GraphCanvas: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const QUERY = "MATCH (n:PetStore:Pets) RETURN n.name LIMIT 25";
const SAVED_QUERY = "MATCH (n:PetStore:Owners) RETURN n.name LIMIT 25";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query: QUERY, autoRun: true } })}>
      run
    </button>
  );
}

describe("the saved frames read back while a frame runs", () => {
  it("keep the frame's answer, and join it", async () => {
    let answer: () => void = () => {};
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        if (String(url).includes("/data/cypher"))
          return new Promise<Response>((resolve) => {
            answer = () =>
              resolve({
                ok: true,
                status: 200,
                // The saved frames land just after this answer is handed to the page, before the
                // page renders it. A statement that changed the session is answered, so the page
                // has another update waiting and renders the answer later, not at once.
                json: async () => {
                  releaseRead({
                    frames: [
                      {
                        id: "saved-frame",
                        query: SAVED_QUERY,
                        status: "done",
                        nodes: [],
                        edges: [],
                        rows: [],
                        columns: ["n.name"],
                      },
                    ],
                    history: [SAVED_QUERY],
                  });
                  return {
                    results: [
                      { statement: QUERY, status: 200, body: { rows: [], columns: ["n.name"] } },
                    ],
                    session_changed: true,
                  };
                },
              } as unknown as Response);
          });
        if (String(url).includes("/data/graph-schema"))
          return json({ node_labels: [], relationship_types: [] });
        return json({});
      }),
    );
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <Run />
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("run"));
    expect((await screen.findAllByText("Running…")).length).toBeGreaterThan(0);

    await act(async () => answer());

    await waitFor(() => {
      expect(screen.queryByText("Running…")).toBeNull();
      expect(screen.getAllByText("No rows")).toHaveLength(2);
    });
    expect(screen.getAllByText(QUERY, { exact: false }).length).toBeGreaterThan(0);
    expect(screen.getAllByText(SAVED_QUERY, { exact: false }).length).toBeGreaterThan(0);
  });
});
