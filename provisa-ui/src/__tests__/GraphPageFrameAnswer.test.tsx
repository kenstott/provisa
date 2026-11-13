// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2d47b1-c063-4f9a-a51e-2b9d70c3f816
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A frame says what state its query is in, and only that: running while no answer has come (and
// no "No rows", which is an answer), "No rows" without "Running" once an empty answer has, and the
// error once a failed one has. The maintainer's report showed "Running…" above "No rows".

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import "../i18n";

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
import { graphStateOf } from "../components/graph/graph-persistence";

const graphState = graphStateOf("cypher");
const QUERY = "MATCH (n:PetStore:Pets) RETURN n.name LIMIT 25";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

function serve(cypher: () => Promise<Response>) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      if (String(url).includes("/data/cypher")) return cypher();
      if (String(url).includes("/data/graph-schema"))
        return json({ node_labels: [], relationship_types: [] });
      return json({});
    }),
  );
}

function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query: QUERY, autoRun: true } })}>
      run
    </button>
  );
}

function runQuery() {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <Run />
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
  fireEvent.click(screen.getByText("run"));
}

describe("a frame's running state", () => {
  beforeEach(() => {
    localStorage.clear();
    graphState.frames = [];
  });

  it("says running, and no 'No rows', while no answer has come", async () => {
    let answer: (r: Response) => void = () => {};
    serve(() => new Promise<Response>((resolve) => (answer = resolve)));
    runQuery();
    expect((await screen.findAllByText("Running…")).length).toBeGreaterThan(0);
    expect(screen.queryByText("No rows")).toBeNull();
    answer(json({ results: [] }));
    await waitFor(() => expect(screen.queryByText("Running…")).toBeNull());
  });

  it("shows 'No rows' without 'Running' after an empty answer", async () => {
    serve(async () =>
      json({
        results: [{ statement: QUERY, status: 200, body: { rows: [], columns: ["n.name"] } }],
      }),
    );
    runQuery();
    expect(await screen.findByText("No rows")).toBeInTheDocument();
    expect(screen.queryByText("Running…")).toBeNull();
  });

  it("shows the error, without 'Running', after a failed answer", async () => {
    serve(async () =>
      json({ detail: "cypher_http request timed out after 30s", code: "data.query_timeout" }, 504),
    );
    runQuery();
    expect(await screen.findByTestId("graph-frame-error")).toBeInTheDocument();
    expect(screen.queryByText("Running…")).toBeNull();
    expect(screen.queryByText("No rows")).toBeNull();
  });

  it("shows the error when the request itself fails", async () => {
    serve(async () => {
      throw new TypeError("Failed to fetch");
    });
    runQuery();
    expect(await screen.findByTestId("graph-frame-error")).toHaveTextContent("Failed to fetch");
    expect(screen.queryByText("Running…")).toBeNull();
  });
});
