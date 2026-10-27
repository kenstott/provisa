// Copyright (c) 2026 Kenneth Stott
// Canary: 875f9826-1804-4660-970a-c6d5b0e5f3fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-778: a refused Cypher statement is answered in the app's error body. The frame says the
// reason (by its code, in the reader's language) and lists each access violation under it --
// never a bare "cypher failed (403)".

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
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

const VIOLATIONS = [
  { code: "column_not_visible", message: "Column pets.price is not visible to role all" },
  { code: "table_not_visible", message: "Table owners is not visible to role all" },
];

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

function serve(cypher: () => Response) {
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

function Run({ query }: { query: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query, autoRun: true } })}>run</button>
  );
}

function runQuery(query: string) {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <Run query={query} />
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
  fireEvent.click(screen.getByText("run"));
}

const refusal = {
  detail: VIOLATIONS.map((v) => v.message).join("; "),
  code: "cypher.access_violations",
  params: { count: 2 },
  violations: VIOLATIONS,
};

describe("GraphPage — a refused statement (REQ-778)", () => {
  beforeEach(() => {
    localStorage.clear();
    // The page keeps its frames across mounts: each test starts with none.
    graphState.frames = [];
  });

  it("says the refusal and lists each access violation under it", async () => {
    serve(() =>
      json({
        results: [{ statement: "MATCH (n) RETURN n LIMIT 25", status: 403, body: refusal }],
      }),
    );
    runQuery("MATCH (n) RETURN n LIMIT 25");
    expect(await screen.findByTestId("graph-frame-error")).toHaveTextContent(
      "This role may not run this query:",
    );
    const listed = screen.getByTestId("graph-frame-violations");
    expect(listed.querySelectorAll("li")).toHaveLength(2);
    expect(listed).toHaveTextContent("Column pets.price is not visible to role all");
    expect(listed).toHaveTextContent("Table owners is not visible to role all");
    expect(screen.queryByText(/cypher failed/)).toBeNull();
  });

  it("says a refusal of the whole request the same way", async () => {
    serve(() => json(refusal, 403));
    runQuery("MATCH (n) RETURN n LIMIT 25");
    expect(await screen.findByTestId("graph-frame-violations")).toHaveTextContent(
      "Table owners is not visible to role all",
    );
  });

  it("says a refusal with no violations by its detail", async () => {
    serve(() =>
      json({
        results: [
          {
            statement: "MERGE (n:Pets)",
            status: 400,
            body: { detail: "MERGE is not supported", code: "cypher.refused", params: {} },
          },
        ],
      }),
    );
    runQuery("MERGE (n:Pets)");
    expect(await screen.findByTestId("graph-frame-error")).toHaveTextContent(
      "MERGE is not supported",
    );
    expect(screen.queryByTestId("graph-frame-violations")).toBeNull();
  });
});
