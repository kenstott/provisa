// Copyright (c) 2026 Kenneth Stott
// Canary: 0ee3c48d-f7fc-4af7-96c1-9296d2a27d96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// #296: a refused /data/graph-counts is no counts. The sidebar shows its error where the counts go,
// the pills count "?", nothing crashes, and the error is not stored as counts for the next visit.
// A refused /data/graph-schema is no schema: its error shows where the labels and types go.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";
import "../i18n";

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
  useTables: () => ({ tables: [], loading: false }),
}));
vi.mock("../components/graph/GraphCanvas", () => ({ GraphCanvas: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const SCHEMA = {
  node_labels: [{ table_label: "Customers", table_id: 1, properties: [], pk_columns: ["id"] }],
  relationship_types: [{ type: "BOUGHT", source: "Customers", target: "Customers" }],
};

const ok = (body: unknown) =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

function serve(
  counts: () => Response | Promise<Response>,
  schema: () => Response | Promise<Response> = () => ok(SCHEMA),
) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      if (String(url).includes("/data/graph-counts")) return counts();
      if (String(url).includes("/data/graph-schema")) return schema();
      return ok({});
    }),
  );
}

function renderPage() {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <GraphPage />
      </MemoryRouter>
    </MantineProvider>,
  );
}

const storedCounts = () =>
  Object.keys(localStorage).filter((k) => k.includes("provisa.graph.counts"));

describe("GraphPage — a refused graph-counts request (#296)", () => {
  beforeEach(() => localStorage.clear());

  it("shows the response's error where the counts go, counts '?', and stores nothing", async () => {
    serve(
      () =>
        new Response(JSON.stringify({ detail: "engine unreachable" }), {
          status: 500,
          headers: { "content-type": "application/json" },
        }),
    );
    renderPage();
    const error = await screen.findByTestId("graph-counts-error");
    expect(error).toHaveTextContent("Counts unavailable: engine unreachable");
    expect(screen.getByTestId("graph-all-nodes")).toHaveTextContent("*(?)");
    expect(screen.getByTestId("graph-all-rels")).toHaveTextContent("*(?)");
    expect(storedCounts()).toEqual([]);
  });

  it("shows a request that reached no answer the same way", async () => {
    serve(() => Promise.reject(new TypeError("Failed to fetch")));
    renderPage();
    expect(await screen.findByTestId("graph-counts-error")).toHaveTextContent(
      "Counts unavailable: Failed to fetch",
    );
    expect(storedCounts()).toEqual([]);
  });

  it("counts as before when the request is answered", async () => {
    serve(
      () =>
        new Response(
          JSON.stringify({ node_count: 12, rel_count: 3, label_counts: {}, rel_counts: {} }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    );
    renderPage();
    await waitFor(() => expect(screen.getByTestId("graph-all-nodes")).toHaveTextContent("*(12)"));
    expect(screen.queryByTestId("graph-counts-error")).toBeNull();
    expect(storedCounts()).toHaveLength(1);
  });
});

describe("GraphPage — a refused graph-schema request", () => {
  beforeEach(() => localStorage.clear());

  it("shows the response's error where the labels and types go, not 'no labels found'", async () => {
    const counts = vi.fn(() => ok({}));
    serve(counts, () => new Response("schema build failed", { status: 503 }));
    renderPage();
    expect(await screen.findByTestId("graph-schema-error")).toHaveTextContent(
      "Schema unavailable: schema build failed",
    );
    expect(screen.getAllByRole("alert")).toHaveLength(2);
    expect(screen.queryByText("No labels found")).toBeNull();
    // No schema, nothing to count.
    expect(counts).not.toHaveBeenCalled();
  });

  it("shows a request that reached no answer the same way", async () => {
    serve(
      () => ok({}),
      () => Promise.reject(new TypeError("Failed to fetch")),
    );
    renderPage();
    expect(await screen.findByTestId("graph-schema-error")).toHaveTextContent(
      "Schema unavailable: Failed to fetch",
    );
  });
});
