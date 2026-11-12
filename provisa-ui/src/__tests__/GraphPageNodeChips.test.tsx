// Copyright (c) 2026 Kenneth Stott
// Canary: 73fb08d9-29e0-4e0d-afc9-cae85e9bf583
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The graph explorer's node chips: a plain label's chip runs its query, a parameterized label's
// chip opens the parameters dialog and runs nothing until it is confirmed.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
// One set, as the real context holds one: a new set on every render re-runs the counts fetch.
const NO_DOMAINS = vi.hoisted(() => new Set<string>());
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: NO_DOMAINS }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useTables: () => ({ tables: [], loading: false }),
}));
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";

const NODE = (table: string, nfc: { name: string; type: string }[]) => ({
  domain_label: null,
  domain_id: null,
  table_label: table,
  table_id: 1,
  properties: [],
  pk_columns: [],
  native_filter_columns: nfc,
});

describe("GraphPage — node chips", () => {
  it("runs a plain chip's query and opens the dialog for a parameterized chip", async () => {
    const cypher: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        let body: unknown = {};
        if (String(url).includes("/data/cypher")) {
          if (init?.body !== undefined) cypher.push(String(init.body));
          body = { results: [] };
        } else if (String(url).includes("/data/graph-counts"))
          body = { node_count: 2, rel_count: 0, label_counts: {}, rel_counts: {} };
        else if (String(url).includes("/data/graph-schema"))
          body = {
            node_labels: [NODE("PET", []), NODE("BREED", [{ name: "breed_name", type: "text" }])],
            relationship_types: [],
          };
        return new Response(JSON.stringify(body), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }),
    );
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(await screen.findByTestId("graph-label-BREED"));
    expect(await screen.findByText("Parameters for BREED")).toBeInTheDocument();
    expect(cypher).toEqual([]);
    fireEvent.click(screen.getByText("Cancel"));
    fireEvent.click(screen.getByTestId("graph-label-PET"));
    await waitFor(() => expect(cypher.length).toBe(1));
    expect(cypher[0]).toContain("MATCH (n:PET) RETURN n LIMIT 25");
  });
});
